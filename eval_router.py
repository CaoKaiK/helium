import os
from datetime import date, timedelta

from utils.firebase_connection import init

db = init()

# load config file and stream routers
config = db.collection(u'config').document(u'config').get()
routers = db.collection(u'routers').stream()

eval_year = config.get('routers_eval_year')
eval_month = config.get('routers_eval_month')
eval_day = config.get('routers_eval_day')

last_eval_date = date(eval_year, eval_month, eval_day)
date_yesterday = date.today() - timedelta(days=1)

print('--- Evaluate router rewards ---')
print(f'Starting: {eval_year}-{eval_month}-{eval_day}')


while date_yesterday >= last_eval_date:
  day = last_eval_date.day
  month = last_eval_date.month
  year = last_eval_date.year

  for router in routers:
    challenger = 0
    beacon = 0
    witness = 0
    data = 0
    
    router_name = router.to_dict()['name']
    print(router_name)

    router_ref = db.collection(u'routers').document(router_name).collection(u'activities')

    router_rewards_that_day = router_ref\
      .where(u'type', u'in',[u'rewards_v2', u'rewards_v1']) \
      .where(u'day', u'==', day) \
      .where(u'month', u'==', month) \
      .where(u'year', u'==', year).get()

    for reward_act in router_rewards_that_day:
      reward_act = reward_act.to_dict()
      for reward in reward_act['rewards']:
        amount = reward['amount'] / 1e8
        if reward['type'] == 'poc_challengers':
          challenger = challenger + amount
        elif reward['type'] == 'poc_challengees':
          beacon = beacon + amount        
        elif reward['type'] == 'poc_witnesses':
          witness = witness + amount
        elif reward['type'] == 'data_credits':
          data = data + amount
        else:
          rew_type = reward['type']
          print(f'!!! unknown reward type {rew_type}')

    total = challenger + beacon + witness + data
    print(total)

  print(last_eval_date)
  last_eval_date = last_eval_date + timedelta(days=1)





