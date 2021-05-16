from datetime import date, timedelta

from utils.firebase_connection import init

db = init()

# load config file and stream routers
config = db.collection(u'config').document(u'config').get()
routers = db.collection(u'routers').stream()

routers_list = []
for router in routers:
  routers_list.append(router)

# next day to evaluate
eval_year = config.get('routers_eval_year')
eval_month = config.get('routers_eval_month')
eval_day = config.get('routers_eval_day')

last_eval_date = date(eval_year, eval_month, eval_day)
# stop evaluation at latest day where all data is available
# yesterday
date_yesterday = date.today() - timedelta(days=1)

print('--- Evaluate router rewards ---')
print(f'From: {last_eval_date} Until: {date_yesterday}')

# iterate days and switch between router within while loop
while date_yesterday >= last_eval_date:
  # update last date
  # this date is the oldest date for which no eval
  # has taken place
  day = last_eval_date.day
  month = last_eval_date.month
  year = last_eval_date.year

  # iterate routers
  for router in routers_list:
    # reset reward types
    challenger = 0
    beacon = 0
    witness = 0
    data = 0
    consensus = 0
    
    router_name = router.to_dict()['name']
    # print(router_name)

    router_ref = db.collection(u'routers').document(router_name).collection(u'activities')

    router_rewards_that_day = router_ref\
      .where(u'type', u'in',[u'rewards_v2', u'rewards_v1']) \
      .where(u'day', u'==', day) \
      .where(u'month', u'==', month) \
      .where(u'year', u'==', year).get()

    # iterate all reward activities that day
    for reward_act in router_rewards_that_day:
      reward_act = reward_act.to_dict()
      # iterate all reward in the same reward activity
      for reward in reward_act['rewards']:
        amount = round(reward['amount'] / 1e8, 8)
        if reward['type'] == 'poc_challengers':
          challenger += amount
        elif reward['type'] == 'poc_challengees':
          beacon += amount        
        elif reward['type'] == 'poc_witnesses':
          witness += amount
        elif reward['type'] == 'data_credits':
          data += amount
        elif reward['type'] == 'consensus':
          consensus += amount
        else:
          rew_type = reward['type']
          print(f'!!! unknown reward type {rew_type}')

    total = challenger + beacon + witness + data + consensus

    reward_type_map = {
      'total': total,
      'challenger': challenger,
      'beacon': beacon,
      'witness': witness,
      'data': data,
      'consensus': consensus
    }

    date_string = last_eval_date.strftime('%Y-%m-%d')
    reward_ref = db.collection(u'routers')\
      .document(router_name)\
      .collection(u'rewards')\
      .document(date_string)
    
    reward_ref.set({'reward_type': reward_type_map})

  print(last_eval_date)
  last_eval_date = last_eval_date + timedelta(days=1)





