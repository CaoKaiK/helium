from datetime import date, timedelta

from utils.firebase_connection import init
from utils.export_report import export

db = init()

# load config file and stream routers
config_ref = db.collection(u'config').document(u'config')
config = config_ref.get()
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

print('--- Evaluate router ---')
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
    challenger_count = 0
    beacon = 0
    beacon_count = 0
    witness = 0
    witness_count = 0
    data = 0
    data_count = 0
    consensus = 0
    consensus_count = 0
    
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
          challenger_count += 1
        elif reward['type'] == 'poc_challengees':
          beacon += amount        
          beacon_count += 1
        elif reward['type'] == 'poc_witnesses':
          witness += amount
          witness_count += 1
        elif reward['type'] == 'data_credits':
          data += amount
          data_count += 1
        elif reward['type'] == 'consensus':
          consensus += amount
          consensus_count += 1
        else:
          rew_type = reward['type']
          print(f'!!! unknown reward type {rew_type}')

    total = challenger + beacon + witness + data + consensus
    total_count = challenger_count + beacon_count + witness_count + data_count + consensus_count

    rewards_map = {
      'total': total,
      'challenger': challenger,
      'beacon': beacon,
      'witness': witness,
      'data': data,
      'consensus': consensus
    }
    rewards_count_map = {
      'total': total_count,
      'challenger' : challenger_count,
      'beacon': beacon_count,
      'witness': witness_count,
      'data': data_count,
      'consensus': consensus_count
    }

    date_string = last_eval_date.strftime('%Y-%m-%d')
    eval_ref = db.collection(u'routers')\
      .document(router_name)\
      .collection(u'eval')\
      .document(date_string)
    
    eval_ref.set({
      'day': day,
      'month': month,
      'year': year,
      'rewards': rewards_map,
      'count': rewards_count_map
      })


  print(last_eval_date)
    
  last_eval_date = last_eval_date + timedelta(days=1)

  if last_eval_date.day == 1:
    # export monthly report
    export(last_eval_date, db)
    print('Export')


# config_ref.update({u'routers_eval_day': date.today().day})
# config_ref.update({u'routers_eval_month': date.today().month})
# config_ref.update({u'routers_eval_year': date.today().year})



