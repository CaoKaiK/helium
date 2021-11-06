from logging import log
from utils.firebase_connection import init
from utils.logger import get_logger

logger = get_logger('database')

db = init()

wallet_list = [u'mining-ug']

for wallet_name in wallet_list:

  logger.info(f'Reading wallet: {wallet_name}')
  activities = db.collection(u'wallets').document(wallet_name).collection(u'activities').stream()

  activities_list = []
  for activity in activities:
    activities_list.append(activity.to_dict())
  
  prev_balance = 0
  for activity in activities_list:
    
    amount = activity.get('amount', 0)
    fee = activity.get('fee_hnt', 0)

    prev_balance += amount
    prev_balance -= fee

    
    balance = activity['balance']
    if balance != prev_balance:
      diff = balance - prev_balance

      height = activity.get('height')
      time = activity.get('time')

      logger.error(f'{diff} - Block {height} - {time}')
      prev_balance += diff