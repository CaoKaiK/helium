import os
from datetime import date, timedelta

from utils.firebase_connection import init
from utils.helium_api import get_account, get_activities
from utils.classify_activity import classify_wallet_activity

db = init()

# load config file and stream wallets
config_ref = db.collection(u'config').document(u'config')
config = config_ref.get()
wallets = db.collection(u'wallets').stream()

# # extract 
# eval_year = config.get('wallets_eval_year')
# eval_month = config.get('wallets_eval_month')
# eval_day = config.get('wallets_eval_day')

# read overwrite flag
overwrite = config.get('wallets_overwrite')

for wallet in wallets:
  account_address = wallet.to_dict()['address']
  account_name = wallet.to_dict()['name']

  # get account balance for cross check
  account = get_account(account_address)
  balance = account.get('balance') / 1e8
  print(balance)

  cursor = ''
  activities = []
  current_balance = balance

  while not activities or cursor:
    activities, cursor = get_activities(account_address, cursor, get='wallet')
    
    print('.')

    for activity in activities:
      activity = classify_wallet_activity(activity, current_balance)
   
      height = activity.get('height')
      hash_act = activity.get('hash')

      event_ref = db.collection(u'wallets').document(account_name).collection(u'activities').document(f'{height}_{hash_act}')

      event = event_ref.get()

      if (not event.exists or overwrite):
        if activity:
          act_type = activity.get('type')
          print(f'New entry: {act_type} on block {height}')
          event_ref.set(activity)
          current_balance = activity.get('prev_balance')
        else:
          print('Skipping known transaction with no balance change')
      else:
        print('--- completed ---')
        print(f'--- latest block: {height} ---')
        cursor = ''
        break

if overwrite:
  config_ref.update({u'wallets_overwrite': False})