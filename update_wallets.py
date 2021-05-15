import firebase_admin
from utils.firebase_connection import init
from utils.helium_api import get_account, get_activities
from utils.classify_activity import classify_wallet_activity

db = init()

# load config file and stream wallets
config_ref = db.collection(u'config').document(u'config')
config = config_ref.get()
wallets = db.collection(u'wallets').stream()

# read overwrite flag
overwrite = config.get('wallets_overwrite')

# iterate wallets
for wallet in wallets:
  account_name = wallet.to_dict()['name']
  account_address = wallet.to_dict()['address']
  print(f'### Wallet: {account_name} ###')


  # get account balance for cross check
  account = get_account(account_address)
  balance = account.get('balance') / 1e8
  print(f'Current balance: {balance}')

  cursor = ''
  activities = []
  current_balance = balance

  # initiate first call and repeat until no cursor is returned
  while not activities or cursor:
    activities, cursor = get_activities(account_address, cursor, get='wallet')
    
    print('.')

    for activity in activities:
      # arrange activity according to type
      activity = classify_wallet_activity(activity, current_balance)
   
      height = activity.get('height')
      hash_act = activity.get('hash')
      
      # firebase db document ref
      event_ref = db.collection(u'wallets').document(account_name).collection(u'activities').document(f'{height}_{hash_act}')
      event = event_ref.get()

      # create documents if it doesn't exist
      # overwrite document if overwrite is true
      if (not event.exists or overwrite):
        if activity:
          act_type = activity.get('type')
          print(f'New entry: {act_type} on block {height}')
          event_ref.set(activity)
          # set current balance to previous balance
          current_balance = activity.get('prev_balance')
        else:
          print('Skipping known transaction with no balance change')
      else:
        print('--- completed ---')
        print(f'--- latest block: {height} ---')
        
        # check current_balance of already existing event 
        # with current balance of last transaction in this run
        firebase_balance = event.get('current_balance')
        current_balance = activity.get('current_balance')
        
        if difference:= firebase_balance - current_balance == 0:
          print('Check balance ok')
        else:
          print(f'Check balance failed: {difference}')

        # set cursor to empty to break while loop
        cursor = ''
        # break activity loop
        break

# reset overwrite flag
if overwrite:
  config_ref.update({u'wallets_overwrite': False})