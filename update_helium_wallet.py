
from os import mkdir
from utils.firebase_connection import init
from utils.helium_api import get_account, get_activities
from utils.classify_activity import classify_wallet_activity
from utils.logger import get_logger
from utils.balance_fifo import run_balance

logger = get_logger('wallets')

db = init()

# load config file and stream wallets
config_ref = db.collection(u'config').document(u'config')
config = config_ref.get()
wallets = db.collection(u'wallets').where(u'type', '==', 'helium').stream()

balance_hnt_ref = db.collection(u'balance_hnt')

# read overwrite flag
overwrite = config.get('wallets_overwrite')
if overwrite:
  logger.warning(f'Overwrite: {overwrite}')

# iterate wallets
for wallet in wallets:
  wallet_name = wallet.to_dict()['name']
  wallet_address = wallet.to_dict()['address']

  # get account balance for cross check
  account = get_account(wallet_address)
  balance = account.get('balance')
  current_height = account.get('block')
  logger.info(f'{wallet_name} - Current balance: {balance} on {current_height}')

  cursor = ''
  activities = []

  # initiate first call and repeat until no cursor is returned
  # or activity already in db - see break
  while not activities or cursor:
    activities, cursor = get_activities(wallet_address, cursor, get='wallet')
    
    logger.debug(f'{wallet_name} - Cursor was returned: {cursor}')

    for activity in activities:
      # arrange activity according to type
      activity = classify_wallet_activity(activity, wallet_address, logger)

      height = activity.get('height')
      hash_act = activity.get('hash')
      
      # firebase db document ref
      event_ref = db.collection(u'wallets').document(wallet_name).collection(u'activities').document(f'{height}_{hash_act}')
      event = event_ref.get()

      # create documents if it doesn't exist
      # overwrites document if overwrite is true
      if (not event.exists or overwrite):
        if activity:
          act_type = activity.get('type')
          logger.info(f'{wallet_name} - Entry: {act_type} on block {height}')
          event_ref.set(activity)

          # enter event in fifo list
          time = activity['time']
          doc_id = time.strftime('%Y-%m-%dT%H:%M:%S')

          if activity['amount'] > 0:
            fifo_to_allocate = activity['amount']

          elif activity.get('fee_hnt', 0) > 0:
            fifo_to_allocate = -activity['fee_hnt']
          
          else:
            logger.warning(f'{wallet_name} - {act_type} on block {height} not evaluated in FIFO')

          fifo_event = {
            'time': activity['time'],
            'year': activity['year'],
            'month': activity['month'],
            'day': activity['day'],
            'height': height,
            'amount': activity['amount'],
            'fee_hnt': activity.get('fee_hnt', 0),
            'fee_usd': activity.get('fee_usd',0),
            'fifo_to_allocate': fifo_to_allocate,
            'price': activity['price'],
            'type': act_type
          }
          
          fifo_event_ref = balance_hnt_ref.document(doc_id)
          fifo_event_ref.set(fifo_event)

        else:
          logger.info(f'{wallet_name} - Skipping known transaction with no balance change')
      else:
        # set cursor to empty to break while loop
        cursor = ''
        # break activity loop
        break

  # activity list of wallet is now up to date

  # fetch latest balance height and activities
  wallet_ref = db.collection(u'wallets').document(wallet_name)
  activities_ref = wallet_ref.collection(u'activities')
  balance_height = wallet.to_dict()['balance_height']
  last_balance = wallet.to_dict()['balance']

  if overwrite:
    logger.warning(f'{wallet_name} - Reset Balance')
    wallet_ref.update({u'balance': 0})
    wallet_ref.update({u'balance_height': 0})


  logger.info(f'{wallet_name} - Balance from {balance_height} to {current_height}')

  # run balance
  calc_balance, last_activity, last_fee = run_balance(activities_ref, balance_height, last_balance)
  
  # if empty last_activity is returned, no additional events happened between now and last_balance
  if last_activity:
    # height for new balance height and correction
    height = last_activity['height']
    
    # check calculated balance with actual balance
    if calc_balance == balance:
      logger.info(f'{wallet_name} - Balance ok')
      # update balance in wallet
      wallet_ref.update({u'balance': balance})
    else:
      # add correction in front of last activity
      last_fee_height = last_fee['height']
      last_fee['amount'] = 0
      last_fee['fee_hnt'] = calc_balance - balance
      last_fee['fee'] = 0
      last_fee['fee_usd'] = 0
      last_fee['type'] = 'correction'

      event_ref = activities_ref.document(f'{last_fee_height}_')
      event_ref.set(last_fee)

      fifo_event_ref

      logger.info(f'{wallet_name} - Balance corrected by {calc_balance-balance}')

      calc_balance, last_activity, last_fee = run_balance(activities_ref, balance_height, last_balance)

      if calc_balance == balance:
        logger.info(f'{wallet_name} - Balance ok after correction')
      else:
        logger.error(f'{wallet_name} - Balance failed after correction')
      
      # update balance in wallet
      wallet_ref.update({u'balance': balance})

    wallet_ref.update({u'balance_height': height})


# reset overwrite flag
if overwrite:
  config_ref.update({u'wallets_overwrite': False})
  logger.warning('Overwrite: False')
