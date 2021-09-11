from datetime import datetime
from utils.firebase_connection import init
from utils.helium_api import get_account, get_activities
from utils.classify_activity import classify_wallet_activity, create_fifo_event
from utils.logger import get_logger
from utils.balance import outstanding_corrections, make_correction ,run_balance

# daily log files send to wallets directory
logger = get_logger('wallets')

db = init()

# define list of wallets to update
#u'helium-wallet' 
wallet_list = [u'temporary-wallet', u'private-wallet', u'mining-ug']

# load config file and stream wallets
config_ref = db.collection(u'config').document(u'config')
config = config_ref.get()
wallets = db.collection(u'wallets').where(u'name', u'in', wallet_list).stream()

# read overwrite flag
overwrite = config.get('wallets_overwrite')
if overwrite:
  logger.warning(f'Overwrite: {overwrite}')

overwrite_fifo = config.get('fifo_overwrite')
if overwrite_fifo:
  logger.warning(f'Overwrite FIFO: {overwrite}')

# iterate wallets
for wallet in wallets:
  wallet_name = wallet.to_dict()['name']
  wallet_address = wallet.to_dict()['address']
  wallet_ref = db.collection(u'wallets').document(wallet_name)

  # get current account balance and current block/height
  account = get_account(wallet_address)
  current_balance = account.get('balance')
  current_height = account.get('block')
  logger.info(f'{wallet_name} - Current account balance: {current_balance/1e8:.8f} on block {current_height}')

  # check last commited balance and height
  # reset balance and balance height if overwrite is True
  if overwrite:
    logger.warning(f'{wallet_name} - Reset Balance')
    wallet_ref.update({u'balance': 0})
    wallet_ref.update({u'balance_height': 0})
    last_balance = 0
    last_height = 0
  else:
    last_balance = wallet.to_dict()['balance']
    last_height = wallet.to_dict()['balance_height']
  
  logger.info(f'{wallet_name} - Last commited balance:   {last_balance/1e8:.8f} on block {last_height}')

  # check if changes to balance occurred
  if current_balance != last_balance or overwrite:
    update_required = True
  else:
    update_required = False

  # start updating if:
  # update is required due to unequal balance
  # 
  # continue update until either:
  # no cursor is returned => end of activities reached
  # or no activities => initiates first run where no cursor is available
  # 
  # while loop breaks when activity is already present in the db
  # if overwrite is True => loop will run until no cursor returned

  cursor = ''
  activities = []
  activities_staged = []
  amount_staged = 0

  while update_required and (cursor or not activities):
    # get activities and cursor
    # activities start with the most recent event
    activities, cursor = get_activities(wallet_address, logger, cursor, get='wallet')
    
    logger.debug(f'{wallet_name} - Cursor was returned: {cursor}')

    for activity in activities:
      # classify activity according to type
      activity = classify_wallet_activity(activity, wallet_address, logger)

      height = activity.get('height')
      hash_act = activity.get('hash')
      time = activity.get('time', datetime(1990, 1, 1, 1, 1, 1))

      # height defines document name as each block is unique
      # search the event in db
      height_filled_str = str(height).zfill(10)
      time_str = time.strftime('%Y-%m-%dT%H:%M:%S')
      event_ref = db.collection(u'wallets')\
        .document(wallet_name)\
          .collection(u'activities')\
            .document(f'{height_filled_str}_{time_str}')
      
      event = event_ref.get()

      # create document if it doesn't exist
      # overwrites document if overwrite is True
      if (not event.exists or overwrite):
        if activity:
          act_type = activity.get('type')
          act_amount = activity.get('amount')
          act_fee_hnt = activity.get('fee_hnt', 0)


          # # force commit on overwrite
          # if overwrite:
          #   event_ref.set(activity)
          #   logger.info(f'{wallet_name} - Forced Commit: {act_amount/1e8:.8f} from {act_type} on block {height}')
          # # or append to staged list for balance check
          # else:
            
          activities_staged.append(activity)
          # sum of all staged amounts should be equal to difference between last and current balance
          amount_staged = amount_staged + act_amount - act_fee_hnt
          logger.info(f'{wallet_name} - Staged: {act_amount/1e8:.8f} from {act_type} on block {height}')

        else:
          logger.info(f'{wallet_name} - Skipping known transaction with no balance change')
      else:
        # set cursor to empty to break while loop
        cursor = ''
        # break activity loop
        break

  ##### activities are now staged in list #####

  if activities_staged:
    # reference to collection of corrections
    corrections_ref =  db.collection(u'wallets').document(wallet_name).collection(u'corrections')
    outstanding_amount = outstanding_corrections(last_height, corrections_ref)

    # compare last balance + amount staged outstanding and current balance + outstanding
    # value other than 0 indicate missing activities or inaccuracies
    missing_amount = (last_balance + amount_staged) - (current_balance + outstanding_amount)

    logger.info(f'{wallet_name} - Amount staged:   {amount_staged/1e8:.8f}')
    logger.info(f'{wallet_name} - Last balance:    {last_balance/1e8:.8f} on block {last_height}')
    logger.info(f'{wallet_name} - Current balance: {current_balance/1e8:.8f} on block {current_height}')
    logger.info(f'{wallet_name} -                  ==========')
    logger.info(f'{wallet_name} - Outstanding:     {outstanding_amount/1e8:.8f}')
    logger.info(f'{wallet_name} - Difference:      {missing_amount/1e8:.8f}')
    
    # activities involving fees can sometime introduce inaccuracies
    # these inaccuracies are generally 1-3 bones (1e-8 HNT)
    # check if an outgoing activity exists in staged activities
    #last_outgoing_activities = list(filter(lambda activity: activity.get('fee_hnt', 0) > 0, activities_staged))

    #last_outgoing_activities = [d for d in activities_staged if d.get('fee_hnt', 0) > 0]
    last_outgoing_activity = None
    for activity in activities_staged:
      if activity.get('fee_hnt', 0) > 0:
        last_outgoing_activity = activity
        break

    # check if absolut missing amount is a small value and
    if (0 < abs(missing_amount) < 10) and last_outgoing_activity:
      # reference to collection of corrections
      corrections_ref =  db.collection(u'wallets').document(wallet_name).collection(u'corrections')
      # make corrections
      success = make_correction(last_outgoing_activity, missing_amount, corrections_ref)

      # if successful set, missing amount to zero to continue with balance run
      if success:
        logger.info(f'{wallet_name} - Correction:      {missing_amount/1e8:.8f}')
        missing_amount = 0
    
    # correction worked or was not necessary
    if missing_amount == 0:
      # get latest correction reference
      corrections_ref =  db.collection(u'wallets').document(wallet_name).collection(u'corrections')
      # run balance
      activities_for_commit = run_balance(last_balance, last_height, activities_staged, corrections_ref)
      
      # iterate activities and commit to db
      for activity_for_commit in activities_for_commit:

        # prepare document path for wallet
        height = activity_for_commit['height']
        time = activity_for_commit['time']
        act_amount = activity_for_commit['amount']
        act_type = activity_for_commit['type']
        balance = activity_for_commit['balance']

        height_filled_str = str(height).zfill(10)
        time_str = time.strftime('%Y-%m-%dT%H:%M:%S')
        event_ref = db.collection(u'wallets')\
          .document(wallet_name)\
            .collection(u'activities')\
              .document(f'{height_filled_str}_{time_str}')
        
        # get fifo account to register activity in
        fifo_accounts = db.collection(u'fifo').stream()

        for fifo_account in fifo_accounts:
          if wallet_address in fifo_account.to_dict().get('wallets', []):
            fifo_account_name = fifo_account.to_dict().get('name')
            # break on first match
            break
          else:
            fifo_account_name = 'fallback'

        # document path for fifo
        fifo_ref = db.collection(u'fifo')\
          .document(fifo_account_name)\
            .collection(u'balance')\
              .document(f'{height_filled_str}_{wallet_address[0:4]}')
        
        fifo = fifo_ref.get()
        
        # create fifo event from activity
        fifo_event = create_fifo_event(activity_for_commit, wallet_address[0:4], logger)

        # writing to wallet and fifo
        event_ref.set(activity_for_commit)
        logger.info(f'{wallet_name} - Commit: {act_amount/1e8:.8f} from {act_type} on block {height}')
        if not fifo.exists or overwrite_fifo:
          fifo_ref.set(fifo_event)
          logger.info(f'{wallet_name} - FIFO: {act_amount/1e8:.8f} from {act_type} on block {height}')
        
        # update balance
        wallet_ref.update({u'balance': balance})
        wallet_ref.update({u'balance_height': height})

    else:
      # throw error and unstage activities
      logger.warning(f'{wallet_name} - Unresolved Balance - {missing_amount/1e8:.8f}')
      None

  ##### No update required #####
  else:
    logger.info(f'{wallet_name} - No update required')

# reset overwrite flag
if overwrite:
  config_ref.update({u'wallets_overwrite': False})
  logger.warning('Overwrite: False')

if overwrite_fifo:
  config_ref.update({u'fifo_overwrite': False})
  logger.warning('Overwrite FIFO: False')

