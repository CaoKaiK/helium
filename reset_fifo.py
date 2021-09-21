
from utils.firebase_connection import init
from utils.logger import get_logger

logger = get_logger('fifo')

db = init()

# read wallet and overwrite in fifo collection
# , u'temporary-wallet', u'private-wallet'
wallet_list = [u'mining-ug']

fifo_ref = db.collection(u'fifo')

for wallet_name in wallet_list:

  logger.info(f'Reading wallet: {wallet_name}')
  wallet_ref = db.collection(u'wallets').document(wallet_name)

  wallet = wallet_ref.get()
  wallet = wallet.to_dict()

  wallet_address = wallet['address']
  wallet_id = wallet_address[0:4]


  # get fifo account to register activity in
  fifo_accounts = db.collection(u'fifo').stream()
  for fifo_account in fifo_accounts:
    if wallet_address in fifo_account.to_dict().get('wallets', []):
      fifo_account_name = fifo_account.to_dict().get('name')
      # break on first match
      break
    else:
      fifo_account_name = 'fallback'
  
  logger.info(f'Register events in: {fifo_account_name}')
  fifo_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'balance')
  
  activities = db.collection(u'wallets').document(wallet_name).collection(u'activities').stream()

  activities_list = []
  for activity in activities:
    activities_list.append(activity.to_dict())
  
  logger.info(f'Total events: {len(activities_list)}')
  
  for activity in activities_list:
    doc_id = str(activity['height']).zfill(10) + '_' + wallet_id

    if activity['amount'] > 0:
      fifo_to_allocate = activity['amount']
      fee_hnt = 0

    else:
      fee_hnt = activity.get('fee_hnt', 0)

      fifo_to_allocate = -fee_hnt

    fifo_event = {
              'time': activity['time'],
              'year': activity['year'],
              'month': activity['month'],
              'day': activity['day'],
              'height': activity['height'],
              'amount': activity['amount'],
              'fee_hnt': fee_hnt,
              'fee_usd': activity.get('fee_usd',0),
              'fifo_to_allocate': fifo_to_allocate,
              'price_usd': activity['price_usd'],
              'type': activity['type'],
              'committed': False,
              'wallet_id': wallet_id,
              'has_eur': False
            }

    fifo_event_ref = fifo_ref.document(doc_id)
    fifo_event_ref.set(fifo_event)