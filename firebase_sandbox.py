from utils.firebase_connection import init
import pandas as pd

db = init()

# get fifo account to register activity in
fifo_accounts = db.collection(u'fifo').stream()

staged_events_in_df = pd.DataFrame()

for fifo_account in fifo_accounts:
  fifo_account_name = fifo_account.to_dict().get('name')
  print(fifo_account_name)

  if '14pomxxicuQUXsnsL7Ew6R6tJ2VbYJsidgLBwCNHfRGgdpJ9bc8' in fifo_account.to_dict().get('wallets', []):
    fifo_account_name = fifo_account.to_dict().get('name')

    break
    
  else:
    fifo_account_name = 'fallback'

print(fifo_account_name)


  # fifo_events_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'balance')

  # fifo_in = fifo_events_ref.where(u'committed', u'==', False).where(u'fifo_to_allocate', u'>', 0).get()

  # for event_in in fifo_in:
  #   staged_events_in_df = staged_events_in_df.append(event_in.to_dict(), ignore_index=True)
  #   print(event_in.to_dict())

# wallet_name = 'temporary-wallet'
# last_height = 1000000

# corrections_ref =  db.collection(u'wallets').document(wallet_name).collection(u'corrections')
# corrections = corrections_ref.where(u'height', u'>', last_height).stream()

# for correction in corrections:
#   print(correction.to_dict())