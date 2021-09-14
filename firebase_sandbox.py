from os import utime
from utils.firebase_connection import init
import pandas as pd

from datetime import datetime

date = datetime.utcfromtimestamp(1631544114)

print(date)


# db = init()

# exchange_rates = db.collection(u'exchange_rate').get()


# exchange_rates_list = []
# for rate in exchange_rates:
#   exchange_rates_list.append(rate.to_dict())

# exchange_rates_df = pd.DataFrame(exchange_rates_list)

# print(exchange_rates_df)

# exchange_rates_df['day'] = 1
# exchange_rates_df['date'] = pd.to_datetime(exchange_rates_df[['year', 'month', 'day']])

# latest_exchange = exchange_rates_df[exchange_rates_df['date'] == exchange_rates_df['date'].max()]
# # firebase does not accept numpy dt
# # recreate dt from time

# print(latest_exchange['year'])
# print(latest_exchange['year'].values[0])

# latest_exchange_dt = datetime(latest_exchange['year'].values[0], latest_exchange['month'].values[0] + 1, latest_exchange['day'].values[0])




# fifo_events_ref = db.collection(u'fifo').document(u'C&R').collection(u'balance') 
# fifo_events = fifo_events_ref.where(u'committed', u'==', False).get()

# for event in fifo_events:
#   print(event.to_dict())


# # get fifo account to register activity in
# fifo_accounts = db.collection(u'fifo').stream()

# staged_events_in_df = pd.DataFrame()

# for fifo_account in fifo_accounts:
#   fifo_account_name = fifo_account.to_dict().get('name')

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