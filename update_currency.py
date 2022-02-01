from utils.helium_api import get_account
from utils.firebase_connection import init
from utils.logger import get_logger

from datetime import date, datetime

import pandas as pd

# daily log files send to currency directory
logger = get_logger('currency')

db = init()

# define list of accounts to update
#  
account_list = [u'Temporary', u'C&R']

# get exchange rates
exchange_rates = db.collection(u'exchange_rate').get()

exchange_rates_list = []
for rate in exchange_rates:
  exchange_rates_list.append(rate.to_dict())

exchange_rates_df = pd.DataFrame(exchange_rates_list)

exchange_rates_df['day'] = 1
exchange_rates_df['date'] = pd.to_datetime(exchange_rates_df[['year', 'month', 'day']])

latest_exchange = exchange_rates_df[exchange_rates_df['date'] == exchange_rates_df['date'].max()]
# firebase does not accept numpy dt
# recreate dt from time
year = latest_exchange['year'].values[0]
month = latest_exchange['month'].values[0]

if month == 12:
  latest_exchange_dt = datetime( year+1, 1, 1)
else:
  latest_exchange_dt = datetime(year, month, 1)

logger.info(f'Latest exchange value pair: {year} - {month}')

# stream fifo accounts
fifo_accounts = db.collection(u'fifo').where(u'name', u'in', account_list).stream()

for fifo_account in fifo_accounts:
  # account name
  fifo_account_name = fifo_account.to_dict().get('name')
  fifo_account_wallets = fifo_account.to_dict().get('wallets')
  fifo_events_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'balance')
  fifo_exchanges_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'exchanges')

  logger.info(f'Writing exchange value pair - {fifo_account_name}')

  fifo_events = fifo_events_ref.where(u'committed', u'==', False)\
    .where(u'has_eur', u'==', False)\
      .where(u'time', u'<', latest_exchange_dt).get()
  
  logger.info(f'Events to be written - {len(fifo_events)}')

  i = 1
  for fifo_event in fifo_events:
    fifo_event = fifo_event.to_dict()
    year = fifo_event['year']
    month = fifo_event['month']
    price_usd = fifo_event['price_usd']
    fee_usd = fifo_event.get('fee_usd', 0)

    height = fifo_event['height']
    wallet_id = fifo_event.get('wallet_id', '')
    event_id = str(height).zfill(10) + '_' + wallet_id

    eur_usd = exchange_rates_df[(exchange_rates_df['year'] == year) & (exchange_rates_df['month'] == month )]['eur_usd'].values[0]

    event_ref = fifo_events_ref.document(event_id)


    event_ref.update({
      'eur_usd': eur_usd,
      'price_eur': round(price_usd / eur_usd, 5),
      'fee_eur': round(fee_usd / eur_usd, 5),
      'has_eur': True
    })
    
    logger.info(f'Event written - {i}/{len(fifo_events)}')
    i += 1



