import os

from utils.firebase_connection import init
from utils.convert import convert_df_to_datev
from utils.logger import get_logger

import pandas as pd

logger = get_logger('export')

db = init()

account_list = [u'C&R']#, u'Temporary']

# month
eval_year = 2022  
eval_month = 8

for account in account_list:
  # load config file and stream wallets
  fifo_balance_ref = db.collection(u'fifo').document(account).collection(u'balance')
  fifo_corrections_ref = db.collection(u'fifo').document(account).collection(u'corrections')

  # path without extension
  abspath = os.path.dirname(os.path.abspath(__file__))
  export_path_datev = os.path.join(abspath,'export', 'datev', f'datev_{account}_{eval_year}-{eval_month}')



  logger.info(f'{account} - Exporting - {eval_month}-{eval_year}')

  # read events from db
  events = fifo_balance_ref.where(u'year', u'==', eval_year).where(u'month', u'==', eval_month).get()
  corrections = fifo_corrections_ref.where(u'year', u'==', eval_year).where(u'month', u'==', eval_month).get()

  # construct dataframe
  event_list = []
  for event in events:
    event_list.append(event.to_dict())

  for correction in corrections:
    event_list.append(correction.to_dict())

  if event_list:
    event_df = pd.DataFrame(event_list)
    event_df['time'] = event_df['time'].dt.tz_localize(None)
    event_df = event_df.sort_values(by=['height'])

    convert_df_to_datev(event_df.copy(), export_path_datev)
  else:
    logger.info(f'{account} - No events returned for this month')
