import os

from utils.firebase_connection import init
from utils.convert import convert_df_to_datev
from utils.logger import get_logger

import pandas as pd

logger = get_logger('export')

db = init()

# load config file and stream wallets
fifo_balance_ref = db.collection(u'fifo').document(u'C&R')

# month
eval_year = 2021
eval_month = 7

logger.info(f'Exporting {eval_month}-{eval_year}')

# path without extension
abspath = os.path.dirname(os.path.abspath(__file__))
export_path_standard = os.path.join(abspath,'export', 'standard', f'{eval_year}-{eval_month}')
export_path_datev = os.path.join(abspath,'export', 'datev', f'datev_{eval_year}-{eval_month}')

events = balance_hnt_ref.where(u'year', u'==', eval_year).where(u'month', u'==', eval_month).get()

event_list = []
for event in events:
  event_list.append(event.to_dict())

if event_list:
  event_df = pd.DataFrame(event_list)
  event_df['time'] = event_df['time'].dt.tz_localize(None)
  event_df = event_df.sort_values(by=['time'])

  convert_df_to_datev(event_df.copy(), export_path_datev)
