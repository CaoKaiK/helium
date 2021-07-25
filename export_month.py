import os

from utils.firebase_connection import init
from utils.convert import convert_df_to_datev
from utils.logger import get_logger

import pandas as pd

logger = get_logger('export')

db = init()

# load config file and stream wallets
balance_hnt_ref = db.collection(u'balance_hnt')

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

#   activity_list = []
#   for activity in wallets_activities:
#     activity_list.append(activity.to_dict())
  
#   if activity_list:
#     wallet_df = pd.DataFrame(activity_list)
#     # remove timezone
#     wallet_df['time'] = wallet_df['time'].dt.tz_localize(None)
#     # sortcolumns
#     wallet_df = wallet_df.sort_index(axis=1)

#     # datev
#     convert_df_to_datev(wallet_df.copy(), export_path_datev)

#     # debugging
#     standard_path = export_path_standard + '_' + wallet_name + '.xlsx'
#     wallet_df.to_excel(standard_path, index=False)

#   print(f'Entries in firebase db: {len(wallets_activities)}')