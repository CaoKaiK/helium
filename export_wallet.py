from datetime import date, timedelta
import os

from utils.firebase_connection import init
from utils.convert import convert_df_to_datev

import pandas as pd

db = init()

# load config file and stream wallets
config = db.collection(u'config').document(u'config').get()
wallets = db.collection(u'wallets').stream()

wallets_list = []
for wallet in wallets:
  wallets_list.append(wallet)

# pick specific wallet or comment
wallets_list = wallets_list[0:1]

# month
eval_year = 2021
eval_month = 6

# path without extension
abspath = os.path.dirname(os.path.abspath(__file__))
export_path_standard = os.path.join(abspath,'export', 'standard', f'{eval_year}-{eval_month}')
export_path_datev = os.path.join(abspath,'export', 'datev', f'{eval_year}-{eval_month}')

for wallet in wallets_list:
  wallet_name = wallet.to_dict()['name']
  print(f'Exporting {wallet_name}:')
  
  wallet_ref = db.collection(u'wallets').document(wallet_name).collection(u'activities')
  wallets_activities = wallet_ref\
    .where(u'year', u'==', eval_year)\
    .where(u'month', u'==', eval_month).get()
  
  activity_list = []
  for activity in wallets_activities:
    activity_list.append(activity.to_dict())
  
  if activity_list:
    wallet_df = pd.DataFrame(activity_list)
    # remove timezone
    wallet_df['time'] = wallet_df['time'].dt.tz_localize(None)
    # sortcolumns
    wallet_df = wallet_df.sort_index(axis=1)

    # datev
    convert_df_to_datev(wallet_df.copy(), export_path_datev)

    # debugging
    standard_path = export_path_standard + '_' + wallet_name + '.xlsx'
    wallet_df.to_excel(standard_path, index=False)

  print(f'Entries in firebase db: {len(wallets_activities)}')