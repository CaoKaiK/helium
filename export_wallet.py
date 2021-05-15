from datetime import date, timedelta

from utils.firebase_connection import init

db = init()

# load config file and stream wallets
config = db.collection(u'config').document(u'config').get()
wallets = db.collection(u'wallets').stream()

wallets_list = []
for wallet in wallets:
  wallets_list.append(wallet)

# month
eval_year = 2021
eval_month = 4

for wallet in wallets_list:
  wallet_name = wallet.to_dict()['name']
  
  wallet_ref = db.collection(u'wallets').document(wallet_name).collection(u'activities')
  wallets_activities_ref = wallet_ref\
    .where(u'year', u'==', eval_year)\
    .where(u'month', u'==', eval_month)
  
  