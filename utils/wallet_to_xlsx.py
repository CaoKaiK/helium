import os
import pandas as pd

from .firebase_connection import init


db = init()

wallets = db.collection(u'wallets').stream()

wallets_list = []
for wallet in wallets:
  wallets_list.append(wallet)

# pick specific wallet or comment
wallet = wallets_list[0]
wallet_name = wallet.to_dict()['name']

wallet_ref = db.collection(u'wallets').document(wallet_name).collection(u'activities')
wallets_activities = wallet_ref.get()

activity_list_dicts = []

for activity in wallets_activities:
  activity_list_dicts.append(activity.to_dict())

df = pd.DataFrame(activity_list_dicts)
df['time'] = df['time'].dt.tz_localize(None)

df.to_excel('wallet.xlsx', index=False)
