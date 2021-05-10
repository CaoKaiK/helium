import os
from datetime import datetime

from utils.helium_api import get_activities
from utils.firebase_connection import init
from utils.classify_activity import classify_activity

db = init()

# list of routers
routers = db.collection(u'routers').stream()
wallets = db.collection(u'wallets').stream()

overwrite = False

for router in routers:
  hotspot_name = router.to_dict()['name']
  hotspot_address = router.to_dict()['address']
  print(f'### Router: {hotspot_name} ###')

  cursor = ''
  activities = []

  while not activities or cursor:
    activities, cursor = get_activities(hotspot_address, cursor, get='hotspot')

    print('.')

    for activity in activities:
      # arrange activity dict according to activity
      activity = classify_activity(activity)

      height = activity.get('height')
      hash_act = activity.get('hash')

      event_ref = db.collection(u'routers').document(hotspot_name).collection(u'activities').document(f'{height}_{hash_act}')

      event = event_ref.get()

      if (not event.exists or overwrite):
        if activity:
          act_type = activity.get('type')
          print(f'New entry: {act_type}')
          event_ref.set(activity)
      else:
        print('--- completed ---')
        print(f'--- latest block: {height} ---')
        cursor = ''
        break



# for wallet in wallets:
#   wallet = wallet.to_dict()
#   wallet_address = wallet.get('address')
#   wallet_name = wallet.get('name')
#   print(f'- {wallet_name} -')

#   cursor = ''
#   activities = []

#   while not activities or cursor:
#     activities, cursor = get_activities(wallet_address, cursor, get='wallet')