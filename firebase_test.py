import os
from datetime import datetime

from utils.helium_api import get_hotspot_activities
from utils.firebase_connection import init
from utils.classify_activity import classify_activity

db = init()

# list of routers
routers = db.collection(u'routers').stream()

overwrite = False

for router in routers:
  hotspot_name = router.to_dict()['name']
  hotspot_address = router.to_dict()['address']
  print(f'{hotspot_name}')

  cursor = ''
  activities = []


  while not activities or cursor:
    activities, cursor = get_hotspot_activities(hotspot_address, cursor)

    print('.', end='', flush=True)

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
          print(f'{act_type}')
          event_ref.set(activity)
      else:
        print(f'{height}')
        cursor = ''
        break

