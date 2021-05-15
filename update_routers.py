from logging import log
from utils.helium_api import get_activities
from utils.firebase_connection import init
from utils.classify_activity import classify_activity
from utils.logger import get_logger

logger = get_logger('update_routers')

db = init()

# load config file and stream routers
config_ref = db.collection(u'config').document(u'config')
config = config_ref.get()
routers = db.collection(u'routers').stream()

# read overwrite flag
overwrite = config.get('routers_overwrite')
if overwrite:
  logger.warning(f'Overwrite: {overwrite}')

# iterate routers
for router in routers:
  hotspot_name = router.to_dict()['name']
  hotspot_address = router.to_dict()['address']
  logger.debug(f'### Router: {hotspot_name} ###')

  cursor = ''
  activities = []

  # initiate first call and repeat until no cursor is returned
  while not activities or cursor:
    activities, cursor = get_activities(hotspot_address, cursor, get='hotspot')

    logger.debug(f'Cursor was returned: {cursor}')

    for activity in activities:
      # arrange activity dict according to type
      activity = classify_activity(activity)

      height = activity.get('height')
      hash_act = activity.get('hash')

      # firebase db document ref
      event_ref = db.collection(u'routers').document(hotspot_name).collection(u'activities').document(f'{height}_{hash_act}')
      event = event_ref.get()

      # create documents if it doesn't exist
      # overwrite document if overwrite is true
      if (not event.exists or overwrite):
        if activity:
          act_type = activity.get('type')
          logger.info(f'Entry: {act_type} on block {height}')
          event_ref.set(activity)
        else:
          logger.info('Skipping known activity')
      else:
        logger.debug('### router completed ###')
        # set cursor to empty to break while loop
        cursor = ''
        # break activity loop
        break

# reset overrite flag
if overwrite:
  config_ref.update({u'routers_overwrite': False})
  logger.warning('Overwrite: False')

logger.info('--------------------------------------------------------')