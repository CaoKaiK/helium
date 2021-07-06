import os

from utils.firebase_connection import init
from utils.logger import get_logger

import pandas as pd

logger = get_logger('fifo')

db = init()
# load balance
balance_hnt_ref = db.collection(u'balance_hnt')

if True:
  # reset balance_hnt
  activities = db.collection(u'wallets').document(u'helium-wallet').collection(u'activities').stream()

  activities_list = []
  for activity in activities:
    activities_list.append(activity.to_dict())

  print(len(activities_list))
  prev_doc_id = ''
  add_fee_hnt = 0
  prev_fee_hnt = 0
  for activity in activities_list:
    # enter event in fifo list
    time = activity['time']
    doc_id = time.strftime('%Y-%m-%dT%H:%M:%S')

    if doc_id == prev_doc_id:
      add_fee_hnt += prev_fee_hnt

    prev_doc_id = doc_id

    height = activity['height']
    act_type = activity['type']

    if activity['amount'] > 0:
      fifo_to_allocate = activity['amount']
      fee_hnt = 0

    elif activity.get('fee_hnt', 0) > 0:
      fee_hnt = activity['fee_hnt']
      if add_fee_hnt > 0:
        fee_hnt += add_fee_hnt
        print('Correction worked')

        add_fee_hnt = 0
      
      prev_fee_hnt = fee_hnt
      
      fifo_to_allocate = -fee_hnt

    fifo_event = {
              'time': activity['time'],
              'year': activity['year'],
              'month': activity['month'],
              'day': activity['day'],
              'height': height,
              'amount': activity['amount'],
              'fee_hnt': fee_hnt,
              'fee_usd': activity.get('fee_usd',0),
              'fifo_to_allocate': fifo_to_allocate,
              'price': activity['price'],
              'type': act_type
            }
          
    fifo_event_ref = balance_hnt_ref.document(doc_id)
    fifo_event_ref.set(fifo_event)  