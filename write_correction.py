from datetime import datetime as dt

from numpy import trapz
from requests.api import get

from utils.helium_api import get_height
from utils.firebase_connection import init

height = 1155205
day = 27
month = 12
year = 2021

amount_eur = [0.15, 0.18]
target = [2726, 1502]
origin = [1501, 2725]
description = [
  'Umtausch|Neutral|0.0049 HNT|Block 1147707|Kurs Ein 30.491 €/HNT',
  'Umtausch|Gewinn|0.0049 HNT|Block 1147707|Kurs Aus 36.798 €/HNT'
]

db = init()

doc_id = str(height).zfill(10)

correction_ref = db.collection(u'fifo').document(u'C&R').collection(u'corrections').document(doc_id)

correction = {
  'type': 'correction',
  'height': height,
  'year': year,
  'month': month,
  'day': day,
  'amount_eur': amount_eur,
  'origin': origin,
  'target': target,
  'description': description
}

correction_snap = correction_ref.get()

if not correction_snap.exists:
  correction_ref.set(correction)
else:
  print('already exists')