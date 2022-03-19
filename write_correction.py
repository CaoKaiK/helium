from datetime import datetime as dt

from numpy import trapz
from requests.api import get

from utils.helium_api import get_height
from utils.firebase_connection import init

height = 1206790
day = 31
month = 1
year = 2022

amount_eur = [0.01, 0.01]
target = [2726, 1100]
origin = [1500, 2725]
description = [
  'Typ 2|Neutral|0.0004 HNT|Block 1202242|Kurs Ein 24.521 €/HNT',
  'Typ 2|Gewinn|0.0004 HNT|Block 1202242|Kurs Aus 24.880 €/HNT',
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