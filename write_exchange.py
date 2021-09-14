from datetime import datetime as dt

from numpy import trapz
from requests.api import get

from utils.helium_api import get_height
from utils.firebase_connection import init

# info
transaction_datetime = dt(2021, 9, 30, 18, 44, 0)
amount_hnt = 70.85
usd_hnt = 21.7181
usd_eur = 0.853825
transaction_amount = 927.18
transaction_fee = 9.59

print(transaction_datetime)
#print(transaction_datetime.isoformat())
transaction_height = get_height(transaction_datetime)

print(transaction_height)

db = init()

fifo_exchange_ref = db.collection(u'fifo').document(u'C&R').collection(u'exchanges')

doc_id = str(transaction_height).zfill(10)
transaction_ref = fifo_exchange_ref.document(doc_id)

transaction = {
  'type': 'exchange',
  'amount': int(round(amount_hnt*1e8)),
  'fifo_to_allocate': -int(round(amount_hnt*1e8)),
  'price': usd_hnt,
  'price_eur': round(usd_hnt * usd_eur, 5),
  'usd_eur': usd_eur,
  'year': transaction_datetime.year,
  'month': transaction_datetime.month,
  'day': transaction_datetime.day,
  'time': transaction_datetime,
  'fee_hnt': 0,
  'fee_usd': 0,
  'from': 'HNT',
  'height': transaction_height,
  'transaction_amount': transaction_amount,
  'transaction_fee_eur': transaction_fee,
  'wallet_id': '',
  'committed': False,
  'use': False
}

transaction_snap = transaction_ref.get()

if not transaction_snap.exists:
  transaction_ref.set(transaction)
else:
  print('already exists')