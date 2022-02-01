from datetime import datetime as dt

from numpy import trapz
from requests.api import get

from utils.helium_api import get_height
from utils.firebase_connection import init

# info
transaction_datetime = dt(2021, 12, 31, 22, 19, 0)
amount_hnt = 11.93719131
usd_hnt = 38.2249
usd_eur = 0.877824
transaction_amount = 400.55
transaction_fee = 6.97

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
  'price_usd': usd_hnt,
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