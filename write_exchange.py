from datetime import datetime as dt

from numpy import trapz
from requests.api import get

from utils.helium_api import get_height
from utils.firebase_connection import init

# info
transaction_datetime = dt(2021, 6, 1, 11, 53, 0)
amount_hnt = 149
usd_hnt = 15.8754
usd_eur = 0.818411
transaction_amount = 1935.9
transaction_fee = 0.8

print(transaction_datetime)
transaction_height = get_height(transaction_datetime)

db = init()

fifo_exchange_ref = db.collection(u'fifo').document(u'Temporary').collection(u'exchanges')

doc_id = str(transaction_height).zfill(10)
transaction_ref = fifo_exchange_ref.document(doc_id)

transaction = {
  'type': 'exchange',
  'amount': int(round(amount_hnt*1e8, 8)),
  'fifo_to_allocate': -int(round(amount_hnt*1e8, 8)),
  'price': usd_hnt,
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
  'committed': False
}

transaction_snap = transaction_ref.get()

if not transaction_snap.exists:
  transaction_ref.set(transaction)
else:
  print('already exists')