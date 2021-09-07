from datetime import datetime as dt

from numpy import trapz
from requests.api import get

from utils.helium_api import get_height
from utils.firebase_connection import init

# info
transaction_datetime = dt(2021, 8, 30, 20, 44, 0)
amount_hnt = 50
usd_hnt = 21.7181
usd_eur = 0.85382514
transaction_fee = 9.59
transaction_amount = 927.18

print(transaction_datetime)

db = init()
balance_hnt_ref = db.collection(u'balance_hnt')

doc_id = transaction_datetime.strftime('%Y-%m-%dT%H:%M:%S')
transaction_ref = balance_hnt_ref.document(doc_id)

transaction = {
  'type': 'exchange',
  'amount': int(amount_hnt*1e8),
  'fifo_to_allocate': -int(amount_hnt*1e8),
  'price': usd_hnt,
  'usd_eur': usd_eur,
  'year': transaction_datetime.year,
  'month': transaction_datetime.month,
  'day': transaction_datetime.day,
  'time': transaction_datetime,
  'fee_hnt': 0,
  'fee_usd': 0,
  'from': 'HNT',
  'height': get_height(transaction_datetime),
  'transaction_amount': transaction_amount,
  'transaction_fee_eur': transaction_fee
}

transaction_ref.set(transaction)  