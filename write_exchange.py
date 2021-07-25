from datetime import datetime as dt

from numpy import trapz
from requests.api import get

from utils.helium_api import get_height
from utils.firebase_connection import init

transaction_datetime = dt(2021, 7, 18, 15, 13, 0)

# amount HNT
amount_hnt = 183
usd_hnt = 11.6121
usd_eur = 0.856487
transaction_fee = 14.03
transaction_amount = 1820

print(transaction_datetime)

db = init()
balance_hnt_ref = db.collection(u'balance_hnt')

doc_id = transaction_datetime.strftime('%Y-%m-%dT%H:%M:%S')
transaction_ref = balance_hnt_ref.document(doc_id)

transaction = {
  'type': 'exchange',
  'amount': amount_hnt*1e8,
  'fifo_to_allocate': -amount_hnt*1e8,
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