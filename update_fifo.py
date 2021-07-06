import os

from utils.helium_api import get_account
from utils.firebase_connection import init
from utils.logger import get_logger

import pandas as pd

def rounded_sum(value, collector, precision):
  rounded_value = round(value, precision)
  diff = value - rounded_value

  collector += diff

  return rounded_value, collector

logger = get_logger('fifo')

db = init()

# load balance
balance_hnt_ref = db.collection(u'balance_hnt')

# balance in events - mining, incomining transactions
balance_in = balance_hnt_ref.where(u'fifo_to_allocate', u'>', 0).get()

balance_in_list = []
for balance_event in balance_in:
  balance_in_list.append(balance_event.to_dict())

balance_in_df = pd.DataFrame(balance_in_list)
balance_in_df = balance_in_df.sort_values(by=['time'])

# balance out events - fees, exchange
balance_out = balance_hnt_ref.where(u'fifo_to_allocate', u'<', 0).get()
if balance_out:
  balance_out_list = []
  for balance_event in balance_out:
    balance_out_list.append(balance_event.to_dict())

  balance_out_df = pd.DataFrame(balance_out_list)
  balance_out_df = balance_out_df.sort_values(by=['time'])
else:
  balance_out_df = pd.DataFrame()

for index, event in balance_out_df.iterrows():
  # get event ref for fifo changes
  doc_id = event['time'].strftime('%Y-%m-%dT%H:%M:%S')
  event_out_ref = balance_hnt_ref.document(doc_id)

  # fifo to allocate with incoming events
  fifo_to_allocate = event['fifo_to_allocate']
  realized_price = event['price']
  event_type = event['type']

  logger.info(f'Outgoing Event - {doc_id} - {event_type} - Required: {-fifo_to_allocate} - {realized_price}')

  
  # cost neutral
  cost_neutral = []
  # summed amount smaller 0.00$
  cost_neutral_corr = 0

  # realized earnings/losses
  realized = []
  # summed earnings smaller 0.00$
  realized_corr = 0

  for index, event_in in balance_in_df.iterrows():
    # get event in ref for fifo changes
    doc_id = event_in['time'].strftime('%Y-%m-%dT%H:%M:%S')
    event_in_ref = balance_hnt_ref.document(doc_id)
    height = event_in['height']

    available_fifo = event_in['fifo_to_allocate']
    purchase_price = event_in['price']

    # fifo required is greater than fifo available in this event_in
    if fifo_to_allocate + available_fifo < 0:
      # cost neutral amount
      rounded_neutral, cost_neutral_corr = rounded_sum(available_fifo * purchase_price / 1e8, cost_neutral_corr, 3)
      if rounded_neutral > 0:
        cost_neutral.append({'amount': rounded_neutral, 'height': height, 'amount_hnt': available_fifo})

        rounded_realized, realized_corr = rounded_sum(available_fifo * (realized_price - purchase_price) / 1e8, realized_corr, 3)
        realized.append({'amount': rounded_realized, 'height': height, 'price': purchase_price})
      
      # update remaining amount
      fifo_to_allocate += available_fifo

      logger.debug(f'Partial fill - {doc_id} - {height} - Used: {available_fifo} - Remaining: 0 - {purchase_price}')

      # update event_in
      event_in_ref.update({u'fifo_to_allocate': 0})
      # update in dataframe
      balance_in_df = balance_in_df.drop([index])

    # fifo required is smaller or equal fifo available in this event_in
    # also used for clean up and break -> going to next event_out
    else:
      # cost neutral amount
      rounded_neutral, cost_neutral_corr = rounded_sum(-fifo_to_allocate * purchase_price / 1e8, cost_neutral_corr, 3)
      if rounded_neutral > 0:
        cost_neutral.append({'amount': rounded_neutral, 'height': height, 'amount_hnt': -fifo_to_allocate})
        
        rouned_realized, realized_corr = rounded_sum(-fifo_to_allocate * (realized_price - purchase_price) / 1e8, realized_corr, 3)
        realized.append({'amount': rouned_realized, 'height': height, 'price': purchase_price})

      
      # update remaining fifo in last event_in
      available_fifo += fifo_to_allocate

      logger.debug(f'Complete fill - {doc_id} - {height} - Used: {-fifo_to_allocate} - Remaining: {available_fifo} - {purchase_price}')

      # update event in database
      event_in_ref.update({u'fifo_to_allocate': available_fifo})
      # update in dataframe
      balance_in_df.loc[index, 'fifo_to_allocate'] = available_fifo

      # add all collection bookings
      if round(cost_neutral_corr, 2) > 0:
        cost_neutral.append({
          'type': 'collection',
          'amount': round(cost_neutral_corr, 2)
        })
      
      if round(realized_corr, 2) > 0:
        realized_corr.append({
          'type': 'collection',
          'amount': round(realized_corr, 2)
        })

      # update event out
      event_out_ref.update({
        u'fifo_to_allocate': 0,
        u'cost_neutral': cost_neutral,
        u'realized': realized,
        })
      # break event in for loop
      break
  
  ### outgoing transaction ###
  # calculate pseudo fifo amount for cost neutral transfer
  if event_type == 'transaction':
    pseudo_fifo_amount = event['amount']

    pseudo_balance_in_df = balance_in_df.copy()

    # in USD
    pseudo_neutral_amount = 0

    for index, event_in in pseudo_balance_in_df.iterrows():
      height = event_in['height']

      available_fifo = event_in['fifo_to_allocate']
      purchase_price = event_in['price']

      # fifo required is greater than fifo available in this event_in
      if pseudo_fifo_amount + available_fifo < 0:
        # cost neutral amount - will be rounded at the end
        pseudo_neutral_amount += available_fifo * purchase_price / 1e8

        # update remaining amount
        pseudo_fifo_amount += available_fifo

        logger.debug(f'Pseudo partial fill - {height} - Used: {available_fifo} - Remaining: 0 - {purchase_price}')

        # update in dataframe
        pseudo_balance_in_df = pseudo_balance_in_df.drop([index])

      # fifo required is smaller or equal fifo available in this event_in
      # also used for clean up and break -> going to next event_out
      else:
        # cost neutral amount
        pseudo_neutral_amount += -pseudo_fifo_amount * purchase_price / 1e8

        # update remaining fifo in last event_in
        available_fifo += pseudo_fifo_amount

        logger.debug(f'Pseudo complete fill - {height} - Used: {-pseudo_fifo_amount} - Remaining: {available_fifo} - {purchase_price}')

        # update event out
        event_out_ref.update({
          u'pseudo_neutral_amount': round(pseudo_neutral_amount, 3)
          })
        # break event in for loop
        break
      



# get updated balance
balance_in = balance_hnt_ref.where(u'fifo_to_allocate', u'>', 0).get()

balance_in_list = []
for balance_event in balance_in:
  balance_in_list.append(balance_event.to_dict())
    
balance_in_df = pd.DataFrame(balance_in_list)

remaining_hnt = balance_in_df['fifo_to_allocate'].sum()

logger.info(f'Company account should still have {remaining_hnt} Bones')
account = get_account('14pomxxicuQUXsnsL7Ew6R6tJ2VbYJsidgLBwCNHfRGgdpJ9bc8') # 146AgmWsmksqHUUXT6n8HEgcScD5bhS52fzzRFK7srWYRX2nUBM
balance = account['balance']
logger.info(f'Helium account currently holds {balance} Bones')