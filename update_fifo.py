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

  # earnings
  realized_earnings = []
  # summed earnings smaller 0.00$
  earnings_cor = 0

  # losses
  realized_losses = []
  # summed losses smaller 0.00$
  losses_cor = 0

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
      rounded_neutral, cost_neutral_corr = rounded_sum(available_fifo * purchase_price / 1e8, cost_neutral_corr, 2)
      if rounded_neutral > 0:
        cost_neutral.append(rounded_neutral)
      # losses
      if purchase_price > realized_price:
        rounded_loss, losses_cor = rounded_sum(available_fifo * (purchase_price - realized_price) / 1e8, losses_cor, 2)
        if rounded_loss > 0:
          realized_losses.append(rounded_loss)
      # earnings
      elif purchase_price < realized_price:
        rounded_earning, earnings_cor = rounded_sum(available_fifo * (realized_price - purchase_price) / 1e8, earnings_cor, 2)
        if rounded_earning > 0:
          realized_earnings.append(rounded_earning)
      else:
        # no addition to losses or earnings
        None
      
      # update remaining amount
      fifo_to_allocate += available_fifo

      logger.debug(f'Part filled - {doc_id} - {height} - Used: {available_fifo} - Remaining: {0} - {purchase_price}')

      # update event_in
      event_in_ref.update({u'fifo_to_allocate': 0})
      # update in dataframe
      balance_in_df = balance_in_df.drop([index])

    # fifo required is smaller or equal fifo available in this event_in
    # also used for clean up and break -> going to next event_out
    else:
      # cost neutral amount
      rounded_neutral, cost_neutral_corr = rounded_sum(-fifo_to_allocate * purchase_price / 1e8, cost_neutral_corr, 2)
      if rounded_neutral > 0:
        cost_neutral.append(rounded_neutral)
      # losses
      if purchase_price > realized_price:
        rounded_loss, losses_cor = rounded_sum(-fifo_to_allocate * (purchase_price - realized_price) / 1e8, losses_cor, 2)
        if rounded_loss > 0:
          realized_losses.append(rounded_loss)
      # earnings
      elif purchase_price < realized_price:
        rounded_earning, earnings_cor = rounded_sum(-fifo_to_allocate * (realized_price - purchase_price) / 1e8, earnings_cor, 2)
        realized_earnings.append({
          'height': event_in['height'],
          'amount': rounded_earning
        })
      else:
        # no addition to losses or earnings
        None
      
      # update remaining fifo in last event_in
      available_fifo += fifo_to_allocate

      logger.debug(f'Fully filled - {doc_id} - {height} - Used: {-fifo_to_allocate} - Remaining: {available_fifo} - {purchase_price}')

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
      
      if round(losses_cor, 2) > 0:
        realized_losses.append({
          'type': 'collection',
          'amount': round(losses_cor, 2)
        })
      
      if round(earnings_cor, 2) > 0:
        realized_earnings.append({
          'type': 'collection',
          'amount': round(earnings_cor, 2)
        })

      # update event out
      event_out_ref.update({
        u'fifo_to_allocate': 0,
        u'cost_neutral': cost_neutral,
        u'realized_earnings': realized_earnings,
        u'realized_losses': realized_losses
        })
      

      break

# get updated balance
balance_in = balance_hnt_ref.where(u'fifo_to_allocate', u'>', 0).get()

balance_in_list = []
for balance_event in balance_in:
  balance_in_list.append(balance_event.to_dict())
    
balance_in_df = pd.DataFrame(balance_in_list)

remaining_hnt = balance_in_df['fifo_to_allocate'].sum()

logger.info(f'Company account should still have {remaining_hnt} Bones')
account = get_account('14pomxxicuQUXsnsL7Ew6R6tJ2VbYJsidgLBwCNHfRGgdpJ9bc8')
balance = account['balance']
logger.info(f'Helium account currently holds {balance} Bones')