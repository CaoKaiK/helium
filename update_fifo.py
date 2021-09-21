from copy import deepcopy
from logging import log

from pandas.io.sql import table_exists
from utils.helium_api import get_account
from utils.firebase_connection import init
from utils.logger import get_logger

import pandas as pd

def rounded_sum(value, collector, precision):
  # rounds a value to required precision
  rounded_value = round(value, precision)
  diff = value - rounded_value

  collector += diff
  # and return the rounded value and increments the omitted amount
  # as a collector var
  return rounded_value, collector

# daily log files send to fifo directory
logger = get_logger('fifo')

db = init()

# define list of accounts to update
#  u'C&R' u'Temporary',
account_list = [u'C&R']

# stream fifo accounts
fifo_accounts = db.collection(u'fifo').where(u'name', u'in', account_list).stream()

for fifo_account in fifo_accounts:
  # account name
  fifo_account_name = fifo_account.to_dict().get('name')
  fifo_account_wallets = fifo_account.to_dict().get('wallets')
  fifo_events_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'balance')
  fifo_exchanges_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'exchanges')

  # get all outgoing events that are not Committed yet and already posses a eur price
  # fees from services, exchange
  fifo_events_out = fifo_events_ref.where(u'committed', u'==', False)\
    .where(u'has_eur', u'==', True)\
      .where(u'fifo_to_allocate', u'<', 0).get() #outgoing

  if fifo_events_out:
    fifo_events_out_list = []
    for fifo_event_out in fifo_events_out:
      fifo_events_out_list.append(fifo_event_out.to_dict())
    
    fifo_events_out_df = pd.DataFrame(fifo_events_out_list).sort_values(by=['time'])
  else:
    fifo_events_out_df = pd.DataFrame()
  
  # load exchanges
  # these entries will always have an realized eur/usd value
  fifo_exchanges = fifo_exchanges_ref.where(u'committed', u'==', False)\
    .where(u'use', u'==', True)\
      .where(u'fifo_to_allocate', u'<', 0).get() # 

  if fifo_exchanges:
    fifo_exchanges_out_list = []
    for fifo_exchange in fifo_exchanges:
      fifo_exchanges_out_list.append(fifo_exchange.to_dict())
    
    fifo_exchange_df = pd.DataFrame(fifo_exchanges_out_list)

    fifo_events_out_df = fifo_events_out_df.append(fifo_exchange_df).sort_values(by=['height'])

  # get all ingoing events that are not committed yet: 
  # mining, incoming transactions

  fifo_events_in = fifo_events_ref.where(u'committed', u'==', False)\
    .where(u'has_eur', u'==', True)\
      .where(u'fifo_to_allocate', u'>', 0).get() # ingoing

  if fifo_events_in:
    fifo_events_in_list = []
    for fifo_event_in in fifo_events_in:
      fifo_events_in_list.append(fifo_event_in.to_dict())
    
    fifo_events_in_df = pd.DataFrame(fifo_events_in_list).sort_values(by=['height'])
  else:
    fifo_events_in_df = pd.DataFrame()
  
  # get all transactions that are not yet fully accounted
  # for with cost neutral amounts from exchanges
  transactions = fifo_events_ref.where(u'type', u'==', 'transaction').get()
  
  if transactions:
    transaction_list = []
    for transaction in transactions:
      if transaction.to_dict()['amount'] < 0:
        transaction_list.append(transaction.to_dict())
    
    transactions_df = pd.DataFrame(transaction_list).sort_values(by=['height'])
  else:
    transactions_df = pd.DataFrame()

  # running balance for checksum
  fifo_out = 0
  fifo_in = 0

  # error flag
  error_flag = False

  # staged events
  staged_events_in_list = []
  staged_events_out_list = []
  staged_transactions = []

  for index_out, event_out in fifo_events_out_df.iterrows():
    # get event ref for fifo changes
    event_out_id = str(event_out['height']).zfill(10) + '_' + event_out['wallet_id']
    event_out_ref = fifo_events_ref.document(event_out_id)

    # fifo to allocate from outgoing events
    event_out_height = event_out['height']
    event_out_type = event_out['type']
    event_out_fifo = event_out['fifo_to_allocate'] # needs to be allocated
    realized_price_usd = event_out['price_usd']
    realized_price_eur = event_out['price_eur']
    
    fifo_out += -event_out_fifo
    
    event_out_height_str = str(event_out_height).rjust(10, ' ')
    required_str = str(-event_out_fifo).rjust(12, ' ')
    logger.debug(f'{fifo_account_name} - Staged - Outgoing Event <== Block {event_out_height_str} - Required: {required_str} - Price {realized_price_usd:.2f}$/HNT {realized_price_eur:.2f} Eur/HNT - {event_out_type}')

    # cost neutral
    cost_neutral = []
    # summed amount smaller 0.00
    cost_neutral_corr_usd = 0
    cost_neutral_corr_eur = 0

    # realized earnings/losses
    realized = []
    # summed earnings smaller 0.00Eur
    realized_corr_usd = 0
    realized_corr_eur = 0

    for index_in, event_in in fifo_events_in_df.iterrows():
      # get event in ref for fifo changes
      event_in_id = str(event_in['height']).zfill(10) + '_' + event_in['wallet_id']

      # fifo available for allocation from incoming event
      event_in_height = event_in['height']
      event_in_fifo = event_in['fifo_to_allocate']
      purchase_price_usd = event_in['price_usd']
      purchase_price_eur = event_in['price_eur']

      # original query already filters for fifo > 0
      # however, if multiple outgoing event occur in the same run
      # the fifo_events_in_df will keep track of the current values
      if event_in_fifo > 0:
        # fifo required is greater than fifo available in this incoming event
        if event_out_fifo + event_in_fifo < 0:
          # cost neutral amount
          rounded_neutral_usd, cost_neutral_corr_usd = rounded_sum(event_in_fifo * purchase_price_usd / 1e8, cost_neutral_corr_usd, 3)
          rounded_neutral_eur, cost_neutral_corr_eur = rounded_sum(event_in_fifo * purchase_price_eur / 1e8, cost_neutral_corr_eur, 3)

          if rounded_neutral_usd > 0 or rounded_neutral_eur > 0:
            cost_neutral.append({
              'amount_usd': rounded_neutral_usd,
              'amount_eur': rounded_neutral_eur,
              'height': event_in_height,
              'amount_hnt': event_in_fifo
              })

            rounded_realized_usd, realized_corr_usd = rounded_sum(event_in_fifo * (realized_price_usd - purchase_price_usd) / 1e8, realized_corr_usd, 3)
            rounded_realized_eur, realized_corr_eur = rounded_sum(event_in_fifo * (realized_price_eur - purchase_price_eur) / 1e8, realized_corr_eur, 3)
            
            realized.append({
              'amount_usd': rounded_realized_usd,
              'amount_eur': rounded_realized_eur,
              'height': event_in_height,
              'price_usd': purchase_price_usd,
              'price_eur': purchase_price_eur})

          
          # update remaining amount
          event_out_fifo += event_in_fifo
          fifo_in += event_in_fifo

          # checkpoint
          if event_in_height > event_out_height:
            # using fifo that did not exists at outgoing event
            logger.error(f'{fifo_account_name} - Incoming Event: {event_in_height} > Outgoing Event: {event_out_height}')
            error_flag = True
          
          # stage event_in to df
          fifo_events_in_df.loc[index_in, 'event_id'] = event_in_id
          fifo_events_in_df.loc[index_in, 'fifo_to_allocate'] = 0
          fifo_events_in_df.loc[index_in, 'committed'] = True

          # stage to list for commit
          staged_events_in_list.append({
            'event_id': event_in_id,
            'fifo_to_allocate': 0,
            'committed' : True
          })

          event_in_height_str = str(event_in_height).rjust(10, ' ')
          used_str = str(event_in_fifo).rjust(12, ' ')
          remaining_str = str(0).rjust(12, ' ')
          logger.debug(f'{fifo_account_name} - Staged - Incoming Event ==> Block {event_in_height_str} - Used:     {used_str} - Remaining: {remaining_str} - Price {purchase_price_usd:.2f}$/HNT {purchase_price_eur:.2f} Eur/HNT - Complete fill')
          
        # fifo required is smaller or equal fifo available in this event_in
        # also used for clean up and break -> going to next event_out
        else:
          # cost neutral amount
          rounded_neutral_usd, cost_neutral_corr_usd = rounded_sum(-event_out_fifo * purchase_price_usd / 1e8, cost_neutral_corr_usd, 3)
          rounded_neutral_eur, cost_neutral_corr_eur = rounded_sum(-event_out_fifo * purchase_price_eur / 1e8, cost_neutral_corr_eur, 3)

          if rounded_neutral_usd > 0 or rounded_neutral_usd > 0:
            cost_neutral.append({
              'amount_usd': rounded_neutral_usd,
              'amount_eur': rounded_neutral_eur,
              'height': event_in_height,
              'amount_hnt': -event_out_fifo
              })
            
            rouned_realized_usd, realized_corr_usd = rounded_sum(-event_out_fifo * (realized_price_usd - purchase_price_usd) / 1e8, realized_corr_usd, 3)
            rouned_realized_eur, realized_corr_eur = rounded_sum(-event_out_fifo * (realized_price_eur - purchase_price_eur) / 1e8, realized_corr_eur, 3)
            
            realized.append({
              'amount_usd': rouned_realized_usd,
              'amount_eur': rouned_realized_eur,
              'height': event_in_height,
              'price_usd': purchase_price_usd,
              'price_eur': purchase_price_eur
              })

          # update remaining fifo in last event_in
          event_in_fifo += event_out_fifo
          fifo_in += -event_out_fifo

          # checkpoint
          if event_in_height > event_out_height:
            # using fifo that did not exists at outgoing event
            logger.error(f'Incoming Event: {event_in_height} > Outgoing Event: {event_out_height}')
            error_flag = True

          # stage event_in to df
          fifo_events_in_df.loc[index_in, 'event_id'] = event_in_id
          fifo_events_in_df.loc[index_in, 'fifo_to_allocate'] = event_in_fifo

          # stage to list for commit
          staged_events_in_list.append({
            'event_id': event_in_id,
            'fifo_to_allocate': event_in_fifo,
            'partial': True
          })

          event_in_height_str = str(event_in_height).rjust(10, ' ')
          used_str = str(-event_out_fifo).rjust(12, ' ')
          remaining_str = str(event_in_fifo).rjust(12, ' ')
          logger.debug(f'{fifo_account_name} - Staged - Incoming Event ==> Block {event_in_height_str} - Used:     {used_str} - Remaining: {remaining_str} - Price {purchase_price_usd:.2f}$/HNT {purchase_price_eur:.2f} Eur/HNT - Partial fill')

          # # add all collection bookings
          # if round(cost_neutral_corr, 2) > 0:
          #   cost_neutral.append({
          #     'type': 'collection',
          #     'amount': round(cost_neutral_corr, 2)
          #   })
          
          # if round(realized_corr, 2) > 0:
          #   realized.append({
          #     'type': 'collection',
          #     'amount': round(realized_corr, 2)
          #   })

          # stage event_out to df
          fifo_events_out_df.loc[index_out, 'event_id'] = event_out_id
          fifo_events_out_df.loc[index_out, 'fifo_to_allocate'] = 0

          # if event comes from exchange table
          # copy document and extend dict for creating new event
          # during commit
          if event_out['type'] == 'exchange':
            event_out_snap = fifo_exchanges_ref.document(event_out_id.replace('_', '')).get()
            
            event_out_dict = event_out_snap.to_dict()

            # update previous transactions
            # fill transaction amount with cost neutral
            cost_neutral_copy = deepcopy(cost_neutral)

            for index, transaction in transactions_df.iterrows():
              transaction_id = str(transaction['height']).zfill(10) + '_' + transaction['wallet_id']
              cost_neutral_amount_usd = 0
              cost_neutral_amount_eur = 0
              transfer_amount_init = transaction['amount'] 
              transfer_amount = transaction.get('fifo_neutral', transfer_amount_init)

              # fill transaction until no events in this exchange
              # or transfer amount = 0 for this transaction
              while cost_neutral_copy and transfer_amount < 0:
                neutral_event = cost_neutral_copy.pop(0)

                # reduce transfer amount by neutral event HNT consumption
                # transfer amount is negative
                transfer_amount += neutral_event['amount_hnt']

                # event has more HNT than required cut and save
                # rest for next transaction
                if transfer_amount > 0:
                  partial_amount = transfer_amount 
                  transfer_amount = 0
                  # calculate remaining neutral amounts for remaining HNT
                  partial_amount_usd = partial_amount / neutral_event['amount_hnt'] * neutral_event['amount_usd']
                  partial_amount_eur = partial_amount / neutral_event['amount_hnt'] * neutral_event['amount_eur']

                  neutral_event['amount_usd'] -= partial_amount_usd
                  neutral_event['amount_eur'] -= partial_amount_eur
                  # insert at beginning of cost_neutral_copy list
                  # for next transaction
                  cost_neutral_copy.insert(0,{
                    'amount_hnt': partial_amount,
                    'amount_usd': partial_amount_usd,
                    'amount_eur': partial_amount_eur
                  })
                  
                # add cost neutral amounts to counter
                cost_neutral_amount_usd += neutral_event['amount_usd']
                cost_neutral_amount_eur += neutral_event['amount_eur']

              # conclude transactions
              transactions_df.loc[index, 'fifo_neutral'] = transfer_amount

              if cost_neutral_amount_usd > 0 or cost_neutral_amount_eur > 0:
                transaction_dict = {
                  'event_id': transaction_id,
                  'cost_neutral_amount_usd': round(cost_neutral_amount_usd, 3),
                  'cost_neutral_amount_eur': round(cost_neutral_amount_eur, 3),
                  'fifo_neutral': transfer_amount
                }
                staged_transactions.append(transaction_dict)
                logger.debug(f'{fifo_account_name} - Staged - Cost Neutral Amount {transfer_amount} HNT for Transaction {transaction_id} - {cost_neutral_amount_usd:.3f}USD | {cost_neutral_amount_eur:.3f} EUR')

          else:
            event_out_dict = {}


          event_out_dict['event_id'] = event_out_id
          event_out_dict['fifo_to_allocate'] = 0
          event_out_dict['committed'] = True
          event_out_dict['cost_neutral'] = cost_neutral

          event_out_dict['realized'] = realized


          # stage to list for commit
          staged_events_out_list.append(event_out_dict)

          # break event in for loop
          break
        
  # sanity checks

  # remaining FIFO In
  if not fifo_events_in_df.empty:
    remaining_hnt = int(fifo_events_in_df[fifo_events_in_df['fifo_to_allocate'] > 0]['fifo_to_allocate'].sum())
  else:
    remaining_hnt = 0
    logger.warning(f'{fifo_account_name} - No Incoming Events - Not Committing')
    

  # unfullfilled FIFO Out
  fifo_exchanges_unfullfilled = fifo_exchanges_ref.where(u'committed', u'==', False)\
    .where(u'use', u'==', False)\
      .where(u'fifo_to_allocate', u'<', 0).get()
  
  open_hnt = 0
  for fifo_unfullfilled in fifo_exchanges_unfullfilled:
    open_hnt += fifo_unfullfilled.to_dict()['fifo_to_allocate']

  # check that amount in equals amount out
  logger.info(f'{fifo_account_name} - Staged in:   {fifo_in/1e8:.8f}')
  logger.info(f'{fifo_account_name} - Staged out:  {fifo_out/1e8:.8f}')

  if fifo_in != fifo_out:
    error_flag = True
    logger.warning(f'{fifo_account_name} - Unequal FIFO Amount - Not Committing')

  # get events that have been skipped due to missing eur value
  fifo_events_skipped = fifo_events_ref.where(u'committed', u'==', False)\
    .where(u'has_eur', u'==', False).get() #outgoing and ingoing
  
  remaining_in = 0
  remaining_out = 0
  for event_skipped in fifo_events_skipped:
    event_skipped = event_skipped.to_dict()
    if event_skipped['fifo_to_allocate'] > 0:
      remaining_in += event_skipped['fifo_to_allocate']
    else:
      remaining_out += event_skipped['fifo_to_allocate']



  # get account for balance
  total_balance = 0
  for wallet_address in fifo_account_wallets:
    account = get_account(wallet_address)
    balance = account['balance']
    total_balance += balance

    balance_str = str(round(balance/1e8, 8)).rjust(12, ' ')
    logger.info(f'{fifo_account_name} - Wallet Balance:     {balance_str}')

  total_balance_str = str(round(total_balance/1e8, 8)).rjust(12, ' ')
  remaining_hnt_str = str(round(remaining_hnt/1e8, 8)).rjust(12, ' ')
  open_hnt_str = str(round(-open_hnt/1e8, 8)).rjust(12, ' ')
  remaining_in_str = str(round(remaining_in/1e8, 8)).rjust(12, ' ')
  remaining_out_str = str(round(-remaining_out/1e8, 8)).rjust(12, ' ')
  

  logger.info(f'{fifo_account_name} -                    =============')
  logger.info(f'{fifo_account_name} - Total Balance:      {total_balance_str}')
  logger.info(f'{fifo_account_name} - FIFO Remaining:     {remaining_hnt_str}')
  logger.info(f'{fifo_account_name} - FIFO Required:     -{open_hnt_str}')
  logger.info(f'{fifo_account_name} - Open Ingoing:       {remaining_in_str}')
  logger.info(f'{fifo_account_name} - Open Outgoing:     -{remaining_out_str}')


  logger.info(f'{fifo_account_name} -                    =============')

  missing_amount = (remaining_hnt + remaining_in + remaining_out + open_hnt) - total_balance 
  missing_amount_str = str(round(missing_amount/1e8, 8)).rjust(12, ' ')
  logger.info(f'{fifo_account_name} - Missing:            {missing_amount_str}')

  if missing_amount != 0:
    error_flag = True
    logger.warning(f'{fifo_account_name} - Missing Amount Error - Not Committing')


  debug = pd.DataFrame(staged_events_out_list)




  if not error_flag:
    # commit transactions
    for transaction_commit in staged_transactions:
      transaction_id = transaction_commit['event_id']
      print(transaction_commit)
      transaction_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'balance').document(transaction_id)

      transaction = transaction_ref.get()
      previous_amount_usd = transaction.to_dict().get('cost_neutral_amount_usd', 0)
      previous_amount_eur = transaction.to_dict().get('cost_neutral_amount_eur', 0)

      cost_neutral_amount_usd = previous_amount_usd + transaction_commit['cost_neutral_amount_usd']
      cost_neutral_amount_eur = previous_amount_eur + transaction_commit['cost_neutral_amount_eur']

      logger.debug(f'{fifo_account_name} - Try Commit Transaction - {transaction_id}')

      transaction_ref.update({
            'cost_neutral_amount_usd': cost_neutral_amount_usd,
            'cost_neutral_amount_eur': cost_neutral_amount_eur,
            'fifo_neutral': transaction_commit['fifo_neutral'] 
          })
    # commit to db
    for event_out_commit in staged_events_out_list:
      event_out_id = event_out_commit['event_id']
      event_out_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'balance').document(event_out_id)

      event_out = event_out_ref.get()

      logger.debug(f'{fifo_account_name} - Try Commit - {event_out_id}')

      # event already exists
      # e.g. service fees
      if event_out.exists:
        # do not overwrite or update committed events
        if not event_out.to_dict()['committed']:
          event_out_ref.update(event_out_commit)
          logger.info(f'{fifo_account_name} - Commit Existing Event Out - {event_out_id}')

        else:
          logger.warning(f'{fifo_account_name} - Failed Event Out- {event_out_id} - Already committed')
      # external event
      # e.g. exchange
      else:
        # write result to balance
        event_out_ref.set(event_out_commit)
        logger.info(f'{fifo_account_name} - Commit Exchange Event to Event Out - {event_out_id}')

        event_out_exchange_ref = fifo_exchanges_ref.document(event_out_id.replace('_', ''))
        # update event in exchange
        event_out_exchange_ref.update({
          'fifo_to_allocate': 0,
          'committed': True
        })
        logger.info(f'{fifo_account_name} - Commit Exchange Event- {event_out_id}')
        
    
    for event_in_commit in staged_events_in_list:
      event_in_id = event_in_commit['event_id']
      event_in_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'balance').document(event_in_id)

      event_in = event_in_ref.get()

      # do not overwrite or update committed events
      # unless it is part of a partial transaction
      if not event_in.to_dict()['committed'] or event_in.to_dict().get('partial', True):
        event_in_ref.update(event_in_commit)
        logger.info(f'{fifo_account_name} - Commit Event In - {event_in_id}')
      else:
        logger.warning(f'{fifo_account_name} - Failed Commit In - {event_in_id} - Already committed')

