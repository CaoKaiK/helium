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
#  
account_list = [u'C&R', u'Temporary']

# stream fifo accounts
fifo_accounts = db.collection(u'fifo').where(u'name', u'in', account_list).stream()

for fifo_account in fifo_accounts:
  # account name
  fifo_account_name = fifo_account.to_dict().get('name')
  fifo_account_wallets = fifo_account.to_dict().get('wallets')
  fifo_events_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'balance')
  fifo_exchanges_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'exchanges')

  # get all outgoing events that are not comitted yet:
  # fees from services, exchange
  fifo_events_out = fifo_events_ref.where(u'committed', u'==', False).where(u'fifo_to_allocate', u'<', 0).get()

  if fifo_events_out:
    fifo_events_out_list = []
    for fifo_event_out in fifo_events_out:
      fifo_events_out_list.append(fifo_event_out.to_dict())
    
    fifo_events_out_df = pd.DataFrame(fifo_events_out_list).sort_values(by=['time'])
  else:
    fifo_events_out_df = pd.DataFrame()
  
  # load exchanges
  fifo_exchanges = fifo_exchanges_ref.where(u'committed', u'==', False).where(u'fifo_to_allocate', u'<', 0).get()

  if fifo_exchanges:
    fifo_exchanges_out_list = []
    for fifo_exchange in fifo_exchanges:
      fifo_exchanges_out_list.append(fifo_exchange.to_dict())
    
    fifo_exchange_df = pd.DataFrame(fifo_exchanges_out_list)

    fifo_events_out_df = fifo_events_out_df.append(fifo_exchange_df).sort_values(by=['time'])

  # get all ingoing events that are not committed yet: 
  # mining, incoming transactions

  fifo_events_in = fifo_events_ref.where(u'committed', u'==', False).where(u'fifo_to_allocate', u'>', 0).get()
  if fifo_events_in:
    fifo_events_in_list = []
    for fifo_event_in in fifo_events_in:
      fifo_events_in_list.append(fifo_event_in.to_dict())
    
    fifo_events_in_df = pd.DataFrame(fifo_events_in_list).sort_values(by=['time'])
  else:
    fifo_events_in_df = pd.DataFrame()

  # running balance for checksum
  fifo_out = 0
  fifo_in = 0

  # error flag
  error_flag = False

  # staged events
  staged_events_in_list = []
  staged_events_out_list = []

  # empty pseudo fifo for transactions
  pseudo_fifo_events_in_df = pd.DataFrame()

  for index_out, event_out in fifo_events_out_df.iterrows():
    # get event ref for fifo changes
    event_out_id = str(event_out['height']).zfill(10) + '_' + event_out['wallet_id']
    event_out_ref = fifo_events_ref.document(event_out_id)

    # fifo to allocate from outgoing events
    event_out_height = event_out['height']
    event_out_type = event_out['type']
    event_out_fifo = event_out['fifo_to_allocate'] # needs to be allocated
    realized_price = event_out['price']
    
    fifo_out += -event_out_fifo
    
    event_out_height_str = str(event_out_height).rjust(10, ' ')
    required_str = str(-event_out_fifo).rjust(12, ' ')
    logger.debug(f'{fifo_account_name} - Staged - Outgoing Event <== Block {event_out_height_str} - Required: {required_str} - Price {realized_price:.2f} - {event_out_type}')

    # cost neutral
    cost_neutral = []
    # summed amount smaller 0.00$
    cost_neutral_corr = 0

    # realized earnings/losses
    realized = []
    # summed earnings smaller 0.00$
    realized_corr = 0

    for index_in, event_in in fifo_events_in_df.iterrows():
      # get event in ref for fifo changes
      event_in_id = str(event_in['height']).zfill(10) + '_' + event_in['wallet_id']

      # fifo available for allocation from incoming event
      event_in_height = event_in['height']
      event_in_fifo = event_in['fifo_to_allocate']
      purchase_price = event_in['price']

      # original query already filters for fifo > 0
      # however, if multiple outgoing event occur in the same run
      # the fifo_events_in_df will keep track of the current values
      if event_in_fifo > 0:
        # fifo required is greater than fifo available in this incoming event
        if event_out_fifo + event_in_fifo < 0:
          # cost neutral amount
          rounded_neutral, cost_neutral_corr = rounded_sum(event_in_fifo * purchase_price / 1e8, cost_neutral_corr, 3)
          if rounded_neutral > 0:
            cost_neutral.append({'amount': rounded_neutral, 'height': event_in_height, 'amount_hnt': event_in_fifo})

            rounded_realized, realized_corr = rounded_sum(event_in_fifo * (realized_price - purchase_price) / 1e8, realized_corr, 3)
            realized.append({'amount': rounded_realized, 'height': event_in_height, 'price': purchase_price})
          
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
          logger.debug(f'{fifo_account_name} - Staged - Incoming Event ==> Block {event_in_height_str} - Used:     {used_str} - Remaining: {remaining_str} - Price {purchase_price:.2f} - Complete fill')
          
        # fifo required is smaller or equal fifo available in this event_in
        # also used for clean up and break -> going to next event_out
        else:
          # cost neutral amount
          rounded_neutral, cost_neutral_corr = rounded_sum(-event_out_fifo * purchase_price / 1e8, cost_neutral_corr, 3)
          if rounded_neutral > 0:
            cost_neutral.append({'amount': rounded_neutral, 'height': event_in_height, 'amount_hnt': -event_out_fifo})
            
            rouned_realized, realized_corr = rounded_sum(-event_out_fifo * (realized_price - purchase_price) / 1e8, realized_corr, 3)
            realized.append({'amount': rouned_realized, 'height': event_in_height, 'price': purchase_price})

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
          logger.debug(f'{fifo_account_name} - Staged - Incoming Event ==> Block {event_in_height_str} - Used:     {used_str} - Remaining: {remaining_str} - Price {purchase_price:.2f} - Partial fill')

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
          
          # if event is a transaction calculate fifo amount
          # as if the transaction was an exchange
          # same function applies but fifo is not commited to events
          elif event_out['type'] == 'transaction':
            pseudo_fifo_amount = event_out['amount']

            if pseudo_fifo_events_in_df.empty:
              pseudo_fifo_events_in_df = fifo_events_in_df.copy()

            # in USD
            pseudo_neutral_amount = 0

            for index_in, event_in in pseudo_fifo_events_in_df.iterrows():
              height = event_in['height']

              available_fifo = event_in['fifo_to_allocate']
              purchase_price = event_in['price']

              # fifo required is greater than fifo available in this event_in
              if pseudo_fifo_amount + available_fifo < 0:
                # cost neutral amount - will be rounded at the end
                pseudo_neutral_amount += available_fifo * purchase_price / 1e8

                # update remaining amount
                pseudo_fifo_amount += available_fifo

                # update in dataframe
                pseudo_fifo_events_in_df = pseudo_fifo_events_in_df.drop([index_in])

              # fifo required is smaller or equal fifo available in this event_in
              # also used for clean up and break -> going to next event_out
              else:
                # cost neutral amount
                pseudo_neutral_amount += -pseudo_fifo_amount * purchase_price / 1e8

                # update remaining fifo in last event_in
                available_fifo += pseudo_fifo_amount

                # update event out
                fifo_events_out_df.loc[index_out, 'pseudo_neutral_amount'] = round(pseudo_neutral_amount, 3)
                event_out_dict['pseudo_neutral_amount'] = round(pseudo_neutral_amount, 3)

                # break event in for loop
                break
                
          else:
            event_out_dict = {}

          ### outgoing transaction ###
          # calculate pseudo fifo amount for cost neutral transfer


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

  # check that amount in equals amount out
  logger.info(f'{fifo_account_name} - Staged in:   {fifo_in/1e8:.8f}')
  logger.info(f'{fifo_account_name} - Staged out:  {fifo_in/1e8:.8f}')

  if fifo_in != fifo_out:
    error_flag = True

  # remaining fifo in should be equivalent to balance
  remaining_hnt = int(fifo_events_in_df[fifo_events_in_df['fifo_to_allocate'] > 0]['fifo_to_allocate'].sum())

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
  logger.info(f'{fifo_account_name} -                    =============')
  logger.info(f'{fifo_account_name} - Total Balance:      {total_balance_str}')
  logger.info(f'{fifo_account_name} - Remaining HNT:      {remaining_hnt_str}')

  logger.info(f'{fifo_account_name} -                    =============')
  missing_amount = remaining_hnt - total_balance
  missing_amount_str = str(round(missing_amount/1e8, 8)).rjust(12, ' ')
  logger.info(f'{fifo_account_name} - Missing:            {missing_amount_str}')

  if missing_amount != 0:
    error_flag = True

  if not error_flag:
    # commit to db
    for event_out_commit in staged_events_out_list:
      event_out_id = event_out_commit['event_id']
      event_out_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'balance').document(event_out_id)

      event_out = event_out_ref.get()

      # event already exists
      # e.g. service fees
      if event_out.exists:
        # do not overwrite or update committed events
        if not event_out.to_dict()['committed']:
          event_out_ref.update(event_out_commit)
          logger.info(f'{fifo_account_name} - Comit - {event_out_id}')

        else:
          logger.warning(f'{fifo_account_name} - Comit - {event_out_id} - Already committed')
      # external event
      # e.g. exchange
      else:
        event_out_ref.set(event_out_commit)

        event_out_exchange_ref = fifo_exchanges_ref.document(event_out_id.replace('_', ''))
        event_out_exchange_ref.update({
          'fifo_to_allocate': 0,
          'committed': True
        })
        logger.info(f'{fifo_account_name} - Comit - {event_out_id}')
        
    
    for event_in_commit in staged_events_in_list:
      event_in_id = event_in_commit['event_id']
      event_in_ref = db.collection(u'fifo').document(fifo_account_name).collection(u'balance').document(event_in_id)

      event_in = event_in_ref.get()

      # do not overwrite or update committed events
      # unless it is part of a partial transaction
      if not event_in.to_dict()['committed'] or event_in.to_dict().get('partial', True):
        event_in_ref.update(event_in_commit)
        logger.info(f'{fifo_account_name} - Comit - {event_in_id}')
      else:
        logger.warning(f'{fifo_account_name} - Comit - {event_in_id} - Already committed')

