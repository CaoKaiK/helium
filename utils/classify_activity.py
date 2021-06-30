from datetime import datetime

from utils.helium_api import get_oracle_price

def classify_activity(activity, logger):
  act_type = activity.get('type')
  date = datetime.fromtimestamp(activity.get('time'))
  # generic activity dict
  activity_dict = {
    'type': act_type,
    'hash': activity.get('hash'),
    'time': date,
    'year': date.year,
    'month': date.month,
    'day': date.day,
    'height': activity.get('height')
  }

  if act_type in ['rewards_v2', 'rewards_v1']:
    # add rewards to activity
    activity_dict['rewards'] = activity.get('rewards')

  elif act_type == 'state_channel_close_v1':
    # add packets and number of datacredits used to activity
    activity_dict['packets'] = activity.get('state_channel').get('summaries')[0].get('num_packets')
    activity_dict['dcs'] = activity.get('state_channel').get('summaries')[0].get('num_dcs')

  elif act_type in ['poc_request_v1', 'poc_receipts_v1', 'assert_location_v2', 'assert_location_v1', 'add_gateway_v1', 'consensus_group_v1']:
    # no additional information used yet
    None
  else:
    # no additional information used yet
    logger.error(f'Unknown activity in classify_activity: {act_type}')
    activity_dict = {}

  return activity_dict

def classify_wallet_activity(activity, wallet_address, logger):
  act_type = activity.get('type')
  date = datetime.fromtimestamp(activity.get('time'))

  # get oracle price on activity height
  height = activity.get('height')
  price = get_oracle_price(height)
  # generic activity dict
  activity_dict = {
    'hash': activity.get('hash'),
    'time': date,
    'year': date.year,
    'month': date.month,
    'day': date.day,
    'height': height,
    'price': price
  }

  ### mining ###
  if act_type in ['rewards_v2', 'rewards_v1']:
    amount = 0
    # iterate multiple rewards in one block
    for reward in activity.get('rewards'):
      amount += reward['amount']

    # add type and amount to activity
    activity_dict['type'] = 'mining'
    activity_dict['amount'] = amount
    activity_dict['fifo_to_allocate'] = amount

  ### transaction ###
  elif act_type in ['payment_v2', 'payment_v1']:
    amount = 0
    # iterate multiple payments in one block
    for payment in activity.get('payments'):
      amount += payment.get('amount')

    # reverse for outgoing transactions
    if activity.get('payer') == wallet_address:
      amount = amount * -1
      fee = activity.get('fee')

      # add payment fee
      fee_usd = fee / 1e5
      fee_hnt = int(round(fee_usd / price * 1e8))
      activity_dict['fee'] = fee
      activity_dict['fee_usd'] = fee_usd
      activity_dict['fee_hnt'] = fee_hnt

      activity_dict['fifo_allocated'] = 0

    # no fee in incoming transactions
    # fifo availables
    else:
      activity_dict['fifo_to_allocate'] = amount
      


    activity_dict['type'] = 'transaction'
    activity_dict['amount'] = amount


  elif act_type in ['token_burn_v1']:
    amount = activity.get('amount')

    fee = activity.get('fee')
    fee_usd = fee / 1e5
    fee_hnt = int(round(fee_usd / price * 1e8))

    activity_dict['type'] = 'burning'
    activity_dict['amount'] = amount
    activity_dict['fee'] = fee
    activity_dict['fee_usd'] = fee_usd
    activity_dict['fee_hnt'] = fee_hnt

    activity_dict['fifo_allocated'] = 0

  elif act_type in ['assert_location_v2', 'assert_location_v1']:
    # check if payed by wallet owner
    if activity.get('payer') == activity.get('owner'):

      fee = activity.get('staking_fee') + activity.get('fee')
      fee_usd = fee / 1e5
      fee_hnt = int(round(fee_usd / price * 1e8))

      activity_dict['type'] = 'assert_location'
      activity_dict['amount'] = 0
      activity_dict['fee'] = fee
      activity_dict['fee_usd'] = fee_usd
      activity_dict['fee_hnt'] = fee_hnt

      activity_dict['fifo_allocated'] = 0

    else:
      # payed by someone else
      activity_dict = {}

  elif act_type in ['add_gateway_v1']:
    # check if payed by wallet owner
    if activity.get('payer') == activity.get('owner'):

      fee = activity.get('staking_fee') + activity.get('fee')
      fee_usd = fee / 1e5
      fee_hnt = int(round(fee_usd / price * 1e8))

      activity_dict['type'] = 'add_gateway'
      activity_dict['amount'] = 0
      activity_dict['fee'] = fee
      activity_dict['fee_usd'] = fee_usd
      activity_dict['fee_hnt'] = fee_hnt

      activity_dict['fifo_allocated'] = 0

    else:
      # payed by someone else
      activity_dict = {}

  elif act_type in ['transfer_hotspot_v1']:
    # check if transfer from or to this wallet
    if activity.get('buyer') == wallet_address:
      fee = activity.get('fee')
      fee_usd = fee / 1e5
      fee_hnt = int(round(fee_usd / price * 1e8))

      activity_dict['type'] = 'transfer_hotspot'
      activity_dict['amount'] = 0
      activity_dict['fee'] = fee
      activity_dict['fee_usd'] = fee_usd
      activity_dict['fee_hnt'] = fee_hnt

      activity_dict['fifo_allocated'] = 0

    else:
      # payed by someone else
      activity_dict = {}

  else:
    logger.error(f'Unknown activity in classify_activity: {act_type}')
    activity_dict = {}

  return activity_dict



