from datetime import datetime

from utils.helium_api import get_oracle_price

def classify_activity(activity):
  act_type = activity.get('type')

  if act_type in ['rewards_v2', 'rewards_v1']:
    date = datetime.fromtimestamp(activity.get('time'))
    activity_dict = {
      'type': act_type,
      'hash': activity.get('hash'),
      'time': date,
      'year': date.year,
      'month': date.month,
      'day': date.day,
      'height': activity.get('height'),
      'rewards': activity.get('rewards')
    }
  elif act_type == 'state_channel_close_v1':
    activity_dict = {
      'type': act_type,
      'hash': activity.get('hash'),
      'time': datetime.fromtimestamp(activity.get('time')),
      'height': activity.get('height'),
      'packets': activity.get('state_channel').get('summaries')[0].get('num_packets'),
      'dcs': activity.get('state_channel').get('summaries')[0].get('num_dcs')
    }
  elif act_type in ['poc_request_v1', 'poc_receipts_v1', 'assert_location_v2', 'assert_location_v1', 'add_gateway_v1', 'consensus_group_v1']:
    activity_dict = {
      'type': act_type,
      'hash': activity.get('hash'),
      'time': datetime.fromtimestamp(activity.get('time')),
      'height': activity.get('height')
    }
  else:
    print(f'!!! unknown transaction {act_type} !!!')
    activity_dict = {
      'type': act_type,
      'hash': activity.get('hash'),
      'time': datetime.fromtimestamp(activity.get('time')),
      'height': activity.get('height'),
    }

  return activity_dict

def classify_wallet_activity(activity, prev_balance):
  act_type = activity.get('type')
  date = datetime.fromtimestamp(activity.get('time'))
  height = activity.get('height')
  price = get_oracle_price(height)

  if act_type in ['rewards_v2', 'rewards_v1']:
    amount = 0
    # iterate multiple rewards in one block
    for reward in activity.get('rewards'):
      amount += reward['amount'] / 1e8
    
    activity_dict = {
      'type': 'mining',
      'amount': amount,
      'hash': activity.get('hash'),
      'time': date,
      'year': date.year,
      'month': date.month,
      'day': date.day,
      'height': height,
      'price': price,
      'prev_balance': round(prev_balance - amount, 8),
      'current_balance': prev_balance
    }
  elif act_type in ['payment_v2', 'payment_v1']:
    amount = 0
    # iterate multiple payments in one block
    for payment in activity.get('payments'):
      amount += payment.get('amount') / 1e8
    
    # add payment fee
    fee = activity.get('fee')
    fee_usd = fee / 1e5
    fee_hnt = fee_usd / price

    activity_dict = {
      'type': 'payment',
      'amount': amount,
      'fee': fee,
      'fee_usd': fee_usd,
      'fee_hnt': fee_hnt,
      'hash': activity.get('hash'),
      'time': date,
      'year': date.year,
      'month': date.month,
      'day': date.day,
      'height': height,
      'price': price,
      'prev_balance': round(prev_balance + amount + fee_hnt, 8),
      'current_balance': prev_balance
    }
  elif act_type in ['token_burn_v1']:
    amount = activity.get('amount') / 1e8

    fee = activity.get('fee')
    fee_usd = fee / 1e5
    fee_hnt = fee_usd / price

    activity_dict = {
      'type': 'burning',
      'amount': amount,
      'fee': fee,
      'fee_usd': fee_usd,
      'fee_hnt': fee_hnt,
      'hash': activity.get('hash'),
      'time': date,
      'year': date.year,
      'month': date.month,
      'day': date.day,
      'height': height,
      'price': price,
      'prev_balance': round(prev_balance + amount + fee_hnt, 8),
      'current_balance': prev_balance
    }
  elif act_type in ['assert_location_v2', 'assert_location_v1']:
    # check if payed by wallet owner
    if activity.get('payer') == activity.get('owner'):

      fee = activity.get('staking_fee') + activity.get('fee')
      fee_usd = fee / 1e5
      fee_hnt = fee_usd / price

      activity_dict = {
        'type': 'assert_location',
        'amount': 0,
        'fee': fee,
        'fee_usd': fee_usd,
        'fee_hnt': fee_hnt,
        'hash': activity.get('hash'),
        'time': date,
        'year': date.year,
        'month': date.month,
        'day': date.day,
        'height': height,
        'price': price,
        'prev_balance': round(prev_balance + fee_hnt, 8),
        'current_balance': prev_balance
      }
    else:
      # payed by someone else
      activity_dict = {}

  elif act_type in ['add_gateway_v1']:
    # check if payed by wallet owner
    if activity.get('payer') == activity.get('owner'):

      fee = activity.get('staking_fee') + activity.get('fee')
      fee_usd = fee / 1e5
      fee_hnt = fee_usd / price

      activity_dict = {
        'type': 'add_gateway',
        'amount': 0,
        'fee': fee,
        'fee_usd': fee_usd,
        'fee_hnt': fee_hnt,
        'hash': activity.get('hash'),
        'time': date,
        'year': date.year,
        'month': date.month,
        'day': date.day,
        'height': height,
        'price': price,
        'prev_balance': round(prev_balance + fee_hnt, 8),
        'current_balance': prev_balance
      }
    else:
      # payed by someone else
      activity_dict = {}
  else:
    print(activity)
  
  return activity_dict



