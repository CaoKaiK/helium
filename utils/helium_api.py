import time
import requests

from utils.logger import get_logger

# log api calls
logger_api = get_logger('api')

BASE_URL = 'https://api.helium.io/v1'

headers = {
  'user-agent': 'python-requests/2.25.1'
}

def get_account(account_address):
  '''
  gets information on an account to retrieve the current balance and the current height
  
  args: 
  account_address: string of account address
  
  returns:
  account: dict of account, balance and height are inside
  '''

  url = f'{BASE_URL}/accounts/{account_address}'
  
  i = 0
  timeout = 0
  while i < 10:
    # relax
    time.sleep(max(2**i-1, timeout/1000))
    # request
    r = requests.get(url, headers=headers)
    logger_api.debug(f'Get {i+1}/10 - Account - {r.status_code}')

    data_json = r.json()
    account = data_json.get('data')

    if r.status_code == 200:
      break
    elif r.status_code == 429:
      timeout = r.json().get('come_back_in_ms', 0)
      logger_api.warning(f'Timeout - {timeout/1000}')
    i += 1

  if r.status_code != 200:
    print(f'Can not retrieve balance - {r.status_code}')
    account = []
  
  #check response
  if type(account.get('balance')) != int:
    raise ValueError('Balance is not of type int')
  elif not(account.get('balance') > 0):
    raise ValueError('Balance not greater 0')
  elif type(account.get('block')) != int:
    raise ValueError('Block is not of type int')
  elif not(account.get('block') > 0):
    raise ValueError('Block not greater 0')
  
  return account

def get_activities(address, logger, cursor='', get='hotspot'):
  '''
  previously: /activity -> changed to roles

  get activities that happend on either a hotspot or an account.
  '''

  if get=='hotspot':
    url = f'{BASE_URL}/hotspots/{address}/activity'
  else:
    url = f'{BASE_URL}/accounts/{address}/roles'

  if cursor:
    params = {'cursor': cursor}
  else:
    params = {}

  i = 0
  timeout = 0
  while i < 20:
    # relax
    time.sleep(max(2**i-1, timeout/1000))
    # request
    r = requests.get(url, params=params, headers=headers)
    logger_api.debug(f'Get {i+1}/10 - Activity - {r.status_code}')

    if r.status_code == 200:
      break
    elif r.status_code == 429:
      timeout = r.json().get('come_back_in_ms', 0)
      logger_api.warning(f'Timeout - {timeout/1000}')

    i += 1

  if r.status_code == 200:
    data_json = r.json()
    
    activities = data_json.get('data')

    if cur_cursor := data_json.get('cursor'):
      cursor = cur_cursor
    else:
      cursor = ''

  else:
    logger.warning(f'get_activities - Failed on Status Code {r.status_code}')
    activities = []
    cursor = ''

  return activities, cursor

def get_rewards(address, height):
  '''
  get rewards for a specific account at a specific block

  args:
  address: string of account
  height: int of height of the rewards

  returns:
  rewards: list of rewards at height

  '''
  url = f'{BASE_URL}/accounts/{address}/rewards/{height}'

  i = 0
  timeout = 0
  while i < 10:
    time.sleep(max(2**i-1, timeout/1000))
    r = requests.get(url)
    logger_api.debug(f'Get {i+1}/10 - Reward - {r.status_code}')
    
    if r.status_code == 200:
      rewards = r.json().get('data')
      break
    elif r.status_code == 429:
      timeout = r.json().get('come_back_in_ms', 0)
      logger_api.warning(f'Timeout - {timeout/1000}')
      rewards = []
    i += 1
  
  return rewards

def get_transaction(hash):
  '''
  '''
  url = f'{BASE_URL}/transactions/{hash}'

  i = 0
  timeout = 0
  while i < 10:
    time.sleep(max(2**i-1, timeout/1000))
    r = requests.get(url)
    logger_api.debug(f'Get {i+1}/10 - Transaction - {r.status_code}')

    if r.status_code == 200:
      transaction = r.json().get('data')
      break
    elif r.status_code == 429:
      timeout = r.json().get('come_back_in_ms', 0)
      logger_api.warning(f'Timeout - {timeout/1000}')
      transaction = []
    i += 1
  
  return transaction

def get_oracle_price(height, logger):
  '''
  get oracle price for block in USD

  args:
  height: int of requested height

  returns:
  price: float of price at requested height
  '''
  url = f'{BASE_URL}/oracle/prices/{height}'


  i = 0
  timeout = 0
  while i < 10:
    # relax
    time.sleep(max(2**i-1, timeout/1000))
    # request
    r = requests.get(url)
    logger_api.debug(f'Get {i+1}/10 - Price - {r.status_code}')

    if r.status_code == 200:
      break
    elif r.status_code == 429:
      timeout = r.json().get('come_back_in_ms', 0)
      logger_api.warning(f'Timeout - {timeout/1000}')
    i += 1

  if r.status_code == 200:
    price = r.json()['data']['price'] / 10e7
    oracle_block = r.json()['data']['block']

  return price

def get_height(time_in):
  '''
  get latest block at time
  
  args:
  time: in datetime format
  '''
  url = f'{BASE_URL}/blocks/height'

  params = {'max_time': time_in.isoformat()}

  i = 0
  timeout = 0
  while i < 10:
    # relax
    time.sleep(max(2**i-1, timeout/1000))
    # request
    r = requests.get(url, params=params, headers=headers)
    logger_api.debug(f'Get {i+1}/10 - Height - {r.status_code}')

    if r.status_code == 200:
      break
    elif r.status_code == 429:
      timeout = r.json().get('come_back_in_ms', 0)
      logger_api.warning(f'Timeout - {timeout/1000}')
    i += 1

  if r.status_code == 200:
    height = r.json()['data']['height']
  
  return height

