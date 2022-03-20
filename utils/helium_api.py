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
  gets information on an account. Mainly balance
  '''

  url = f'{BASE_URL}/accounts/{account_address}'
  
  i = 0
  timeout = 1000
  while i < 10:
    # relax
    #logger_api.debug(f'Start Sleep - Account')
    time.sleep(max(2 ** (i-1), timeout/1000+1))
    #logger_api.debug(f'End Sleep - Account')
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
  
  return account

def get_activities(address, logger, cursor='', get='hotspot'):
  '''
  gets the list of activities for a hotspot or wallet. Cursor points to the set of paginated data.
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
  timeout = 1000
  while i < 20:
    # relax
    #logger_api.debug(f'Start Sleep - Activity')
    time.sleep(max(2 ** (i-1), timeout/1000+10))
    #logger_api.debug(f'End Sleep - Activity')
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

  '''
  url = f'{BASE_URL}/accounts/{address}/rewards/{height}'

  i = 0
  timeout = 0
  while i < 10:
    time.sleep(max(2 ** (i-1),timeout/1000))
    r = requests.get(url)
    logger_api.debug(f'Get {i+1}/10 - Reward - {r.status_code}')
    
    if r.status_code == 200:
      data = r.json().get('data')
      break
    elif r.status_code == 429:
      timeout = r.json().get('come_back_in_ms', 0)
      logger_api.warning(f'Timeout - {timeout/1000}')
      data = []
    i += 1
  
  return data

def get_oracle_price(height, logger):
  '''
  get oracle price for block in USD

  args:
  block: provides the oracle price at a specific block and at which block it initially took effect.
  '''
  url = f'{BASE_URL}/oracle/prices/{height}'


  i = 0
  timeout = 0
  while i < 10:
    # relax
    #logger_api.debug(f'Start Sleep - Price')
    time.sleep(max(2 ** (i-1),timeout/1000))
    #logger_api.debug(f'End Sleep - Price')
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
    #logger_api.debug(f'Start Sleep - Height')
    time.sleep(max(2**(i-1),timeout/1000))
    #logger_api.debug(f'End Sleep - Height')
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

