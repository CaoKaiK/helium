import time
import requests

BASE_URL = 'https://api.helium.io/v1'

def get_activities(address, logger, cursor='', get='hotspot'):
  '''
  gets the list of activities for a hotspot or wallet. Cursor points to the set of paginated data.
  '''

  if get=='hotspot':
    url = f'{BASE_URL}/hotspots/{address}/activity'
  else:
    url = f'{BASE_URL}/accounts/{address}/activity'

  if cursor:
    params = {'cursor': cursor}
  else:
    params = {}
  
  r = requests.get(url, params=params)

  i = 0
  while r.status_code != 200 and i < 10:
    logger.warning(f'get_activities - Retry {i} on Status Code {r.status_code}')
    time.sleep(2 ** (i-1))
    r = requests.get(url, params=params)
    i += 1
    

  if r.status_code == 200:
    data_json = r.json()
    
    activities = data_json.get('data')

    if cur_cursor := data_json.get('cursor'):
      cursor = cur_cursor
    else:
      cursor = ''

  else:
    logger.warning(f'get_activities - Failes on Status Code {r.status_code}')
    activities = []
    cursor = ''

  return activities, cursor


def get_account(account_address):
  '''
  gets information on an account. Mainly balance
  '''

  url = f'{BASE_URL}/accounts/{account_address}'
  
  r = requests.get(url)

  data_json = r.json()

  if r.status_code == 200:

    account = data_json.get('data')

  else:
    print(r.status_code)
    account = []
  
  return account

def get_oracle_price(height, logger):
  '''
  get oracle price for block in USD

  args:
  block: provides the oracle price at a specific block and at which block it initially took effect.
  '''
  url = f'{BASE_URL}/oracle/prices/{height}'

  r = requests.get(url)

  i = 0
  while r.status_code != 200 and i < 10:
    logger.warning(f'get_activities - Retry {i} on Status Code {r.status_code}')
    time.sleep(2 ** (i-1))
    r = requests.get(url)
    i += 1

  if r.status_code == 200:
    price = r.json()['data']['price'] / 10e7
    oracle_block = r.json()['data']['block']

  return price

def get_height(time):
  '''
  get latest block at time
  
  args:
  time: in datetime format
  '''
  url = f'{BASE_URL}/blocks/height'

  params = {'max_time': time.isoformat()}

  r = requests.get(url, params=params)

  i = 0
  while r.status_code != 200 and i < 10:
    # logger.warning(f'get_activities - Retry {i} on Status Code {r.status_code}')
    time.sleep(2 ** (i-1))
    r = requests.get(url, params=params)
    i += 1

  if r.status_code == 200:
    height = r.json()['data']['height']
  
  return height

