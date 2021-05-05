import os
import datetime
from dateutil.relativedelta import relativedelta
import json
import requests

import openpyxl
import pandas as pd

# API
BASE_URL = 'https://api.helium.io/v1'


### functions ###
def get_list_of_hotspots(account_id):
  '''
  gets the list of hotspots attributed to the account id

  args: 
  account_id: unique identifier of account

  returns: 
  hotspots: list of hotspots
  '''
  url = f'{BASE_URL}/accounts/{account_id}/hotspots'

  hotspots = []
  cursor = ''
  while not hotspots or cursor:

    if cursor:
      params = {'cursor': cursor}
    else:
      params = {}

    r = requests.get(url, params=params)

    data_json = r.json()
    if r.status_code == 200:
      hotspots.extend(data_json['data'])
      
      if cur_cursor := data_json.get('cursor'):
        cursor = cur_cursor

  return hotspots

def get_hotspot_rewards(hotspot_address, max_time, min_time):
  '''
  get all rewards for a hotspot in between min and max time

  args:
  hotspot_address: unique hotspot identifier
  max_time: maximum time in format yyyy-mm-dd
  min_time: minimum time in format yyyy-mm-dd
  '''
  url = f'{BASE_URL}/hotspots/{hotspot_address}/rewards'



  rewards = []
  cursor = ''
  while not rewards or cursor:

    if cursor:
      params = {'cursor': cursor, 'max_time': max_time, 'min_time': min_time}
    else:
      params = {'max_time': max_time, 'min_time': min_time}
    
    r = requests.get(url, params=params)

    data_json = r.json()
    if r.status_code == 200:
      rewards.extend(data_json['data'])

      if cur_cursor := data_json.get('cursor'):
        cursor = cur_cursor
        print(cursor)
      else:
        cursor = ''
  
  return rewards

def get_oracle_price(block):
  '''
  get oracle price for block in USD

  args:
  block: provides the oracle price at a specific block and at which block it initially took effect.
  '''
  url = f'{BASE_URL}/oracle/prices/{block}'

  r = requests.get(url)

  if r.status_code == 200:
    price = r.json()['data']['price'] / 10e7
    oracle_block = r.json()['data']['block']

  return price, oracle_block

def get_exchange_rate(time):
  '''
  get USD/EUR exchange rate

  args:
  time: time in 
  '''
  return None
### 

# load config file with list of account ids
with open('config.json', 'r') as file:
  config = json.load(file)

# get all hotspots for accounts
hotspots = []
for account_id in config['account_id']:
  hotspots.extend(get_list_of_hotspots(account_id))

config['hotspots'] = hotspots
with open('config.json', 'w',encoding='utf-8') as file:
  json.dump(config, file, indent=2, ensure_ascii=False)

# evaluate hotspots
month = datetime.date(2021, 4, 1)
min_time = month.strftime('%Y-%m-%d')
max_time = (month + relativedelta(months=1)).strftime('%Y-%m-%d')

dir_path = os.path.join('rewards',min_time)
xls_path = os.path.join(dir_path, 'report.xlsx')

report = []

for hotspot in hotspots:

  file_path = os.path.join(dir_path, hotspot['name']+'.json')

  if not os.path.exists(dir_path):
    os.mkdir(dir_path)
  
  rewards = get_hotspot_rewards(hotspot['address'], max_time=max_time, min_time=min_time)

  for idx in range(0,len(rewards)):
    price, oracle_block = get_oracle_price(rewards[idx]['block'])
    rewards[idx]['price'] = price
    rewards[idx]['oracle_block'] = oracle_block
    rewards[idx]['amount'] = rewards[idx]['amount'] / 10e7

  with open(file_path, 'w', encoding='utf-8') as file:
    json.dump(rewards, file, indent=2, ensure_ascii=False)

  report.extend(rewards)


## export to xls
report_df = pd.DataFrame(report, columns=[
  'timestamp',
  'hash',
  'gateway',
  'block',
  'amount',
  'account',
  'price',
  'oracle_block'
])

report_df['timestamp'] = pd.to_datetime(report_df['timestamp'])

report_df = report_df.sort_values(by=['timestamp'])

report_df['timestamp'] = report_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

report_df = report_df.drop(columns=['gateway', 'oracle_block'])

report_df.to_excel(xls_path, 'Sheet1', index=False)

