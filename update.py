import os
from datetime import datetime as dt
import json
import requests

import openpyxl
import pandas as pd

# api params
BASE_URL = 'https://api.helium.io/v1'


# global params
ACCOUNT_ID = '142AccHcj1jqo31P2kVBB7sJbNZkvKjjroodcXngPjEoaEL2gog'


# get current account balance
url = f'{BASE_URL}/accounts/{ACCOUNT_ID}'
r = requests.get(url)

account_data = r.json()['data']

# get routers in account
url = f'{BASE_URL}/accounts/{ACCOUNT_ID}/hotspots'
r = requests.get(url)

account_data['hotspots'] = r.json()['data']

with open('account.json', 'w', encoding='utf8') as file:
    json.dump(account_data, file, indent=2, ensure_ascii=False)

for hotspot in account_data['hotspots']:
  name = hotspot['name']
  hotspot_address = hotspot['address']
  start_time = pd.to_datetime(hotspot['timestamp_added']).strftime('%Y-%m-%d')
  end_time = pd.to_datetime(dt.today()).strftime('%Y-%m-%d')

  params = {'min_time': start_time, 'max_time': end_time}

  url = f'{BASE_URL}/hotspots/{hotspot_address}/rewards/sum'
  r = requests.get(url, params=params)

  response = r.json()

  path = os.path.join('hotspots', f'{name}.json')
  with open(path, 'w', encoding='utf-8') as file:
    json.dump(response, file, indent=2, ensure_ascii=False)

  # update xlsx file
  xfile = openpyxl.load_workbook('Helium.xlsx')

  sheet = xfile[name]
  sheet['B2'] = pd.to_datetime(response['meta']['min_time']).strftime('%Y-%m-%d')
  sheet['B3'] = pd.to_datetime(response['meta']['max_time']).strftime('%Y-%m-%d')
  sheet['B5'] = response['data']['total']
  sheet['B7'] = response['data']['avg']
  xfile.save('Helium.xlsx')



