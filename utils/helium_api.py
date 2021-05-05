import os
import requests
import json


BASE_URL = 'https://api.helium.io/v1'


def get_hotspot_activities(hotspot_address, cursor=''):
  '''
  gets the list of activities for a hotspot. Cursor points to the set of paginated data.
  '''

  url = f'{BASE_URL}/hotspots/{hotspot_address}/activity'

  if cursor:
    params = {'cursor': cursor}
  else:
    params = {}
  
  r = requests.get(url, params=params)

  data_json = r.json()

  if r.status_code == 200:
    activities = data_json.get('data')

    if cur_cursor := data_json.get('cursor'):
      cursor = cur_cursor
    else:
      cursor = ''

  else:
    print(r.status_code)
    activities = []

  return activities, cursor
  
