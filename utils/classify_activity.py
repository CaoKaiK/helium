from datetime import datetime

def classify_activity(activity):
  act_type = activity.get('type')

  if act_type == 'rewards_v2' or act_type == 'rewards_v1':
    activity_dict = {
      'type': act_type,
      'hash': activity.get('hash'),
      'time': datetime.fromtimestamp(activity.get('time')),
      'height': activity.get('height'),
      'rewards': activity.get('rewards')
    }
  elif act_type == 'poc_request_v1':
    activity_dict = {
      'type': act_type,
      'hash': activity.get('hash'),
      'time': datetime.fromtimestamp(activity.get('time')),
      'height': activity.get('height')
    }
  elif act_type == 'poc_receipts_v1':
    activity_dict = {
      'type': act_type,
      'hash': activity.get('hash'),
      'time': datetime.fromtimestamp(activity.get('time')),
      'height': activity.get('height')
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
  elif act_type == 'assert_location_v2' or act_type == 'assert_location_v2':
    activity_dict = {
      'type': act_type,
      'hash': activity.get('hash'),
      'time': datetime.fromtimestamp(activity.get('time')),
      'height': activity.get('height'),
    }
  elif act_type == 'add_gateway_v1':
    activity_dict = {
      'type': act_type,
      'hash': activity.get('hash'),
      'time': datetime.fromtimestamp(activity.get('time')),
      'height': activity.get('height'),
    }
  elif act_type == 'consensus_group_v1':
    activity_dict = {
      'type': act_type,
      'hash': activity.get('hash'),
      'time': datetime.fromtimestamp(activity.get('time')),
      'height': activity.get('height'),
    }
  else:
    print(act_type)
    activity_dict = {}

  return activity_dict



