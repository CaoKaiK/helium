

def run_balance(activities_ref, balance_height, last_balance):
  # get activities later than latest balance run
  activities = activities_ref.where(u'height', u'>', balance_height).get()

  if activities:
    # iterate activities and add current balance to activity
    for activity in activities:
      activity = activity.to_dict()
      height = activity.get('height')
      hash_act = activity.get('hash')
      amount = activity.get('amount', 0)
      fee_hnt = activity.get('fee_hnt', 0)

      last_balance += amount - fee_hnt

      # update activity
      activity_ref = activities_ref.document(f'{height}_{hash_act}')

      activity_ref.update({u'current_balance': last_balance})
  else:
    activity = {}

  return last_balance, activity

def run_fifo(activities_ref, fifo_height):
  # get activities with remaining fifo amount
  activities_to_allocate = activities_ref.where(u'fifo_allocated', u'==', 0).get()

  if activities_to_allocate:
    for activity_to_allocate in activities_to_allocate:
      activity_to_allocate = activity_to_allocate.to_dict()
      # get to allocate ref
      act_to_height = activities_to_allocate['height']
      act_to_hash = activities_to_allocate['hash']
      activity_to_allocate_ref = activities_ref.document(f'{act_to_height}_{act_to_hash}')
      

      fee_to_allocate = activity_to_allocate.get('fee_hnt')

      activities_for_allocation = activities_ref.where(u'fifo_to_allocate', u'>', 0).get()

      for activity_for_allocation in activities_for_allocation:
        activity_for_allocation = activity_for_allocation.to_dict()
        # get for allocate ref
        act_for_height = activities_for_allocation['height']
        act_for_hash = activities_for_allocation['hash']
        activity_to_allocate_ref = activities_ref.document(f'{act_for_height}_{act_for_hash}')








# def fifo_search(activities_ref, amount):
#   '''
#   search and collect all events needed to fulfill fifo amount of HNT required
#   '''

#   wallets_activities = activities_ref.where()
