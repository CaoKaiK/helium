def run_balance(activities_ref, balance_height, last_balance):
  '''
  updates activities with a running balance. Starting from the last known balance.

  args:
    activities_ref: firebase reference to all activities
    balance_height: int with height of last commited balance
    last_balance: int representing the last commited balance

  returns:
    last_balance: int representing the balance in bones
    last_activity: dict of last activity
    last_fee: dict of last activity with a fee
    
  '''
  
  # get all activities from firebase that are later than latest balance run
  activities = activities_ref.where(u'height', u'>', balance_height).get()

  last_fee = {}

  if activities:
    # iterate activities and add current balance to activity
    for activity in activities:
      activity = activity.to_dict()
      height = activity.get('height')
      hash_act = activity.get('hash')
      amount = activity.get('amount', 0)
      fee_hnt = activity.get('fee_hnt', 0)

      # save activity if it has a positive fee
      if fee_hnt > 0:
        last_fee = activity
      
      # calculate balance
      last_balance += amount - fee_hnt

      # update activity with current balance
      activity_ref = activities_ref.document(f'{height}_{hash_act}')
      activity_ref.update({u'current_balance': last_balance})

  else:
    activity = {}
  
  last_activity = activity

  return last_balance, last_activity, last_fee


