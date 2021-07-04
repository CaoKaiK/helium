

def run_balance(activities_ref, balance_height, last_balance):
  # get activities later than latest balance run
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

      if fee_hnt > 0:
        last_fee = activity
      

      last_balance += amount - fee_hnt

      # update activity
      activity_ref = activities_ref.document(f'{height}_{hash_act}')

      activity_ref.update({u'current_balance': last_balance})

  else:
    activity = {}

  return last_balance, activity, last_fee


