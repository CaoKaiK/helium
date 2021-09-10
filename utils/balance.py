from copy import deepcopy

def outstanding_corrections(last_height, corrections_ref):
  # get corrections still to be applied
  corrections = corrections_ref.where(u'height', u'>', last_height).stream()
  corrections_outstanding = 0
  for correction in corrections:
    corrections_outstanding += correction.to_dict()['fee_hnt']
  
  return corrections_outstanding

def make_correction(last_outgoing_activity, correction_amount, corrections_ref):
  # copy last outgoing activity
  correction = deepcopy(last_outgoing_activity)
  # modify key values
  correction['amount'] = 0
  correction['fee_hnt'] = correction_amount
  correction['fee'] = 0
  correction['fee_usd'] = 0
  correction['type'] = 'correction'

  last_outgoing_height = str(last_outgoing_activity['height']).zfill(10)
  
  # add to corrections collection
  corrections_ref.document(last_outgoing_height).set(correction)

  return True


def run_balance(last_balance, last_height, activities_staged, corrections_ref):
  # get corrections that need to be applied to this collection of staged activities
  corrections = corrections_ref.where(u'height', u'>', last_height).stream()
  correction_list = []
  for correction in corrections:
    correction_list.append(correction.to_dict())

  # list of activities to be committed to db
  activities_for_commit = []

  # iterate in reversed order, so that ealier events come first
  for activity_staged in activities_staged[::-1]:

    # check if correction exist for this run
    if corrections:
      # check if correction exists for this activity
      for correction in correction_list:
        if correction['height'] == activity_staged['height']:
          correction_amount = correction['fee_hnt']
          # increase fee by correction amount and save correction amount
          activity_staged['fee_hnt'] += correction_amount
          activity_staged['correction_hnt'] = correction_amount

    # calculate the balance for this activity (after it has occurred)
    # initial last balance comes from last commit
    current_balance = last_balance + activity_staged['amount'] - activity_staged.get('fee_hnt', 0)
    
    activity_staged['balance'] = current_balance

    # update last balance
    last_balance = current_balance

    activities_for_commit.append(activity_staged)

  return activities_for_commit


  # # get activities later than latest balance run
  # activities = activities_ref.where(u'height', u'>', balance_height).get()

  # last_fee = {}

  # if activities:
  #   # iterate activities and add current balance to activity
  #   for activity in activities:
  #     activity = activity.to_dict()
  #     height = activity.get('height')
  #     hash_act = activity.get('hash')
  #     amount = activity.get('amount', 0)
  #     fee_hnt = activity.get('fee_hnt', 0)

  #     if fee_hnt > 0:
  #       last_fee = activity
      

  #     last_balance += amount - fee_hnt

  #     # update activity
  #     activity_ref = activities_ref.document(f'{height}_{hash_act}')

  #     activity_ref.update({u'current_balance': last_balance})

  # else:
  #   activity = {}

  # return last_balance, activity, last_fee


