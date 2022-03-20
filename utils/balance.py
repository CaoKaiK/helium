from copy import deepcopy

def outstanding_corrections(last_height, corrections_ref):
  '''
  calculates the amount of outstandings correction from the firebase reference path where all corrections are stored.

  args:
  last_height: int of last height where balance was committed
  corrections_ref: firebase reference to all corrections

  returns:
  corrections_outstanding: int of amount of bones outstanding to be committed
  '''
  # get corrections after last height
  corrections = corrections_ref.where(u'height', u'>', last_height).stream()
  corrections_outstanding = 0
  # iterate and sum up
  for correction in corrections:
    corrections_outstanding += correction.to_dict()['fee_hnt']
  
  return corrections_outstanding

def make_correction(last_outgoing_activity, correction_amount, corrections_ref):
  '''
  makes a deepcopy of the last outgoing activity (meaning it had a fee greater 0) and resets most keys to zero.
  Only features such as height, hash, time, etc. are kept so that the resulting activity can later be matched to the original activity

  args:
  last_outgoing_activity: dict of last activity with a fee greater 0
  correction_amount: int of amount of bones that the outgoing event needs to be corrected with
  corrections_ref: firebase reference to where all corrections are saved

  returns:
  True
  
  '''
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
  '''
  also see balance_fifo.run_balance
  updates the balance of activities that have been staged and applies corrections to the balance where necessary

  args:
    last_balance: int representing the last committed balance
    last_height: int with height of last committed balance
    activities_staged: list of dicts with staged activities
    corrections_ref: firebase reference to where all corrections are saved
  
  returns:
    activities_for_commit: list of activities with updated balance and applied correction to be committed

  '''
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
