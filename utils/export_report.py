import os
from datetime import date

import pandas as pd


def export(report_day, db):

  routers = db.collection(u'routers').stream()

  routers_list = []
  for router in routers:
    routers_list.append(router)
  
  # last month
  last_month = report_day.month - 1
  year = report_day.year
  if last_month < 1:
    last_month += 12
    year -= 1

  # path without extension
  abspath = os.path.dirname(os.path.abspath(__file__))
  export_path = os.path.join(abspath,'..' ,'export', 'routers', f'routers_{year}-{last_month}.xlsx')

  router_name_list = []
  rewards_list = []
  for router in routers_list:
    router_name = router.to_dict()['name']

    eval_ref = db.collection(u'routers').document(router_name).collection(u'eval')

    eval_list = eval_ref\
      .where(u'month', u'==', last_month) \
      .where(u'year', u'==', year).get()
    
    total = 0
    for eval in eval_list:
      eval_dict = eval.to_dict()
      total += eval_dict['rewards']['total']
    
    router_name_list.append(router_name)
    rewards_list.append(total)
    



  router_df = pd.DataFrame.from_dict({'router': router_name_list, 'rewards': rewards_list})
  router_df.to_excel(export_path, index=False)

