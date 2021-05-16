import os

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

abspath = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(abspath, 'key', 'helium-data-firebase-adminsdk-7ehtd-b2a18d49c8.json')

def init():
  '''
  initializes database with credential found in folder key.

  projectId connects to helium-data

  returns:
    db: database object for further usage
  '''
  
  cred = credentials.Certificate(file_path)

  firebase_admin.initialize_app(cred, {'projectId': 'helium-data'})

  db = firestore.client()

  return db


