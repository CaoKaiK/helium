import os
import logging
from datetime import date

today = date.today().strftime('%y-%m-%d')
abspath = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(abspath, '..', 'logs', f'{today}.log')

def get_logger(file):
  # create logger 
  logger = logging.getLogger(file)
  logger.setLevel(logging.DEBUG)
  # create file handler which logs even debug messages
  fh = logging.FileHandler(log_path)
  fh.setLevel(logging.DEBUG)
  # create console handler with a higher log level
  ch = logging.StreamHandler()
  ch.setLevel(logging.ERROR)
  # create formatter and add it to the handlers
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  fh.setFormatter(formatter)
  ch.setFormatter(formatter)
  # add the handlers to the logger
  logger.addHandler(fh)
  logger.addHandler(ch)

  return logger