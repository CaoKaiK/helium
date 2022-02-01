import os
import logging
from datetime import date

today = date.today().strftime('%y-%m-%d')
abspath = os.path.dirname(os.path.abspath(__file__))

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_logger(file, formatter=formatter):
  log_dir = os.path.join(abspath, '..', 'logs', file)
  
  if not os.path.exists(log_dir):
    os.mkdir(log_dir)

  log_path = os.path.join(log_dir, f'{today}.log')

  # create logger 
  logger = logging.getLogger(file)
  logger.setLevel(logging.DEBUG)
  # create file handler for level debug or higher
  fh = logging.FileHandler(log_path)
  fh.setLevel(logging.DEBUG)
  # create console handler for level error or higher
  ch = logging.StreamHandler()
  ch.setLevel(logging.ERROR)
  # create formatter and add it to the handlers
  
  fh.setFormatter(formatter)
  ch.setFormatter(formatter)
  # add the handlers to the logger
  logger.addHandler(fh)
  logger.addHandler(ch)

  return logger