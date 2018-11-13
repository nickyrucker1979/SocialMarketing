
import time
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import sys
import pandas as pd
import exasol as e
import logging
import config
reload(sys);
sys.setdefaultencoding("utf8")


#would like to setup email notifications for logging and more in-depth logging

DEFAULT_LOGGING_LEVEL = logging.INFO #or logging.INFO when not debugging

def setupDefaultLogging(name):
    """ returns a logger setup with default settings """

    # create logger
    log = logging.getLogger(name)

    # set the logging level
    log.setLevel(DEFAULT_LOGGING_LEVEL)

    # setup console logging
    stdoutHandler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stdoutHandler.setFormatter(formatter)
    log.addHandler(stdoutHandler)

    # setup file logging
    fileHandler = logging.FileHandler('%s.log' % (name))
    fileHandler.setFormatter(formatter)
    log.addHandler(fileHandler)

    return log

log = setupDefaultLogging('Google Analytics Errors')


exaconnect = e.connect(
            dsn=config.dsn,
            DRIVER=config.DRIVER,
            EXAHOST=config.EXAHOST,
            EXAUID=config.EXAUID,
            EXAPWD=config.EXAPWD,
            autocommit=True
            )


def converttime(epoch_date):
    if epoch_date is "":
        return_date = epoch_date
    else:
        return_date = time.strftime("%Y-%m-%d")
    return return_date

def getValue(myObject, path):
    returnValue = myObject
    for key in path:
        if(returnValue):
            if(type(returnValue) is dict and key in returnValue.keys() and returnValue[key]):
                returnValue = returnValue[key]
            elif(type(returnValue) is list and key <= len(returnValue)):
                returnValue = returnValue[key]
            elif(type(returnValue) is bool and key <= len(returnValue)):
                returnValue = returnValue[key]
            else:
                returnValue = ""
                break
    return returnValue

def removePipe(pipeField):
    if pipeField is "":
        return_text = pipeField
    else:
        return_text = pipeField.replace('|', '')
    return return_text

def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):

  credentials = ServiceAccountCredentials.from_p12_keyfile(
    service_account_email, key_file_location, scopes=scope)

  http = credentials.authorize(httplib2.Http())

  # Build the service object.
  service = build(api_name, api_version, http=http)

  return service


exaconnect.execute('delete from UNIVERSITY_WAREHOUSE_DEV.GA_TRAFFIC_SOURCES')

scope = ['https://www.googleapis.com/auth/analytics.readonly']
service_account_email = config.service_account_email
key_file_location =  'C:\Users\ktreadwell\PycharmProjects\BIRP\Production\My Project-eb2ecd67c2ee.p12'   #'E:\Marketing_Production\My Project-eb2ecd67c2ee.p12'
service = get_service('analytics', 'v3', scope, key_file_location, service_account_email)

results = service.data().ga().get(
    ids='ga:86629329',
    start_date='2017-01-01',
    end_date='today',
    dimensions='ga:fullReferrer, ga:campaign, ga:adwordsCampaignID, ga:keyword, ga:socialNetwork, ga:medium,  ga:source',  ## , ga:source, ga:medium,  ',
    metrics='ga:organicSearches',
    samplingLevel='HIGHER_PRECISION',
    start_index=1).execute()
start_index = getValue(results, ['query', 'start-index'])
totalResults = getValue(results, ['totalResults'])


while start_index < totalResults:
    results = service.data().ga().get(
          ids='ga:86629329',
          start_date='2017-01-01',
          end_date='today',
          dimensions='ga:fullReferrer, ga:campaign, ga:adwordsCampaignID, ga:keyword, ga:socialNetwork, ga:medium,  ga:source',
          metrics='ga:organicSearches',
          samplingLevel='HIGHER_PRECISION',
          start_index=start_index).execute()
    start_index = getValue(results, ['query', 'start-index'])
    items = getValue(results, ['itemsPerPage'])
    totalResults = getValue(results, ['totalResults'])
    start_index = start_index + items
    for row in getValue(results, ['rows']):
      exa_write = pd.DataFrame({
          'a': (getValue(row, [0])).replace('(not set)', ''),
          'b': (getValue(row, [1])).replace('(not set)', ''),
          'c': (getValue(row, [2])).replace('(not set)', ''),
          'd': (getValue(row, [3])).replace('(not set)', ''),
          'e': (getValue(row, [4])).replace('(not set)', ''),
          'f': (getValue(row, [5])).replace('(none)', ''),
          'g': (getValue(row, [6])).replace('(direct)', 'direct'),
          'h': (getValue(row, [7])).replace('(not set)', '')
      },index=[0])
      exaconnect.writePandas(exa_write, 'UNIVERSITY_WAREHOUSE_DEV.GA_TRAFFIC_SOURCES')
