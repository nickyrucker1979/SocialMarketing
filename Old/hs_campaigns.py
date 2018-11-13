import requests
import urllib
import json
import time
import sys
import pandas as pd
import exasol as e
import logging
import certifi
import urllib3
import config
reload(sys);
sys.setdefaultencoding("utf8")


http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())


apikey = config.apikey


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

log = setupDefaultLogging('HubSpot Campaigns Errors')


exaconnect = e.connect(
            dsn=config.dsn,
            DRIVER=config.DRIVER,
            EXAHOST=config.EXAHOST,
            EXAUID=config.EXAUID,
            EXAPWD=config.EXAPWD,
            autocommit=True
            )


def makeRequest(url):
    maxAttempts = 10
    response = None
    for attempt in range(maxAttempts):
        try:
            response = requests.get(url);
            break
        except Exception as e:
            log.debug("Request failed with: {}".format(e))
            if(attempt + 1 == maxAttempts):
                raise
    return response


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


def converttime(epoch_date):
    if epoch_date is "":
        return_date = epoch_date
    else:
        return_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(epoch_date)/1000))
    return return_date

exaconnect.execute('delete from UNIVERSITY_WAREHOUSE_DEV.HS_CAMPAIGNS')

first_url = makeRequest('https://api.hubapi.com/email/public/v1/campaigns/by-id?hapikey=' + apikey)
json_data = json.loads(first_url.text)
has_more = json_data['hasMore']

while has_more:
    offset = json_data['offset']
    args = {'offset': offset}
    url_params = urllib.urlencode(args)
    url = ('https://api.hubapi.com/email/public/v1/campaigns/by-id?hapikey=' + apikey)
    new_call = url + '&' + url_params
    response = makeRequest(new_call)
    try:
        json_data = json.loads(response.text)
    except Exception as e:
        print response.text
    has_more = False
    if (type(json_data) is dict and 'hasMore' in json_data.keys()):
        has_more = json_data['hasMore']
    for campaign in json_data['campaigns']:
        campaign_call = ('https://api.hubapi.com/email/public/v1/campaigns/' + str(campaign['id']) + '?appId=' + str(campaign['appId'])  + '&hapikey=' + apikey)
        detail_response = makeRequest(campaign_call)
        try:
            json_data_detail = json.loads(detail_response.text)
        except Exception as ec:
            print detail_response.text
        exa_write = pd.DataFrame({
                'a': json_data_detail['id'],
                'b': json_data_detail['appId'],
                'c': getValue(json_data_detail, ["subject"]),
                'd': getValue(json_data_detail, ["name"]),
                'e': getValue(json_data_detail, ["appName"]),
                'f': getValue(json_data_detail, ["type"]),
                'g': getValue(json_data_detail, ["counters", "processed"]),
                'h': getValue(json_data_detail, ["counters", "deferred"]),
                'i': getValue(json_data_detail, ["counters", "unsubscribed"]),
                'j': getValue(json_data_detail, ["counters", "statuschange"]),
                'k': getValue(json_data_detail, ["counters", "bounce"]),
                'l': getValue(json_data_detail, ["counters", "mta_dropped"]),
                'm': getValue(json_data_detail, ["counters", "dropped"]),
                'n': getValue(json_data_detail, ["counters", "sent"]),
                'o': getValue(json_data_detail, ["counters", "click"]),
                'p': getValue(json_data_detail, ["counters", "open"]),
                'q': getValue(json_data_detail, ["counters", "spamreport"]),
                'r': getValue(json_data_detail, ["processingState"]),
                's': converttime(getValue(json_data_detail, ["lastProcessingFinishedAt"]))
        }, index=[0])
        exaconnect.writePandas(exa_write, 'UNIVERSITY_WAREHOUSE_DEV.HS_CAMPAIGNS')