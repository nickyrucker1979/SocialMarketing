import requests
import urllib
import json
import sys
import time
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

log = setupDefaultLogging('HubSpot Contact Engagement Errors')


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
    return str(returnValue).replace('\n', '').replace('\r', '').replace('<p>', '')

def converttime(epoch_date):
    if epoch_date is "":
        return_date = epoch_date
    else:
        return_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(epoch_date)/1000))
    return return_date

exaconnect.execute('delete from UNIVERSITY_WAREHOUSE_DEV.HS_CONTACT_ENGAGEMENTS')

first_url = makeRequest('https://api.hubapi.com/engagements/v1/engagements/paged?hapikey=' + apikey)
json_data = json.loads(first_url.text)
has_more = json_data['hasMore']

while has_more:
    offset = json_data['offset']
    args = {'offset': offset}
    url_params = urllib.urlencode(args)
    url = ('https://api.hubapi.com/engagements/v1/engagements/paged?hapikey=' + apikey)
    new_call = url + '&' + url_params
    response = makeRequest(new_call)
    json_data = json.loads(response.text)
    try:
        has_more = (json_data['hasMore'])
    except Exception as e:
        if (type(json_data) is dict and 'hasMore' in json_data.keys()):
            print(json_data['hasMore'])
        else:
            print(json_data)
        raise
    for result in json_data['results']:
        exa_write = pd.DataFrame({
            'a': getValue(result, ["engagement", "id"]), #added
            'aa': getValue(result, ["engagement", "active"]),
            'b': converttime(getValue(result, ["engagement", "createdAt"])),
            'bb': converttime(getValue(result, ["engagement", "lastUpdated"])),
            'c': getValue(result, ["engagement", "type"]),
            'cc': getValue(result, ["engagement", "uid"]),
            'd': converttime(getValue(result, ["engagement", "timestamp"])),
            'dd': getValue(result, ["associations", "contactIds", 0]),
            'e': getValue(result, ["associations", "companyIds", 0]),
            'ee': getValue(result, ["associations", "dealIds", 0]),
            'f': getValue(result, ["associations", "ownerIds", 0]),
            'ff': getValue(result, ["associations", "workflowIds", 0]),
            'g': getValue(result, ["metadata", "from", "email"]),
            'gg': getValue(result, ["metadata", "from", "firstName"]),
            'h': getValue(result, ["metadata", "from", "lastName"]),
            'hh': getValue(result, ["metadata", "to", 0, "email"]),
            'i': getValue(result, ["metadata", "to", 0, "firstName"]),
            'ii': getValue(result, ["metadata", "to", 0, "lastName"]),
            'j': getValue(result, ["metadata", "body"]),
            'jj': getValue(result, ["metadata", "status"]),
            'k': getValue(result, ["metadata", "forObjectType"]),
            'kk': getValue(result, ["metadata", "subject"]),
            'l': converttime(getValue(result, ["metadata", "reminders", 0])),
            'll': getValue(result, ["metadata", "text"]),
            'm': getValue(result, ["metadata", "title"]),
            'mm': getValue(result, ["metadata", "source"]),
            'n': converttime(getValue(result, ["metadata", "startTime"])),
            'nn': converttime(getValue(result, ["metadata", "endTime"])),
            'o': getValue(result, ["metadata", "toNumber"]),
            'oo': getValue(result, ["metadata", "fromNumber"]),
            'p': getValue(result, ["engagement", "portalId"]),
            'pp': getValue(result, ["engagement", "createdBy"]),
            'q': getValue(result, ["engagement", "modifiedBy"]),
            'qq': getValue(result, ["engagement", "ownerId"]),
            'r': getValue(result, ["engagement", "teamId"]),
        },index=[0])
        exaconnect.writePandas(exa_write, 'UNIVERSITY_WAREHOUSE_DEV.HS_CONTACT_ENGAGEMENTS')