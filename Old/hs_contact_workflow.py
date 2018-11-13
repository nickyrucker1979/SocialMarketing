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

log = setupDefaultLogging('HubSpot Contact Workflow Errors')


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

exaconnect.execute('delete from UNIVERSITY_WAREHOUSE_DEV.HS_CONTACT_WORKFLOW')

first_url = makeRequest('https://api.hubapi.com/contacts/v1/lists/all/contacts/all?hapikey=' + apikey)
json_data = json.loads(first_url.text)
has_more = json_data['has-more']

while has_more:
    vidOffset = json_data['vid-offset']
    args = {'vidOffset': vidOffset}
    url_params = urllib.urlencode(args)
    url = ('https://api.hubapi.com/contacts/v1/lists/all/contacts/all?hapikey=' + apikey + '&Count=100')
    new_call = url + '&' + url_params
    response = makeRequest(new_call)
    try:
        json_data = json.loads(response.text)
    except Exception as e:
        print response.text
    has_more = False
    if (type(json_data) is dict and 'has-more' in json_data.keys()):
        has_more = json_data['has-more']

    for contact in json_data['contacts']:
        contact_call = ('https://api.hubapi.com/automation/v2/workflows/enrollments/contacts/' + str(contact['vid']) + '?hapikey=' + apikey)
        detail_response = makeRequest(contact_call)
        try:
            json_data_detail = json.loads(detail_response.text)
        except Exception as ec:
            print detail_response.text
        for trigger in getValue(json_data_detail, [0, "triggerSet"]):
            exa_write = pd.DataFrame({
                'a': getValue(contact, ["vid"]),
                'b': getValue(trigger, ['trigger',0,'id']),
                'c': getValue(trigger, ['trigger', 0, 'type']),
                'd': getValue(trigger, ['trigger', 0, 'name']),
                'e': getValue(trigger, ['trigger', 0, 'enrollContactsOnActivation']),
                'f': getValue(json_data_detail, [0, "id"]),
                'g': getValue(json_data_detail, [0, "portalId"]),
                'h': converttime(getValue(json_data_detail, [0, "insertedAt"])),
                'i': converttime(getValue(json_data_detail, [0, "updatedAt"])),
                'j': getValue(json_data_detail, [0, "name"]),
                'k': getValue(json_data_detail, [0, "steps", "emailActions"]),
                'l': getValue(json_data_detail, [0, "steps", "id"]),
                'm': getValue(json_data_detail, [0, "steps", "branchCondition", "filtersListId"])
            },index=[0])
            exaconnect.writePandas(exa_write, 'UNIVERSITY_WAREHOUSE_DEV.HS_CONTACT_WORKFLOW')


            #2541
