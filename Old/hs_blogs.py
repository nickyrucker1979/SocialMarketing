import requests
import exasol as e
import urllib
import urllib3
import json
import time
import sys
import logging
import pandas as pd
import certifi
import config
reload(sys);
sys.setdefaultencoding("utf8")

http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())


apikey = config.apikey

DEFAULT_LOGGING_LEVEL = logging.DEBUG #or logging.INFO when not debugging

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

log = setupDefaultLogging('HubSpot Blogs Errors')

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

exaconnect.execute('delete from UNIVERSITY_WAREHOUSE_DEV.HS_BLOGS')

url = makeRequest('https://api.hubapi.com/content/api/v2/blogs?hapikey=' + apikey)


json_data = json.loads(url.text)
for object in json_data['objects']:
    #outputFile = open('C:\Users\ktreadwell\PycharmProjects\BIRP\Data\hsblogs.txt', 'a')
    #sys.stdout = outputFile
    exa_write = pd.DataFrame({"a": getValue(object, ["absolute_url"]),
            "b" : getValue(object, ["analytics_page_id"]),
            "c": getValue(object, ["category_id"]),
            "d": converttime(getValue(object, ["created_date_time"])),
            "e": getValue(object, ["html_title"]),
            "f": getValue(object, ["id"]),
            "g": getValue(object, ["portal_id"])
        }, index=[0])
    exaconnect.writePandas(exa_write, 'UNIVERSITY_WAREHOUSE_DEV.HS_BLOGS')