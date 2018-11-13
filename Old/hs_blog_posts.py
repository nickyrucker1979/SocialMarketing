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

log = setupDefaultLogging('HubSpot Blog Post Errors')


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

exaconnect.execute('delete from UNIVERSITY_WAREHOUSE_DEV.HS_BLOG_POSTS')

first_url = makeRequest('https://api.hubapi.com/content/api/v2/blog-posts?hapikey=' + apikey)
json_data = json.loads(first_url.text)
count = json_data["total_count"] #210
offset = json_data['offset']
total = json_data['total_count']
while offset<total:
    offset = offset + 20
    args = {'offset': offset}
    url_params = urllib.urlencode(args)
    url = ('https://api.hubapi.com/content/api/v2/blog-posts?hapikey=' + apikey)
    new_call = url + '&' + url_params
    response =  makeRequest(new_call)
    try:
        json_data = json.loads(response.text)
    except Exception as e:
        print response.text
    for object in json_data['objects']:
        exa_write = pd.DataFrame({
                'a': getValue(object, ["analytics_page_id"]),
                'b': getValue(object, ["absolute_url"]),
                'c': getValue(object, ["analytics_page_type"]),
                'd': getValue(object, ["author"]),
                'e': converttime(getValue(object, ["author_at"])),
                'f': getValue(object, ["category"]),
                'g': getValue(object, ["category_id"]),
                'h': getValue(object, ["comment_count"]),
                'i': getValue(object, ["current_state"]),
                'j': getValue(object, ["currently_published"]),
                'k': getValue(object, ["html_title"]),
                'l': getValue(object, ["id"]),
                'm': getValue(object, ["label"]),
                'n': getValue(object, ["live_domain"]),
                'o': getValue(object, ["name"])
        }, index=[0])
        exaconnect.writePandas(exa_write, 'UNIVERSITY_WAREHOUSE_DEV.HS_BLOG_POSTS')