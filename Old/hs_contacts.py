## This code is intended to update the Exasaol UNIVERSITY_WAREHOUSE_DEV.HS_CONTACTS table by removing contacts that have been updated since the last run of this code and adding contacts which have been added.
##Code written by Kate Treadwell kate.treadwell@interworks.com 571.383.0544
import requests
import urllib
import json
import time
import sys
import datetime
import pandas as pd
import exasol as e
import logging
from ratelimit import rate_limited
import config
reload(sys);
sys.setdefaultencoding("utf8")

apikey = config.apikey
one_second = 1

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

log = setupDefaultLogging('HubSpot Contacts')


exaconnect = e.connect(
            dsn=config.dsn,
            DRIVER=config.DRIVER,
            EXAHOST=config.EXAHOST,
            EXAUID=config.EXAUID,
            EXAPWD=config.EXAPWD,
            autocommit=True
            )



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
    if epoch_date == "":
        return_date = epoch_date
    else:
        return_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(abs(float(epoch_date)/1000)))
    return return_date

@rate_limited(1,one_second)
def makeRequest(url):
    maxAttempts = 10
    response = None
    for attempt in range(maxAttempts):
        try:
            response = requests.get(url);
            #print('Timestamp: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())), response
            break
        except Exception as e:
            log.debug("Request failed with: {}".format(e))
            if(attempt + 1 == maxAttempts):
                raise
    return response

exacontact = exaconnect.readData("select distinct vid as id, updated_at from UNIVERSITY_WAREHOUSE_DEV.HS_CONTACTS")
exadf = pd.DataFrame(exacontact)


first_url = makeRequest('https://api.hubapi.com/contacts/v1/lists/all/contacts/all?hapikey=' + apikey)
json_data = json.loads(first_url.text)
has_more = json_data['has-more']

ids = []
date = []

while has_more:
    vidOffset = json_data['vid-offset']
    args = {'vidOffset': vidOffset}
    url_params = urllib.urlencode(args)
    url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/all?hapikey=' + apikey + '&Count=100'
    new_call = url + '&' + url_params
    response = makeRequest(new_call)
    try:
        json_data = json.loads(response.text)
    except Exception as e:
        print response.text, json_data, "error"
    has_more = False
    if (type(json_data) is dict and 'has-more' in json_data.keys()):
        has_more = json_data['has-more']
        for contact in json_data['contacts']:
            ids.append(getValue(contact, ['vid']))
            date.append(converttime(getValue(contact, ["properties", "lastmodifieddate", "value"])))
            contact_data = {'ID': ids, 'date': date}

apidf = pd.DataFrame(contact_data)

fulldf = pd.merge(apidf, exadf, left_on='ID', right_on='ID', how='left')


modifiedids = fulldf[fulldf['date'] > fulldf['UPDATED_AT']]

missingids = fulldf[fulldf['UPDATED_AT'].isnull()]

removeids = modifiedids['ID'].values


missingnewcontacts = missingids['ID'].values.tolist()

modifiednewcontacts = modifiedids['ID'].values.tolist()
print missingnewcontacts
print modifiednewcontacts

deletelist = []
deletelist.extend(removeids)
deletelist = tuple(deletelist)

if deletelist:
    exaconnect.execute('delete from UNIVERSITY_WAREHOUSE_DEV.HS_CONTACTS WHERE VID IN  (' + ','.join(map(str, deletelist)) + ')')
    exaconnect.execute('delete from UNIVERSITY_WAREHOUSE_DEV.HS_CONTACT_ACADEMIC_PROGRAM WHERE VID IN  (' + ','.join(map(str, deletelist)) + ')')
if missingnewcontacts:
    for id in missingnewcontacts:
        print id
        contact_call = 'https://api.hubapi.com/contacts/v1/contact/vid/' + str(id) + '/profile?hapikey=' + apikey
        detail_response = makeRequest(contact_call)
        try:
            json_data_detail = json.loads(detail_response.text)
        except Exception as ec:
            print "error", detail_response.text, contact_call
        exa_write = pd.DataFrame({"A": getValue(json_data_detail, ['vid']),
            "b": converttime(getValue(json_data_detail, ["properties", "birthdate", "value"])),
            "bb": converttime(getValue(json_data_detail, ["properties", "createdate", "value"])),
            "c": getValue(json_data_detail, ["properties", "lifecyclestage", "versions", 0, "value"]),
            "cc": converttime(getValue(json_data_detail, ["properties", "lifecyclestage", "versions", 0, "timestamp"])),
            "ccc": getValue(json_data_detail, ["properties", "hs_analytics_first_referrer", "versions", 0, "value"]),
            "d": converttime(getValue(json_data_detail, ["properties", "hs_analytics_first_referrer", "versions", 0, "timestamp"])),
            "dd": getValue(json_data_detail, ["properties", "recent_conversion_event_name", "versions", 0, "value"]),
            "ddd": getValue(json_data_detail, ["properties", "recent_conversion_event_name", "versions", 0, "timestamp"]),
            "e": getValue(json_data_detail, ["properties", "hs_analytics_num_page_views", "value"]),
            "ee": getValue(json_data_detail, ["properties", "city", "value"]),
            "eee": getValue(json_data_detail, ["properties", "state", "value"]),
            "f": getValue(json_data_detail, ["properties", "zip", "value"]),
            "ff": converttime(getValue(json_data_detail, ["properties", "hs_sales_email_last_opened", "value"])),
            "fff": getValue(json_data_detail, ["properties", "hubspotscore", "value"]),
            "g": getValue(json_data_detail, ["properties", "lastname", "value"]),
            "gg": getValue(json_data_detail, ["properties", "twitterhandle", "value"]),
            "ggg": getValue(json_data_detail, ["properties", "linkedinconnections", "value"]),
            "h": getValue(json_data_detail, ["properties", "phone", "value"]),
            "hh": getValue(json_data_detail, ["properties", "specializations", "value"]),
            "hhh": getValue(json_data_detail, ["properties", "hs_soclial_num_broadcast_clicks", "value"]),
            "i": getValue(json_data_detail, ["properties", "email", "value"]),
            "ii": getValue(json_data_detail, ["properties", "cu_email_address", "value"]),
            "iii": getValue(json_data_detail, ["properties", "hs_lead_status", "value"]),
            "j": getValue(json_data_detail, ["properties", "hs_emails_sends_since_last_engagement", "value"]),
            "jj": getValue(json_data_detail, ["properties", "residency", "value"]),
            "jjj": getValue(json_data_detail, ["properties", "hs_email_optout", "value"]),
            "l": getValue(json_data_detail, ["properties", "cu_admit", "value"]),
            "ll": getValue(json_data_detail, ["properties", "firstname", "value"]),
            "lll": getValue(json_data_detail, ["properties", "linkedinbio", "value"]),
            "m": converttime(getValue(json_data_detail, ["properties", "closeddate", "value"])),
            "mm": converttime(getValue(json_data_detail, ["properties", "lastmodifieddate", "value"])),
            "mmm": getValue(json_data_detail, ["properties", "kloutscoregeneral", "value"]),
            "n": datetime.datetime.today()}, index=[0])
        exaconnect.writePandas(exa_write, 'UNIVERSITY_WAREHOUSE_DEV.HS_CONTACTS')
        majors = getValue(json_data_detail, ["properties", "academic_program", "value"]).split(';')
        print "MAJORS", majors
        for major in majors:
            print id
            table = zip([major], [id])
            print table
            exa_write = pd.DataFrame({"A": table[0][1],
                                      "B": table[0][0]}, index=[0])
            exaconnect.writePandas(exa_write, 'UNIVERSITY_WAREHOUSE_DEV.HS_CONTACT_ACADEMIC_PROGRAM')
if modifiednewcontacts:
    for id in modifiednewcontacts:
        print id
        contact_call = 'https://api.hubapi.com/contacts/v1/contact/vid/' + str(id) + '/profile?hapikey=' + apikey
        detail_response = makeRequest(contact_call)
        try:
            json_data_detail = json.loads(detail_response.text)
        except Exception as ec:
            print "error", detail_response.text, contact_call
        exa_write = pd.DataFrame({"A": getValue(json_data_detail, ['vid']),
            "b": converttime(getValue(json_data_detail, ["properties", "birthdate", "value"])),
            "bb": converttime(getValue(json_data_detail, ["properties", "createdate", "value"])),
            "c": getValue(json_data_detail, ["properties", "lifecyclestage", "versions", 0, "value"]),
            "cc": converttime(getValue(json_data_detail, ["properties", "lifecyclestage", "versions", 0, "timestamp"])),
            "ccc": getValue(json_data_detail, ["properties", "hs_analytics_first_referrer", "versions", 0, "value"]),
            "d": converttime(getValue(json_data_detail, ["properties", "hs_analytics_first_referrer", "versions", 0, "timestamp"])),
            "dd": getValue(json_data_detail, ["properties", "recent_conversion_event_name", "versions", 0, "value"]),
            "ddd": getValue(json_data_detail, ["properties", "recent_conversion_event_name", "versions", 0, "timestamp"]),
            "e": getValue(json_data_detail, ["properties", "hs_analytics_num_page_views", "value"]),
            "ee": getValue(json_data_detail, ["properties", "city", "value"]),
            "eee": getValue(json_data_detail, ["properties", "state", "value"]),
            "f": getValue(json_data_detail, ["properties", "zip", "value"]),
            "ff": converttime(getValue(json_data_detail, ["properties", "hs_sales_email_last_opened", "value"])),
            "fff": getValue(json_data_detail, ["properties", "hubspotscore", "value"]),
            "g": getValue(json_data_detail, ["properties", "lastname", "value"]),
            "gg": getValue(json_data_detail, ["properties", "twitterhandle", "value"]),
            "ggg": getValue(json_data_detail, ["properties", "linkedinconnections", "value"]),
            "h": getValue(json_data_detail, ["properties", "phone", "value"]),
            "hh": getValue(json_data_detail, ["properties", "specializations", "value"]),
            "hhh": getValue(json_data_detail, ["properties", "hs_soclial_num_broadcast_clicks", "value"]),
            "i": getValue(json_data_detail, ["properties", "email", "value"]),
            "ii": getValue(json_data_detail, ["properties", "cu_email_address", "value"]),
            "iii": getValue(json_data_detail, ["properties", "hs_lead_status", "value"]),
            "j": getValue(json_data_detail, ["properties", "hs_emails_sends_since_last_engagement", "value"]),
            "jj": getValue(json_data_detail, ["properties", "residency", "value"]),
            "jjj": getValue(json_data_detail, ["properties", "hs_email_optout", "value"]),
            "l": getValue(json_data_detail, ["properties", "cu_admit", "value"]),
            "ll": getValue(json_data_detail, ["properties", "firstname", "value"]),
            "lll": getValue(json_data_detail, ["properties", "linkedinbio", "value"]),
            "m": converttime(getValue(json_data_detail, ["properties", "closeddate", "value"])),
            "mm": converttime(getValue(json_data_detail, ["properties", "lastmodifieddate", "value"])),
            "mmm": getValue(json_data_detail, ["properties", "kloutscoregeneral", "value"]),
            "n": datetime.datetime.today()}, index=[0])
        exaconnect.writePandas(exa_write, 'UNIVERSITY_WAREHOUSE_DEV.HS_CONTACTS')
        majors = getValue(json_data_detail, ["properties", "academic_program", "value"]).split(';')
        print "MAJORS", majors
        for major in majors:
            print id
            table = zip([major], [id])
            print table
            exa_write = pd.DataFrame({"A": table[0][1],
                                      "B": table[0][0]}, index=[0])
            exaconnect.writePandas(exa_write, 'UNIVERSITY_WAREHOUSE_DEV.HS_CONTACT_ACADEMIC_PROGRAM')