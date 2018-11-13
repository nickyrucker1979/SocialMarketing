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
import config_Hubspot as hsc
import config_Exasol as ec
reload(sys);
sys.setdefaultencoding("utf8")

apikey = hsc.apikey
hubspot_url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/all?hapikey=' + apikey

exaconnect = e.connect(
            dsn=ec.dsn,
            DRIVER=ec.DRIVER,
            EXAHOST=ec.EXAHOST,
            EXAUID=ec.EXAUID,
            EXAPWD=ec.EXAPWD,
            autocommit=True
            )

def get_hubspot_dataframe(passed_url):
    get_active_url = requests.get(passed_url)
    json_details = json.loads(get_active_url.text)#['elements']
    df = pd.io.json.json_normalize(json_details)
    return df

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
