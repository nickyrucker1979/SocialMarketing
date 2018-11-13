import requests
import config
import json
import time
import sys
import pandas as pd
import exasol as e
import datetime
reload(sys);
sys.setdefaultencoding("utf8")


apikey = config.apikey

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
    if epoch_date is "":
        return_date = epoch_date
    else:
        return_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(epoch_date)/1000))
    return return_date


r = exaconnect.readData("select C.VID from UNIVERSITY_WAREHOUSE_DEV.HS_CONTACTS C left join UNIVERSITY_WAREHOUSE_DEV.HS_CONTACT_FORMS CF on C.VID = CF.VID where (C.updated_at < CF.updatedat) or CF.VID IS NULL")
exadf = pd.DataFrame(r)

deletelist = []
deletelist.extend(exadf)
deletelist = tuple(deletelist)

if deletelist:
    exaconnect.execute('delete from UNIVERSITY_WAREHOUSE_DEV.HS_CONTACT_FORMS WHERE VID IN  (' + ','.join(map(str, deletelist)) + ')')



for index, row in exadf.iterrows():
    contact_call = 'https://api.hubapi.com/contacts/v1/contact/vid/' + str(row['VID']) + '/profile?hapikey=' + apikey
    #print contact_call
    detail_response = requests.get(contact_call)
    try:
          json_data_detail = json.loads(detail_response.text)
    except Exception as ec:
          print detail_response.text
    for form in getValue(json_data_detail, ['form-submissions']):
          #print form
          rdf = pd.DataFrame({'aid': getValue(json_data_detail, ['vid']), 'bform': getValue(form, ["title"]), 'cFORM_DATE' : converttime(getValue(form, ["timestamp"])), 'dUPDATEDAT': datetime.datetime.today()}, index=[0])
          exaconnect.writePandas(rdf,'UNIVERSITY_WAREHOUSE_DEV.HS_CONTACT_FORMS')