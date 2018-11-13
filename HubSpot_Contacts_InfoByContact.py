import requests
import urllib
import json
import time
import datetime
import pandas as pd
import config_Hubspot as hsc
import config_Exasol as ec
import exasol as e
import sys

reload(sys);
sys.setdefaultencoding("utf8")

apikey = hsc.apikey

exaconnect = e.connect(
            dsn=ec.dsn,
            DRIVER=ec.DRIVER,
            EXAHOST=ec.EXAHOST,
            EXAUID=ec.EXAUID,
            EXAPWD=ec.EXAPWD,
            autocommit=True
            )

exasol_lookup_db = 'CU_ONLINE_MARKETING_STG.HUBSPOT_CONTACTS'
exasol_import_db = 'CU_ONLINE_MARKETING_STG.HUBSPOT_CONTACTS_INFO'
exasol_import_db_2 = 'CU_ONLINE_MARKETING_STG.HUBSPOT_CONTACTS_ACADEMIC_PROGRAMS'

def cast_unix_to_datetime(dataframe, field):
    dataframe[field] = pd.to_datetime(dataframe[field], unit='ms').dt.date

def get_hubspot_data(passed_url):
    get_active_url = requests.get(passed_url)
    json_details = json.loads(get_active_url.text)
    return json_details

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
        return_date = time.strftime("%Y-%m-%d", time.localtime(abs(float(epoch_date)/1000)))
    return return_date

if __name__ == '__main__':

    exaconnect.execute('truncate table ' + exasol_import_db)
    exaconnect.execute('truncate table ' + exasol_import_db_2)

    hubspot_ids = exaconnect.readData("select vid from " + exasol_lookup_db)
    print(hubspot_ids)

    # test vids -'37079638' has multiple academic programs --60931956 no academic programs -- 31988356, 37079606 - MSIS

    for index, row in hubspot_ids.iterrows():
        vid = str(row['VID'])
        url = 'https://api.hubapi.com/contacts/v1/contact/vid/'+ str(vid) + '/profile?hapikey=' + apikey
        print(url)
        json_data_detail = get_hubspot_data(url)

        hs_df = pd.DataFrame({
                "vid": getValue(json_data_detail, ['vid']),
                "birthdate": converttime(getValue(json_data_detail, ["properties", "birthdate", "value"])),
                "created_date": converttime(getValue(json_data_detail, ["properties", "createdate", "value"])),
                "lifecyclestage": getValue(json_data_detail, ["properties", "lifecyclestage", "versions", 0, "value"]),
                "lifecyclestage_date": converttime(getValue(json_data_detail, ["properties", "lifecyclestage", "versions", 0, "timestamp"])),
                "first_referrer": getValue(json_data_detail, ["properties", "hs_analytics_first_referrer", "versions", 0, "value"]),
                "first_referrer_date": converttime(getValue(json_data_detail, ["properties", "hs_analytics_first_referrer", "versions", 0, "timestamp"])),
                "conversion_name": getValue(json_data_detail, ["properties", "recent_conversion_event_name", "versions", 0, "value"]),
                "conversion_date": getValue(json_data_detail, ["properties", "recent_conversion_event_name", "versions", 0, "timestamp"]),
                "num_page_views": getValue(json_data_detail, ["properties", "hs_analytics_num_page_views", "value"]),
                "city": getValue(json_data_detail, ["properties", "city", "value"]),
                "state": getValue(json_data_detail, ["properties", "state", "value"]),
                "zip": getValue(json_data_detail, ["properties", "zip", "value"]),
                "sales_email_last_opened": converttime(getValue(json_data_detail, ["properties", "hs_sales_email_last_opened", "value"])),
                "hubspot_score": getValue(json_data_detail, ["properties", "hubspotscore", "value"]),
                "first_name": getValue(json_data_detail, ["properties", "firstname", "value"]),
                "last_name": getValue(json_data_detail, ["properties", "lastname", "value"]),
                "twitter": getValue(json_data_detail, ["properties", "twitterhandle", "value"]),
                "linkedin": getValue(json_data_detail, ["properties", "linkedinconnections", "value"]),
                "linkedinbio": getValue(json_data_detail, ["properties", "linkedinbio", "value"]),
                "phone": getValue(json_data_detail, ["properties", "phone", "value"]),
                "specializations": getValue(json_data_detail, ["properties", "specializations", "value"]),
                "email": getValue(json_data_detail, ["properties", "email", "value"]),
                "cu_email": getValue(json_data_detail, ["properties", "cu_email_address", "value"]),
                "lead_status": getValue(json_data_detail, ["properties", "hs_lead_status", "value"]),
                "emails_since_last_engagement": getValue(json_data_detail, ["properties", "hs_emails_sends_since_last_engagement", "value"]),
                "residency": getValue(json_data_detail, ["properties", "residency", "value"]),
                "cu_admit": getValue(json_data_detail, ["properties", "cu_admit", "value"]),
                "closeddate": converttime(getValue(json_data_detail, ["properties", "closeddate", "value"])),
                "lastmodifieddate": converttime(getValue(json_data_detail, ["properties", "lastmodifieddate", "value"])),
                "kloutscore": getValue(json_data_detail, ["properties", "kloutscoregeneral", "value"]),
                "academic_program": getValue(json_data_detail, ["properties", "academic_program", "value"]),
                "import_date": datetime.date.today()
        }, index=[0])

        hs_df_ordered = hs_df[[
            "vid",
            "first_name",
            "last_name",
            # "birthdate",
            "created_date",
            "lifecyclestage",
            "lifecyclestage_date",
            # "first_referrer",
            # "first_referrer_date",
            "conversion_name",
            "conversion_date",
            "num_page_views",
            "city",
            "state",
            "zip",
            # "sales_email_last_opened",
            "hubspot_score",
            "twitter",
            "linkedin",
            "linkedinbio",
            "phone",
            "specializations",
            "email",
            "cu_email",
            "lead_status",
            "emails_since_last_engagement",
            "residency",
            "cu_admit",
            "closeddate",
            "lastmodifieddate",
            "kloutscore",
            "import_date"
        ]]
        # print(hs_df)
        # print('')
        print(hs_df_ordered)

        try:
            exaconnect.writePandas(hs_df_ordered, exasol_import_db)
        except:
            pass

        print('')

        majors = getValue(json_data_detail, ["properties", "academic_program", "value"]).split(';')
        try:
            for major in majors:
                table = zip([major], hs_df['vid'])
                hs_academic_program_df = pd.DataFrame({"vid": table[0][0],
                                          "academic_program": table[0][1]}, index=[0])
                print(hs_academic_program_df)

                exaconnect.writePandas(hs_academic_program_df, exasol_import_db_2)
        except:
            pass
