import requests
import urllib
import json
import pandas as pd
import config_Hubspot as hsc
import config_Exasol as ec
import exasol as e
import sys

reload(sys);
sys.setdefaultencoding("utf8")

apikey = hsc.apikey
hubspot_url = 'https://api.hubapi.com/contacts/v1/lists/recently_updated/contacts/recent?count=100&hapikey=' + apikey

exaconnect = e.connect(
            dsn=ec.dsn,
            DRIVER=ec.DRIVER,
            EXAHOST=ec.EXAHOST,
            EXAUID=ec.EXAUID,
            EXAPWD=ec.EXAPWD,
            autocommit=True
            )

exasol_import_db = 'CU_ONLINE_MARKETING_STG.HUBSPOT_CONTACTS'
exasol_lookup_db = 'CU_ONLINE_MARKETING.HUBSPOT_CONTACTS'

has_more = True
vid_offset = ''

def cast_unix_to_datetime(dataframe, field):
    dataframe[field] = pd.to_datetime(dataframe[field], unit='ms').dt.date

def get_hubspot_dataframe(passed_url):
    get_active_url = requests.get(passed_url)
    json_details = json.loads(get_active_url.text)['contacts']
    df = pd.io.json.json_normalize(json_details)
    return df

def get_hubspot_dataframe_offsets(passed_url):
    get_active_url = requests.get(passed_url)
    json_details = json.loads(get_active_url.text)
    df = pd.io.json.json_normalize(json_details)
    df['has-more'] = df['has-more'].iloc[0]
    df['vid-offset'] = df['vid-offset'].iloc[0]
    return df

if __name__ == '__main__':

    exaconnect.execute('truncate table ' + exasol_import_db)

    df = get_hubspot_dataframe(hubspot_url)
    print(hubspot_url)
    df_offset = get_hubspot_dataframe_offsets(hubspot_url)
    has_more = df_offset['has-more'].iloc[0]
    vid_offset = df_offset['vid-offset'].iloc[0]

    hubspot_contacts_df = df[[
        'vid',
        'properties.firstname.value',
        'properties.lastname.value',
        'properties.lastmodifieddate.value'
    ]]

    cast_unix_to_datetime(hubspot_contacts_df, 'properties.lastmodifieddate.value')
    print(hubspot_contacts_df)
    exaconnect.writePandas(hubspot_contacts_df, exasol_import_db)

    while has_more:
        args = {'vidOffset': vid_offset}
        url_params = urllib.urlencode(args)
        next_hubspot_call = hubspot_url + '&' + url_params
        print(next_hubspot_call)
        df = get_hubspot_dataframe(next_hubspot_call)
        df_offset = get_hubspot_dataframe_offsets(next_hubspot_call)
        has_more = df_offset['has-more'].iloc[0]
        vid_offset = df_offset['vid-offset'].iloc[0]

        more_hubspot_contacts_df = df[[
            'vid',
            'properties.firstname.value',
            'properties.lastname.value',
            'properties.lastmodifieddate.value'
        ]]

        cast_unix_to_datetime(more_hubspot_contacts_df, 'properties.lastmodifieddate.value')
        print(more_hubspot_contacts_df)
        exaconnect.writePandas(more_hubspot_contacts_df, exasol_import_db)

    # Delete from last 30 days is already imported (so you don't hit the api if already in system)
    truncate_already_imported_records = 'DELETE FROM ' + exasol_import_db + ' WHERE MODIFIED_DATE < (SELECT MAX(LAST_MODIFIED_DATE) FROM ' + exasol_lookup_db +')'
    exaconnect.execute(truncate_already_imported_records)
