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

exasol_import_db = 'CU_ONLINE_MARKETING_STG.HUBSPOT_SOCIAL_POSTS'

def get_socialposts_dataframe(passed_url):
    get_active_url = requests.get(passed_url)
    json_details = json.loads(get_active_url.text)
    df = pd.io.json.json_normalize(json_details)
    return df

def hubspot_converttime(dataframe, field):
    dataframe[field] = pd.to_numeric(dataframe[field])
    dataframe[field] = pd.to_datetime(dataframe[field], unit='ms').dt.date

def split_and_keep_first(dataframe, field, split_character):
    x = dataframe[field]
    x = x.str.split(split_character)
    x = x.apply(pd.Series)[0]
    dataframe[field] = x

if __name__ == '__main__':

    exaconnect.execute('truncate table ' + exasol_import_db)

    url = 'https://api.hubapi.com/broadcast/v1/broadcasts?count=1000&hapikey=' + apikey
    # print(url)

    hs_df = get_socialposts_dataframe(url)
    # print(hs_df)
    print(list(hs_df))

    hs_socialposts_df = hs_df[[
        'broadcastGuid',
        'campaignGuid',
        'channelKey',
        'createdAt',
        'content.title',
        'messageUrl',
        'clicks',
        'likes',
        'retweets',
        'replies',
        'interactionsCount'
    ]]

    hubspot_converttime(hs_socialposts_df, 'createdAt')
    split_and_keep_first(hs_socialposts_df, 'channelKey', ':')

    print(hs_socialposts_df)

    exaconnect.writePandas(hs_socialposts_df, exasol_import_db)
