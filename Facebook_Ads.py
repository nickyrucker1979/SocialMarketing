import requests
import json
import config_Facebook as fbc
import pandas as pd
import exasol as e
import config_Exasol as ec

exaconnect = e.connect(
            dsn=ec.dsn,
            DRIVER=ec.DRIVER,
            EXAHOST=ec.EXAHOST,
            EXAUID=ec.EXAUID,
            EXAPWD=ec.EXAPWD,
            autocommit=True
            )

# FACEBOOK_ADS updated schema on 072518
exasol_import_db = 'CU_ONLINE_MARKETING_STG.FACEBOOK_ADS_0818'

if __name__ == '__main__':

    exaconnect.execute('truncate table ' + exasol_import_db)

    session = requests.Session()

    # Parameters See: https://developers.facebook.com/docs/marketing-api/insights/parameters/v2.10
    session.params.update({'level': 'ad'})

    # session.params.update({'date_preset': 'yesterday'})
    # session.params.update({'date_preset': 'last_90d'})

    session.params.update({'date_preset': 'last_30d'})
    session.params.update({'time_increment': 1})

    session.params.update({'limit' : '10000'})
    session.params.update({'fields':
                               'account_id' +
                               ',ad_id' +
                               ',adset_id' +
                               ',adset_name' +
                               ',campaign_id' +
                               ',campaign_name' +
                               ',clicks' +
                               ',cpc' +
                               ',cpm' +
                               ',cpp' +
                               ',ctr' +
                               ',date_start' +
                               ',date_stop' +
                               ',frequency' +
                               ',impressions' +
                               ',inline_link_click_ctr' +
                               ',inline_link_clicks' +
                               ',objective' +
                               ',reach' +
                               # ',social_clicks' +  # deprecated 0718
                               # ',social_impressions' +  # deprecated 0718
                               # ',social_reach' +  # deprecated 0718
                               # ',social_spend' +  # deprecated 0718
                               ',spend' +
                               # ',total_actions' +  # deprecated 0718
                              ',total_action_value' # +  # deprecated 0718
                               #',unique_clicks' +  # deprecated 0718
                               #',unique_ctr' +  # deprecated 0718
                               #',unique_social_clicks'  # deprecated 0718
                           })

    response = session.get(fbc.facebook_api_url, params={'access_token': fbc.my_access_token, 'appsecret_proof': fbc.my_appsecret_proof})

    # Validate response
    print(response)
    print(response.text)

    response_json = json.loads(response.text)["data"]

    # df = pd.DataFrame(response_json, columns=['campaign_name', 'cpp', 'unique_clicks'])
    df = pd.DataFrame(response_json)
    print(df)

    exaconnect.writePandas(df, exasol_import_db)
