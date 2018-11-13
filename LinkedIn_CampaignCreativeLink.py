import requests
import json
import pandas as pd
import config_LinkedIn as lc
import datetime
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

apikey = lc.apitoken
cu_online_id = lc.cu_online_id

exasol_campaign_table =  'CU_ONLINE_MARKETING_STG.LINKEDIN_CAMPAIGNS_UPDATED'
exasol_import_table =  'CU_ONLINE_MARKETING_STG.LINKEDIN_CAMPAIGN_CREATIVE_LINK'

def get_linkedin_dataframe(passed_url):
    get_active_url = requests.get(passed_url)
    json_details = json.loads(get_active_url.text)['elements']
    df = pd.io.json.json_normalize(json_details)
    return df


def linkedin_converttime(dataframe, field):
    dataframe[field] = pd.to_numeric(dataframe[field])
    dataframe[field] = pd.to_datetime(dataframe[field], unit='ms')


if __name__ == '__main__':

    exaconnect.execute('truncate table ' + exasol_import_table)

    exacontact = exaconnect.readData('select distinct id from ' + exasol_campaign_table)

    campaigns = pd.DataFrame(exacontact)
    print(campaigns)
    campaign_ids = campaigns['ID'].values.tolist()

    active_campaign_ids = []
    print(campaign_ids)
    for campaign in campaign_ids:
        active_campaign_ids.append('urn:li:sponsoredCampaign:' + str(campaign))

    for campaign in active_campaign_ids:
        creatives_df = get_linkedin_dataframe(
            'https://api.linkedin.com/v2/adCreativesV2?q=search&search.campaign.values[0]=' + campaign + '&oauth2_access_token=' + apikey)

        linkedin_converttime(creatives_df, 'changeAuditStamps.created.time')
        linkedin_converttime(creatives_df, 'changeAuditStamps.lastModified.time')
        creatives_df['campaign_id'] = creatives_df['campaign'].str[-9:]

        creative_ids = creatives_df[[
            'campaign_id',
            'id',
            'status',
            'changeAuditStamps.created.time',
            'changeAuditStamps.lastModified.time'
        ]]

        print(creative_ids)
        print('')

        exaconnect.writePandas(creative_ids, exasol_import_table)
