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

exasol_lookup_campaigns_db = 'CU_ONLINE_MARKETING_STG.LINKEDIN_CAMPAIGNS_UPDATED'
exasol_import_db = 'CU_ONLINE_MARKETING_STG.LINKEDIN_ADS'

apikey = lc.apitoken
cu_online_id = lc.cu_online_id


def get_linkedin_dataframe(passed_url):
    get_active_url = requests.get(passed_url)
    json_details = json.loads(get_active_url.text)['elements']
    df = pd.io.json.json_normalize(json_details)
    return df


def linkedin_converttime(dataframe, field):
    dataframe[field] = pd.to_numeric(dataframe[field])
    dataframe[field] = pd.to_datetime(dataframe[field], unit='ms')


if __name__ == '__main__':

    # campaign_ids = ['136487424', '137350024']
    exaconnect.execute('truncate table ' + exasol_import_db)

    exacontact = exaconnect.readData("select distinct id from " + exasol_lookup_campaigns_db)
    campaigns = pd.DataFrame(exacontact)
    print(campaigns)
    campaign_ids = campaigns['ID'].values.tolist()

    active_campaign_ids = []
    print(campaign_ids)
    for campaign in campaign_ids:
        active_campaign_ids.append('urn:li:sponsoredCampaign:' + str(campaign))

    print(active_campaign_ids)

    for campaign in active_campaign_ids:
        creatives_df = get_linkedin_dataframe(
            'https://api.linkedin.com/v2/adCreativesV2?q=search&search.campaign.values[0]=' + campaign + '&oauth2_access_token=' + apikey)

        linkedin_converttime(creatives_df, 'changeAuditStamps.created.time')
        linkedin_converttime(creatives_df, 'changeAuditStamps.lastModified.time')
        creatives_df = creatives_df.loc[(creatives_df['status'] == 'ACTIVE')].reset_index()

        creative_ids = creatives_df[[
            'id',
            'campaign',
            'status',
            'changeAuditStamps.created.time',
            'changeAuditStamps.lastModified.time'
        ]]

        print(creative_ids)
        print('')

        # TODO: Find Creatives update if id and modified is greater than saved data
        # exaconnect.writePandas(creative_ids, 'UNIVERSITY_WAREHOUSE_STAGING.LINKEDIN_CREATIVES')

        print('')
        # TODO: If not in List then end the campaign

        active_ads = creative_ids['id'].values.tolist()

        for creative_ad in active_ads:
            creative_id = str(creative_ad)
            ads_pivot = '&pivot=CREATIVE'
            ads_time_granularity = '&timeGranularity=DAILY'
            ads_creativeid = '&creatives=urn:li:sponsoredCreative:' + creative_id
            start_date = datetime.datetime.now() + datetime.timedelta(-150)
            ads_start_month = '&dateRange.start.month=' + str(start_date.month)
            ads_start_day = '&dateRange.start.day=' + str(start_date.day)
            ads_start_year = '&dateRange.start.year=' + str(start_date.year)
            ads_url = 'https://api.linkedin.com/v2/adAnalyticsV2?q=analytics&oauth2_access_token=' + apikey + ads_start_month + ads_start_day + ads_start_year + ads_time_granularity + ads_pivot + ads_creativeid
            print(ads_url)
        #
            ads_df = get_linkedin_dataframe(ads_url)
            if ads_df.empty:
                print('No Data')
                continue
            else:
                ads_df_w_creativeid = ads_df.loc[:, 'creative_id'] = creative_id
                #  Add creative id to the dataset - not returned get endpoint
                ads_df.reindex().fillna(0)
                active_ads = ads_df[[
                    'creative_id',
                    'clicks',
                    'comments',
                    'companyPageClicks',
                    'costInUsd',
                    'dateRange.start.day',
                    'dateRange.start.month',
                    'dateRange.start.year',
                    'impressions',
                    'follows',
                    'shares',
                    'viralClicks',
                    'viralFollows',
                    'viralImpressions',
                    'viralComments',
                    'viralShares'
                ]]

                active_ads['ad_date'] = active_ads['dateRange.start.year'].map(str) + '-' + active_ads['dateRange.start.month'].map(str) + '-' + active_ads['dateRange.start.day'].map(str)
                active_ads =  active_ads.drop(columns=['dateRange.start.day', 'dateRange.start.month', 'dateRange.start.year'])
                print(active_ads)
                exaconnect.writePandas(active_ads, exasol_import_db)
