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

exasol_db = 'CU_ONLINE_MARKETING_STG.LINKEDIN_CAMPAIGNS_UPDATED'

def get_linkedin_dataframe(passed_url):
    get_active_url = requests.get(passed_url)
    print(get_active_url)
    json_details = json.loads(get_active_url.text)['elements']
    df = pd.io.json.json_normalize(json_details)
    return df


def linkedin_converttime(dataframe, field):
    dataframe[field] = pd.to_numeric(dataframe[field])
    dataframe[field] = pd.to_datetime(dataframe[field], unit='ms')


if __name__ == '__main__':

    exaconnect.execute('truncate table ' + exasol_db)

    ## Get Active Accounts
    account_df = get_linkedin_dataframe(
        'https://api.linkedin.com/v2/adAccountsV2?q=statuses&statuses=ACTIVE&oauth2_access_token=' + apikey)
    # Ignore personal and CU Denver University account (& (account_df['id'] != 500042925))

    # print(account_df)
    # print(list(account_df))

    account_df = account_df.loc[(account_df['type'] != 'PERSONAL')].reset_index()
    active_accounts = account_df[[
        'name',
        'id'
    ]]

    active_account_list = active_accounts['id'].values.tolist()
    active_account_ids = []

    for account in active_account_list:
        active_account_ids.append('urn:li:sponsoredAccount:' + str(account))

    ## Get Active Campaigns
    campaign_df = get_linkedin_dataframe(
        'https://api.linkedin.com/v2/adCampaignsV2?q=statuses&statuses=ACTIVE&oauth2_access_token=' + apikey)
    campaign_df = campaign_df.loc[
        (campaign_df['account'].isin(active_account_ids))].reset_index()

    linkedin_converttime(campaign_df, 'changeAuditStamps.created.time')
    linkedin_converttime(campaign_df, 'changeAuditStamps.lastModified.time')
    # linkedin_converttime(campaign_df, 'runSchedule.start')
    # linkedin_converttime(campaign_df, 'runSchedule.end')

    active_campaigns = campaign_df[[
        'account',
        'id',
        'name',
        'objectiveType',
        'changeAuditStamps.created.time',
        'changeAuditStamps.lastModified.time',
        'costType',
        'dailyBudget.amount',
        # 'totalBudget.amount',
        'type',
        'unitCost.amount',
        'creativeSelection',
        # 'runSchedule.start',
        # 'runSchedule.end',
    ]]

    print(active_campaigns)

    print('')

    exaconnect.writePandas(active_campaigns, exasol_db)
