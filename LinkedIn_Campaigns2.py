import requests
import json
import pandas as pd
import LinkedIn_Config
import exasol as e
import config

exaconnect = e.connect(
            dsn=config.dsn,
            DRIVER=config.DRIVER,
            EXAHOST=config.EXAHOST,
            EXAUID=config.EXAUID,
            EXAPWD=config.EXAPWD,
            autocommit=True
            )

apikey = LinkedIn_Config.apitoken
cu_online_id = LinkedIn_Config.cu_online_id


def get_linkedin_dataframe(passed_url):
    get_active_url = requests.get(passed_url)
    json_details = json.loads(get_active_url.text)['elements']
    df = pd.io.json.json_normalize(json_details)
    return df


def converttime(dataframe, field):
    dataframe[field] = pd.to_numeric(dataframe[field])
    dataframe[field] = pd.to_datetime(dataframe[field], unit='ms')


if __name__ == '__main__':

    ## Get Active Accounts
    account_df = get_linkedin_dataframe(
        'https://api.linkedin.com/v2/adAccountsV2?q=statuses&statuses=ACTIVE&oauth2_access_token=' + apikey)
    # Ignore personal and CU Denver University account
    account_df = account_df.loc[(account_df['type'] != 'PERSONAL') & (account_df['id'] != 500042925)].reset_index()
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

    converttime(campaign_df, 'changeAuditStamps.created.time')
    converttime(campaign_df, 'changeAuditStamps.lastModified.time')
    converttime(campaign_df, 'runSchedule.start')
    converttime(campaign_df, 'runSchedule.end')

    active_campaigns = campaign_df[[
        'account',
        'id',
        'name',
        'objectiveType',
        'changeAuditStamps.created.time',
        'changeAuditStamps.lastModified.time',
        'costType',
        'dailyBudget.amount',
        'totalBudget.amount',
        'type',
        'unitCost.amount',
        'creativeSelection',
        'runSchedule.start',
        'runSchedule.end',
    ]]

    print(active_campaigns)

    print('')

    # exaconnect.writePandas(active_campaigns, 'UNIVERSITY_WAREHOUSE_STAGING.LINKEDIN_CAMPAIGNS')

    exacontact = exaconnect.readData("select distinct id, created_date from UNIVERSITY_WAREHOUSE_STAGING.LINKEDIN_CAMPAIGNS")
    exadf = pd.DataFrame(exacontact)

    print(exadf)

    fulldf = pd.merge(active_campaigns, exadf, left_on='id', right_on='ID', how='left')

    print(fulldf)
    #TODO modified uupdaatteddss