import Facebook_Ads
import GoogleAdwords_GetReport
import LinkedIn_Campaigns
import LinkedIn_AdsByCampaign
import LinkedIn_CampaignCreativeLink
import AskNicely_GetResponses
import HubSpot_SocialPosts
import HubSpot_Contacts_ModifiedContacts
import HubSpot_Contacts_InfoByContact
import Facebook_ETL_ToProduction
import GoogleAdWords_ETL_ToProduction
import LinkedIn_ETL_ToProduction
import AskNicely_ETL_ToProduction
import HubSpot_SocialPosts_ETL_ToProduction
import HubSpot_Contacts_ETL_ToProduction
import exasol as e
import config_Exasol as ec
import datetime as dt

exaconnect = e.connect(
            dsn=ec.dsn,
            DRIVER=ec.DRIVER,
            EXAHOST=ec.EXAHOST,
            EXAUID=ec.EXAUID,
            EXAPWD=ec.EXAPWD,
            autocommit=True
            )

# FACEBOOK_ADS updated schema on 072518
exasol_import_error_db = 'CU_ONLINE_MARKETING_STG.MARKETING_ETL_ERRORS'

files = [
    'Facebook_Ads',
    'GoogleAdwords_GetReport',
    'LinkedIn_Campaigns',
    'LinkedIn_AdsByCampaign',
    'LinkedIn_CampaignCreativeLink',
    'AskNicely_GetResponses',
    'HubSpot_SocialPosts',
    'HubSpot_Contacts_ModifiedContacts',
    'HubSpot_Contacts_InfoByContact',
    'Facebook_ETL_ToProduction',
    'GoogleAdWords_ETL_ToProduction',
    'LinkedIn_ETL_ToProduction',
    'AskNicely_ETL_ToProduction',
    'HubSpot_SocialPosts_ETL_ToProduction',
    'HubSpot_Contacts_ETL_ToProduction'
]

if __name__ == '__main__':

    for f in files:
        run_file = f + '.py'
        try:
            execfile(run_file)
        except:
            pass;
        #     time_now = dt.datetime.now()
        #     time_now_str = str(time_now)
        #     print(time_now_str)
        #     etl_error_statement = "INSERT INTO " + exasol_import_error_db + " values (" + time_now_str + ", " + run_file + ", ETL Failed )"
        #     print(etl_error_statement)
        #     exaconnect.execute(etl_error_statement)
