import exasol as e
import config_Exasol as ec
import pandas as pd

exaconnect = e.connect(
            dsn=ec.dsn,
            DRIVER=ec.DRIVER,
            EXAHOST=ec.EXAHOST,
            EXAUID=ec.EXAUID,
            EXAPWD=ec.EXAPWD,
            autocommit=True
            )
exasol_import_db = 'CU_ONLINE_MARKETING.HUBSPOT_SOCIAL_POSTS'

# CAMPAIGN_ID is manually, hard-coded transformed since no API is available to join on - go to campaigns in Hubspot frontend to see guid in url
etl_merge_statement = """
    MERGE INTO CU_ONLINE_MARKETING.HUBSPOT_SOCIAL_POSTS prd
    USING CU_ONLINE_MARKETING_STG.HUBSPOT_SOCIAL_POSTS stg
    ON (prd.POST_ID = stg.POST_ID)
    WHEN MATCHED THEN UPDATE SET
         prd.CLICKS = stg.CLICKS,
         prd.LIKES = stg.LIKES,
         prd.RETWEETS = stg.RETWEETS,
         prd.REPLIES = stg.REPLIES,
         prd.ITERACTIONS = stg.ITERACTIONS
    WHERE prd.POST_ID = stg.POST_ID
    WHEN NOT MATCHED THEN INSERT VALUES
      (
        stg.POST_ID,
        case stg.CAMPAIGN_ID
            when '15cc12bb-009b-4874-aa24-c73417225558' then 'MSIS'
            when '93d7e13b-73f7-4c8c-a196-f7bbf4ae0e0b' then 'BAPS'
            when 'b8820d37-72ac-42d9-bf3e-08e4168873cd' then 'MCJ'
            else 'CU Online'
        end,
        case stg.POST_CHANNEL
            when 'GooglePlusPage' then 'Google Plus'
            when 'FacebookPage' then 'Facebook'
            when 'LinkedInCompanyPage' then 'LinkedIn'
            else stg.POST_CHANNEL
        end,
        stg.CREATED_DATE,
        stg.POST_TITLE,
        stg.POST_URL,
        stg.CLICKS,
        stg.LIKES,
        stg.RETWEETS,
        stg.REPLIES,
        stg.ITERACTIONS

      );

    """

if __name__ == '__main__':

    exaconnect.execute(etl_merge_statement)
