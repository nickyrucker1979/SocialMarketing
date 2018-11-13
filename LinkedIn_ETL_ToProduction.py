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

# LinkedIn's API pulls in today's data (others pull through yesterday).  Delete max date since it is incomplete and merge new results).
etl_delete_yesterdays_data = """
    delete from CU_ONLINE_MARKETING.LINKEDIN_ADS prd
    where prd.AD_DATE =
    (Select MAX(AD_DATE) from CU_ONLINE_MARKETING.LINKEDIN_ADS)
    """

etl_merge_statement = """
    MERGE INTO CU_ONLINE_MARKETING.LINKEDIN_ADS prd
    USING CU_ONLINE_MARKETING_STG.LINKEDIN_ADS_VIEW stg
    ON (prd.CAMPAIGN_ID = stg.CAMPAIGN_ID
    and prd.AD_DATE = stg.AD_DATE)
    WHEN NOT MATCHED THEN INSERT VALUES
      (
        AD_DATE,
        CAMPAIGN_ID,
        PROGRAM,
        CAMPAIGN_NAME,
        CREATED_DATE,
        COST_TYPE,
        DAILY_BUDGET,
        OBJECTIVE_TYPE,
        CAMPAIGN_TYPE,
        UNIT_COST_AMOUNT,
        '', -- 'DEPRECATED_START_DATE' 8-16-18
        '', -- 'DEPRECATED_END_DATE' 8-16-18
        CREATIVE_ID,
        CLICKS,
        AD_COMMENTS,
        COMPANY_PAGE_CLICKS,
        COST,
        IMPRESSIONS,
        FOLLOWS,
        SHARES,
        VIRAL_CLICKS,
        VIRAL_FOLLOWS,
        VIRAL_IMPRESSIONS,
        VIRAL_COMMENTS,
        VIRAL_SHARES
      );

    """

if __name__ == '__main__':

    exaconnect.execute(etl_delete_yesterdays_data)
    exaconnect.execute(etl_merge_statement)
