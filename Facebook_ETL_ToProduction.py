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

# FACEBOOK_ADS deprecated fields on 0725
etl_merge_statement = """
    MERGE INTO CU_ONLINE_MARKETING.FACEBOOK_ADS prd
    USING CU_ONLINE_MARKETING_STG.FACEBOOK_ADS_0818 stg
    ON (prd.AD_ID = stg.AD_ID
    and prd.AD_DATE = stg.DATE_START)
    WHEN NOT MATCHED THEN INSERT VALUES
      (
        left(stg.CAMPAIGN_NAME,4),
        stg.DATE_START,
        stg.ACCOUNT_ID,
        stg.CAMPAIGN_ID,
        stg.CAMPAIGN_NAME,
        stg.AD_ID,
        stg.ADSET_ID,
        stg.ADSET_NAME,
        stg.CLICKS,
        stg.CPC,
        stg.CPM,
        stg.CPP,
        stg.CTR,
        stg.FREQUENCY,
        stg.IMPRESSIONS,
        stg.INLINE_LINK_CLICK_CTR,
        stg.INLINE_LINK_CLICKS,
        stg.OBJECTIVE,
        stg.REACH,
        '', -- stg.SOCIAL_CLICKS, -- deprecated 0718
        '', -- stg.SOCIAL_IMPRESSIONS, -- deprecated 0718
        '', -- stg.SOCIAL_REACH, -- deprecated 0718
        '', -- stg.SOCIAL_SPEND, -- deprecated 0718
        stg.SPEND,
        stg.TOTAL_ACTION_VALUE,
        '', -- stg.TOTAL_ACTIONS, -- deprecated 0718
        '', -- stg.UNIQUE_CLICKS, -- deprecated 0718
        '', -- stg.UNIQUE_CTR, -- deprecated 0718
        '' -- stg.UNIQUE_SOCIAL_CLICKS -- deprecated 0718
      );

    """

if __name__ == '__main__':

    exaconnect.execute(etl_merge_statement)
