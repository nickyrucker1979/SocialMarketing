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

etl_merge_statement = """
    MERGE INTO CU_ONLINE_MARKETING.GOOGLE_ADWORDS prd
    USING CU_ONLINE_MARKETING_STG.GOOGLE_ADWORDS stg
    ON (prd.AD_ID = stg.AD_ID
    and prd.AD_DATE = stg.AD_DATE)
    WHEN NOT MATCHED THEN INSERT VALUES
      (
        stg.AD_DATE,
        stg.CAMPAIGN_ID,
        stg.CAMPAIGN_NAME,
        stg.ADGROUP_ID,
        stg.ADGROUP_NAME,
        stg.ADGROUP_STATUS,
        stg.AD_TYPE,
        stg.DESCRIPTION,
        stg.HEADLINE,
        stg.AD_ID,
        stg.AD_STATUS,
        stg.CLICKS,
        stg.CONVERSION_RATE,
        stg.CONVERSIONS,
        stg.COST,
        stg.COST_PER_CONVERSION,
        stg.CLICK_THROUGH_RATE,
        stg.ENGAGEMENT_RATE,
        stg.ENGAGEMENTS,
        stg.IMPRESSIONS,
        stg.INTERACTIONS,
        stg.PERCENT_INTERACTIONS_NEW,
        stg.VIDEO_VIEWS
      );

    """

if __name__ == '__main__':

    exaconnect.execute(etl_merge_statement)
