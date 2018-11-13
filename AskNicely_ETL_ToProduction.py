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
    MERGE INTO CU_ONLINE_MARKETING.ASK_NICELY_RESPONSES prd
    USING CU_ONLINE_MARKETING_STG.ASK_NICELY_RESPONSES stg
    ON (prd.RESPONSE_ID = stg.RESPONSE_ID
    and prd.RESPONSE_DATE = stg.RESPONSE_DATE)
    WHEN NOT MATCHED THEN INSERT VALUES
      (
        stg.RESPONSE_ID,
        stg.RESPONSE_DATE,
        stg.NAME,
        stg.SEGMENT,
        stg.EMAIL,
        stg.ANSWER
      );

    """

if __name__ == '__main__':

    exaconnect.execute(etl_merge_statement)
