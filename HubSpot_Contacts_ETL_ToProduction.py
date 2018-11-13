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


etl_delete_prd_contacts = """
    merge into CU_ONLINE_MARKETING.HUBSPOT_CONTACTS prd
    using CU_ONLINE_MARKETING_STG.HUBSPOT_CONTACTS_VIEW stg
        on (prd.VID = stg.VID)
    when matched then delete
    ;
    """

etl_contacts_to_prd = """
    merge into CU_ONLINE_MARKETING.HUBSPOT_CONTACTS prd
    using CU_ONLINE_MARKETING_STG.HUBSPOT_CONTACTS_VIEW stg
        on (prd.VID = stg.VID)
    when not matched then insert values
        (
          stg.VID,
          stg.FIRST_NAME,
          stg.LAST_NAME,
          stg.ACADEMIC_PROGRAM,
          stg.CREATED_DATE,
          stg.LIFECYCLE_STAGE,
          stg.LIFECYCLE_STAGE_DATE,
          stg.CONVERSION_NAME,
          stg.CONVERSION_DATE,
          stg.NUM_PAGE_VIEWS,
          stg.HOME_CITY,
          stg.HOME_STATE,
          stg.HOME_ZIP,
          stg.HUBSPOT_SCORE,
          stg.TWITTER,
          stg.LINKED_IN,
          stg.LINKED_IN_BIO,
          stg.PHONE,
          stg.SPECIALIZATIONS,
          stg.EMAIL,
          stg.CU_EMAIL,
          stg.LEAD_STATUS,
          stg.EMAILS_SINCE_LAST_ENGAGEMENT,
          stg.RESIDENCY,
          stg.CU_ADMIT,
          stg.CLOSED_DATE,
          stg.LAST_MODIFIED_DATE,
          stg.KLOUTSCORE,
          stg.IMPORT_DATE
    );

    """


if __name__ == '__main__':

    exaconnect.execute(etl_delete_prd_contacts)
    exaconnect.execute(etl_contacts_to_prd)
