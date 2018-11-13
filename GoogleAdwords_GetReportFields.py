from googleads import adwords
import config_GoogleAdWords as gadc
import pandas as pd

adwords_client = adwords.AdWordsClient(
    gadc.developer_token,
    gadc.oauth2_client,
    client_customer_id=gadc.client_customer_id
)

report_name = 'AD_PERFORMANCE_REPORT'


# # print the fields in the report and datatypes
def get_fields_in_report(client, report_type):
    # Initialize appropriate service.
    report_definition_service = client.GetService(
        'ReportDefinitionService', version='v201802')

    # Get report fields.
    fields = report_definition_service.getReportFields(report_type)

    # Display results.
    print('Report type "%s" contains the following fields:' % report_type)
    for field in fields:
        print(' - %s (%s)' % (field['fieldName'], field['fieldType']))
        if 'enumValues' in field:
            print('  := [%s]' % ', '.join(field['enumValues']))


if __name__ == '__main__':
    get_fields_in_report(adwords_client, report_name)
