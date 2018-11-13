from googleads import adwords
import config_GoogleAdWords as gadc
import pandas as pd
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

exasol_import_db = 'CU_ONLINE_MARKETING_STG.GOOGLE_ADWORDS'

adwords_client = adwords.AdWordsClient(
    gadc.developer_token,
    gadc.oauth2_client,
    client_customer_id=gadc.client_customer_id
)

report_name = 'AD_PERFORMANCE_REPORT'
api_version = 'v201802'


def percent_sign_to_float(x):
    return float(x.strip('%')) / 100


def df_convert_percent(dataframe, field):
    dataframe[field] = dataframe[field].apply(lambda x: percent_sign_to_float(x))
    return

def df_convert_currency(dataframe, field):
    dataframe[field] = dataframe[field].apply(lambda x: float(x) / float(1000000))

def get_report_data(client, report_type):
    report_downloader = client.GetReportDownloader(version=api_version)

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                    .Select('Date',
                            'CampaignId',
                            'CampaignName',
                            'AdGroupId',
                            'AdGroupName',
                            'AdGroupStatus',
                            'AdType',
                            'Description',
                            'HeadlinePart1',
                            'Id',
                            'Status',
                            'Clicks',
                            'ConversionRate',
                            'Conversions',
                            'Cost',
                            'CostPerConversion',
                            'Ctr',
                            'EngagementRate',
                            'Engagements',
                            'Impressions',
                            'Interactions',
                            'PercentNewVisitors',
                            'VideoViews')
                    .From(report_type)
                    .Where('CampaignStatus').In('ENABLED')
                    # .Where('AdGroupStatus').In('ENABLED')
                    # .Where('Status').In('ENABLED')
					 .During('LAST_30_DAYS')
                    # .During('LAST_MONTH')  # https://developers.google.com/adwords/api/docs/guides/reporting#date_ranges
                    # .During('20180101, 20180524')  # Custom date range
                    .Build())

    # You can provide a file object to write the output to. For this
    # demonstration we use sys.stdout to write the report to the screen.
    downloaded_report = report_downloader.DownloadReportAsStreamWithAwql(report_query,
                                                                         'CSV',
                                                                         skip_report_header=True,
                                                                         skip_column_header=False,
                                                                         skip_report_summary=True,
                                                                         include_zero_impressions=True)
    df = pd.read_csv(downloaded_report)
    return df


if __name__ == '__main__':

    exaconnect.execute('truncate table ' + exasol_import_db)

    report_df = get_report_data(adwords_client, report_name)

    df_convert_percent(report_df, 'CTR')
    df_convert_percent(report_df, 'Engagement rate')
    df_convert_percent(report_df, '% new sessions')
    df_convert_percent(report_df, 'Conv. rate')
    df_convert_currency(report_df, 'Cost')
    df_convert_currency(report_df, 'Cost / conv.')

    print(report_df)
    # print(report_df.columns)
    # report_df.to_csv('ActiveAdsPerformance.csv')

    exaconnect.writePandas(report_df, exasol_import_db)
