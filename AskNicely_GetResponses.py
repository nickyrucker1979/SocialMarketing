import requests
import pandas as pd
import json
import datetime
from time import mktime
import exasol as e
import config_Exasol as ec
import config_AskNicely as anc

exaconnect = e.connect(
            dsn=ec.dsn,
            DRIVER=ec.DRIVER,
            EXAHOST=ec.EXAHOST,
            EXAUID=ec.EXAUID,
            EXAPWD=ec.EXAPWD,
            autocommit=True
            )

exasol_import_db = 'CU_ONLINE_MARKETING_STG.ASK_NICELY_RESPONSES'

today = datetime.date.today()
yesterday = (today - datetime.timedelta(7))
unix_yesterday = str(int(mktime(yesterday.timetuple())))
# # Epoch Time on Jan 1 2018
# unix_jan1 = str("1514764800")

url = "https://universityofcolorado1.asknice.ly/api/v1/responses/desc/5000/1/" + str(unix_yesterday) + "/json?X-apikey="
api_token = anc.api_token

def get_dataframe(passed_url, passed_api_token, dataframe_element):
    get_active_url = requests.request("GET", passed_url + passed_api_token)
    json_details = json.loads(get_active_url.text)[dataframe_element]
    df = pd.DataFrame(json_details)
    return df

def cast_unix_to_datetime(dataframe, field):
    dataframe[field] = pd.to_datetime(dataframe[field], unit='s').dt.date

# def replace_special_chars(dataframe, field):
#     dataframe[field] = dataframe[field].str.replace('&39;', '')
#     dataframe[field] = dataframe[field].str.replace('&#39;', '')
#     dataframe[field] = dataframe[field].str.replace('&amp;', 'and')

if __name__ == '__main__':

    exaconnect.execute('truncate table ' + exasol_import_db)

    ask_nicely_df = get_dataframe(url, api_token, 'data')
    if ask_nicely_df.empty:
        print('no results')
        pass;
    else:
        cast_unix_to_datetime(ask_nicely_df, 'responded')
        # replace_special_chars(ask_nicely_df, 'comment')

        ask_nicely = ask_nicely_df[[
            'response_id',
            'responded',
            'name',
            'segment',
            'email',
            'answer'
            # 'comment',

        ]]

        print(ask_nicely)
        # print('')
        # print(list(ask_nicely_df))

        exaconnect.writePandas(ask_nicely, exasol_import_db)
