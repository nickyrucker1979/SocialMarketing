from googleads import oauth2

developer_token = ''
client_customer_id = ''
client_id = ''
client_secret = ''
refresh_token = ''

oauth2_client = oauth2.GoogleRefreshTokenClient(
    client_id,
    client_secret,
    refresh_token
)
