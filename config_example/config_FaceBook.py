# config for CU Online Facebook acccount

import hashlib
import hmac

my_app_id = ''
my_app_secret = ''
# CU Online System User Token
my_access_token = ''
my_ad_account_id = ''

hashed = hmac.new(
    my_app_secret.encode('utf-8'),
    msg=my_access_token.encode('utf-8'),
    digestmod=hashlib.sha256
)

my_appsecret_proof = hashed.hexdigest()

# https://graph.facebook.com/v{n}/{request-path}
api = 'https://graph.facebook.com'
version = 'v3.0'
root_node = my_ad_account_id
edge = 'insights'

facebook_api_url = "{}/{}/{}/{}".format(api, version, root_node, edge)
