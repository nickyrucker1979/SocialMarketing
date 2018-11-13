import config_LinkedIn as lc

clientid = lc.client_id
client_secret = lc.client_secret

# Run this script:
# https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id=864jwz2i8dr5tp&redirect_uri=https://www.google.com%2Fauth%2Flinkedin&state=1008916290
#
# Take returned google parameter and pass the code into this script:
# https://www.linkedin.com/oauth/v2/accessToken?grant_type=authorization_code&code=AQSN7HCZ1HEZHkKFBneQmtuYq2D1O6R7Z-SlTSnK6I_PrO5LsnB3Zio982gVhE33lXG01VNAQyqotUYIWGm30RnE1UgfSfcPC8iP7tJtS56dXCseXovR4wBTqHMO1RTjA-KY7bh31_E1nCXW_6VwNzW_Ict0DQ&redirect_uri=https://www.google.comm&client_id=864jwz2i8dr5tp&client_secret=YE7LJJaF16q0W1TK


# run the url below to return a code to pass into token url to return linkedin token
get_code_url = 'https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id=' + clientid + '&redirect_uri=https://www.google.com&state=1008916290'

returned_code = '' # Take returned google parameter and pass the code
get_token_url = 'https://www.linkedin.com/oauth/v2/accessToken?grant_type=authorization_code&code=' + returned_code + '&redirect_uri=https://www.google.com&client_id=' + clientid + '&client_secret=' + client_secret
