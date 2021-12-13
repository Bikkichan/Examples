import json
 
sf_login = "SalesforceLogin_KW.json"

# Opening JSON file
sf_open = open(sf_login)
 
# returns JSON object as a dictionary
creds = json.load(sf_open)

username = creds['login']['username'],
password = creds['login']['password'],
security_token = creds['login']['token'],
version = '47.0'