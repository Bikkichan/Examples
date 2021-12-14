import json
 
sf_login = "SalesforceLogin_KW.json"

# Opening JSON file
sf_open = open(sf_login)
 
# returns JSON object as a dictionary
creds = json.load(sf_open)

USERNAME = creds['login']['username'],
PASSWORD = creds['login']['password'],
SECURITY_TOKEN = creds['login']['token'],
VERSION = '47.0'