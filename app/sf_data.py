import config 

#%%

# SOQL scripts
# function to pass query to - makes a connection with SF credentials

def query_sf(query):
    # Connection to salesforce - refer to JSON file
    with open("SalesforceLogin_KW.json", "r") as login_file:
        creds = json.load(login_file)

    sf = Salesforce(username=creds['login']['username'],
                    password=creds['login']['password'],
                    security_token=creds['login']['token'],
                    version='47.0')

    sf.query_all(query)
    return pd.json_normalize(sfdata_raw['records'])


def TIPP1_FR_data(date_string):
    query = """
        SELECT
            Form_Response_Section__r.Form_Response__r.Name, 
            Answer__c,
            Form_Question__r.Name,
            Form_Response_Section__r.Form_Response__r.Submitted__c, 
            Form_Response_Section__r.Form_Response__r.User__r.Username,
            (SELECT 
                BodyLength, 
                Name 
            FROM Attachments 
            WHERE ParentId != Null)
        FROM Form_Response_Answer__c 
        WHERE Form_Response_Section__r.Form_Response__r.Form__r.Name = 'F-3315' 
        AND Form_Response_Section__r.Form_Response__r.Submitted__c > {}T00:00:00+10:00
        """.format(date_string)
    return query 