import config 
import pandas as pd
from simple_salesforce import Salesforce

#%%

# SOQL scripts
# function to pass query to - makes a connection with SF credentials

def query_sf(query):
    sf = Salesforce(username = config.USERNAME, 
                    password = config.PASSWORD,
                    security_token = config.SECURITY_TOKEN,
                    version = config.VERSION)

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