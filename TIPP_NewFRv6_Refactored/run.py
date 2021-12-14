import pandas as pd
from simple_salesforce import Salesforce
from SalesforceXytoolsCore import RestApi
import json
import func
from tqdm import tqdm
import os
import requests



# Save file locations:

bank_dups_csv = 'Bank_Dupes.csv'
user_dupes_csv = 'Username_Dupes.csv'
ip_dups_csv = 'IP_Dupes.csv'
mobile_dupes_csv = 'Mobile_Dupes.csv'
email_dupes_csv = 'Email_Dupes.csv'


# read files
def read_master_list():
    # dont use absolute file paths
    # master_list = pd.read_csv('../Current/All_Forms.csv', dtype=str) 

    csv_path = os.path.join('..','Current','All_Forms.csv')
    master_list = pd.read_csv(csv_path, dtype=str)
    master_list = master_list.replace(to_replace="\.0+$", value="", regex=True)

    master_list['Submitted'] = pd.to_datetime(master_list['Submitted'], dayfirst=True)
    master_list = master_list.sort_values(by='Submitted',ascending=True)
    master_list.drop(master_list.filter(regex="Unname"),axis=1, inplace=True)

    return master_list 


def save_master_list(master_list):
    csv_path = os.path.join('..','Current','All_Forms.csv')
    master_list.to_csv(csv_path, index=False)


def load_frankbook():
    # start line 26
    # Load Frankenbook and remove groups assessed "OK"
    #frank = pd.read_excel('../Consolidated_TIPP_workbook.xlsx', 'Follow Up - Referrals', converters={k: str for k in range(98)})
    frank = pd.read_excel('../Consolidated_TIPP_workbook.xlsx', 'Follow Up - Referrals', dtype=str)
    frank = frank[['Updated Folder/Group Name', 'Group (old ref)', 'Category', 'User Username', 'Mobile', 'Email', 'BSB and Account']].copy()

    frank = frank[frank['Category'] != "OK"]
    frank['Email'] = frank['Email'].str.lower()
    groupnums = list(map(float, (frank['Group (old ref)'].unique())))
    print('\nCurrent max group: {0}\n'.format(max(groupnums)))

    return frank


def exclude_subset(master, results):
    temp_date = pd.Timestamp('today').floor('d') - pd.Timedelta(5, unit = 'd')
    
    subset = master[(master['Submitted']>temp_date)]
    
    return results[~results['Form Response Name'].isin(subset['Form Response Name'])]


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


# queries
def TIPP1_FR_data(date_string):
    query = """
        SELECT
            Form_Response_Section__r.Form_Response__r.Name, 
            Answer__c,
            Form_Question__r.Name,
            Form_Response_Section__r.Form_Response__r.Submitted__c, 
            Form_Response_Section__r.Form_Response__r.User__r.Username,
            (SELECT BodyLength, Name from Attachments WHERE ParentId != Null)
        FROM Form_Response_Answer__c 
        WHERE Form_Response_Section__r.Form_Response__r.Form__r.Name = 'F-3315' 
        AND Form_Response_Section__r.Form_Response__r.Submitted__c > {0}T00:00:00+10:00
        """.format(date_string)
    return query_sf(query)
    

def TIPPDVS_FR_data(date_string):
    query = """
        SELECT
            Form_Response_Section__r.Form_Response__r.Name, 
            Answer__c,
            Form_Question__r.Name,
            Form_Response_Section__r.Form_Response__r.Submitted__c, 
            Form_Response_Section__r.Form_Response__r.User__r.Username,
            (SELECT BodyLength, Name from Attachments WHERE ParentId != Null)
        FROM Form_Response_Answer__c 
        WHERE Form_Response_Section__r.Form_Response__r.Form__r.Name = 'F-3464'
        AND Form_Response_Section__r.Form_Response__r.Internal_Status__c = 'Accepted'
        AND Form_Response_Section__r.Form_Response__r.Submitted__c > {0}T00:00:00+10:00
        """.format(date_string)
    return query_sf(query)


def TIPPnonDVS_FR_data(date_string):
    query = """
        SELECT
            Form_Response_Section__r.Form_Response__r.Name, 
            Answer__c,
            Form_Question__r.Name,
            Form_Response_Section__r.Form_Response__r.Submitted__c, 
            Form_Response_Section__r.Form_Response__r.User__r.Username,
            (SELECT BodyLength, Name from Attachments WHERE ParentId != Null)
        FROM Form_Response_Answer__c 
        WHERE Form_Response_Section__r.Form_Response__r.Form__r.Name = 'F-3493'
        AND Form_Response_Section__r.Form_Response__r.Submitted__c > {0}T00:00:00+10:00
        """.format(date_string)
    return query_sf(query)


def LOGIN_IP_data(date_string):
    query = """
        SELECT 
            Username, 
            EventDate, 
            SourceIp, 
            Status, 
            Browser, 
            Platform, 
            Country, 
            Subdivision, 
            City, 
            PostalCode
        FROM LoginEvent 
        WHERE EventDate > {0}T00:00:00+08:00
        """.format(date_string)
    return query_sf(query)


def OPPS_data(fr_list):
    query = """
        SELECT 
            Id, 
            OpportunityCode__c, 
            Name, 
            Risk_Mitigation__c, 
            Risk_Description__c, 
            Form_Response__r.Application__r.Status__c, 
            Form_Response__r.Name,
            Form_Response__r.Opportunity__r.StageName,
            Form_Response__r.Application__r.Name, 
            Form_Response__r.Application__r.Id
        FROM Opportunity
        WHERE Form_Response__r.Name IN {0}
        """.format(fr_list)
    return query_sf(query)


# data formating
def format_df(df):
    cols = [
        'Form_Response_Section__r.Form_Response__r.Name',
        'Form_Question__r.Name', 
        'Attachments.records']
    
    subset_cols = [
        'Form_Response_Section__r.Form_Response__r.Name',
        'Form_Question__r.Name']
    
    rename_cols = {
        'Form_Response_Section__r.Form_Response__r.Name': 'Form Response Name',
        'Form_Question__r.Name': 'Form Question Name'}

    attachment_df = df[cols].copy()
    attachment_df.drop_duplicates(subset=subset_cols, keep='last', inplace=True)
    attachment_df.dropna(axis=0, inplace=True)
    attachment_df.rename(columns=rename_cols, inplace=True)
    return df


def unpack_data(df):
    for index, row in df.iterrows():
        df.loc[index, 'url'] = row['Attachments.records'][0]['attributes']['url'] 
        df.loc[index, 'name'] = row['Attachments.records'][0]['Name']
        df.loc[index, 'filesize'] = row['Attachments.records'][0]['BodyLength']

    df.drop(['Attachments.records'], axis=1, inplace=True)
    return df


# sf dataframes
def TIPPDVS_FR_attachments(df):
    # Create new dataframe with pivot containing attachment records
    attachments = format_df(df)

    # unpack attachment data
    attachments = unpack_data(attachments)
    
    # Rename question names
    attachments['Form Question Name'].replace({
       'FQ-148828':'Evidence of Employment',
       'FQ-151803':'Letter requesting COVID test'}, 
       inplace=True)

    return attachments


def TIPPDVS_FR_format(df, attachments):
    # make pivot table
    attach_pivot = attachments.pivot(index='Form Response Name', columns='Form Question Name', values=['filesize', 'name']).reset_index()

    # extract column names
    attach_pivot.columns = [f'{i}.{j}' for i, j in attach_pivot.columns]
    
    # rename columns
    attach_pivot.rename(columns={
        'Form Response Name.':'Form Response Name',
        'filesize.Evidence of Employment':'Employment Evidence File Size (Bytes)',
        'name.Evidence of Employment':'Employment Evidence File Name',
        'filesize.Letter requesting COVID test':'Letter Requesting Test File Size (Bytes)',
        'name.Letter requesting COVID test':'Letter Requesting Test File Name'},
        inplace=True)

    # Pivot and merge dfs
    tipp_dvs_fr = func.pivot_frs(df, attach_pivot)
    
    # drop column names
    drop_qs3464 = [
        'FQ-148562', 
        'FQ-148599',
        'FQ-151747',
        'FQ-148665',
        'FQ-148601',
        'FQ-148602',
        'FQ-148603',
        'FQ-148604',
        'FQ-148605',
        'FQ-148606',
        'FQ-148607',
        'FQ-148610',
        'FQ-148611',
        'FQ-148615',
        'FQ-148616',
        'FQ-148617',
        'FQ-148620',
        'FQ-148621',
        'FQ-148622',
        'FQ-151748',
        'FQ-148590',
        'FQ-148666',
        'FQ-151749',
        'FQ-148564',
        'FQ-148565',
        'FQ-148567',
        'FQ-148672',
        'FQ-148673',
        'FQ-148740',
        'FQ-148743',
        'FQ-153944',
        'FQ-148791',
        'FQ-148792',
        'FQ-148814',
        'FQ-148794',
        'FQ-148795',
        'FQ-148796',
        'FQ-148815',
        'FQ-148799',
        'FQ-148800',
        'FQ-148801',
        'FQ-148802',
        'FQ-148803',
        'FQ-148805',
        'FQ-148806',
        'FQ-148807',
        'FQ-148808',
        'FQ-148809',
        'FQ-148810',
        'FQ-148804',
        'FQ-148811',
        'FQ-148812',
        'FQ-148813',
        'FQ-148817',
        'FQ-151758',
        'FQ-151759',
        'FQ-151760',
        'FQ-151761',
        'FQ-151764',
        'FQ-149049',
        'FQ-149050',
        'FQ-149051',
        'FQ-149127',
        'FQ-151766',
        'FQ-149040',
        'FQ-149041',
        'FQ-149042',
        'FQ-153295',
        'FQ-151768',
        'FQ-151767',
        'FQ-148644',
        'FQ-151783',
        'FQ-148819',
        'FQ-151792',
        'FQ-148645',
        'FQ-159852',
        'FQ-148824',
        'FQ-148825',
        'FQ-148826',
        'FQ-148660',
        'FQ-148636',
        'FQ-148637',
        'FQ-148632',
        'FQ-148633',
        'FQ-148634',
        'FQ-151807',
        'FQ-151808',
        'FQ-151809',
        'FQ-176893',
        'FQ-176894',
        'FQ-177320',
        'FQ-178059',
        'FQ-178060',
        'FQ-178061',
        'FQ-178062',
        'FQ-178063']

    # drop columns
    tipp_dvs_fr.drop(drop_qs3464, axis=1, inplace=True)

    # rename columns
    tipp_dvs_fr.rename(columns={
        'FQ-158673':'Title',
        'FQ-148609':'I am 17 years and older',
        'FQ-148563':'I confirm that I have read and understood the Program Guidelines',
        'FQ-148573':'First Name',
        'FQ-148828':'Evidence of Employment',
        'FQ-148657':'Bank Account Name',
        'FQ-148572':'Last Name',
        'FQ-148658':'BSB',
        'FQ-148580':'Date of Birth',
        'FQ-148608':'I have been tested for COVID-19 or I am required to care for someone who is required to self-isolate',
        'FQ-148659':'Account No',
        'FQ-148600':'I currently live in Victoria',
        'FQ-148661':'BSB number and Account number are correct',
        'FQ-148612':'I am unable to work as a result of the requirement to self-isolate',
        'FQ-148662':'Has anyone else used these bank details when applying for payment under this program?',
        'FQ-148613':'I am not receiving any income, earnings or salary maintenance from work as a result of self-isolation at home',
        'FQ-148635':'Full name of person completing this application',
        'FQ-151804':'Are you self employed?',
        'FQ-148575':'Mobile',
        'FQ-148614':'I have exhausted sick leave and carers leave entitlements',
        'FQ-148653':'ABN of business you are employed at',
        'FQ-148663':'I am not receiving income support (JobKeeper or JobSeeker)',
        'FQ-148664':'I am not receiving another worker support payment, including the PLDP',
        'FQ-148643':'Name of the business that you are primarily employed at',
        'FQ-148576':'Email',
        'FQ-148646':'Employer Street Address',
        'FQ-148625':'I agree that I may have to repay the funds if I do not self-isolate while waiting for my test results',
        'FQ-148647':'Employer Suburb/Town',
        'FQ-148568':'Residential Street Address',
        'FQ-148648':'Employer Postcode',
        'FQ-148569':'Residential Suburb/Town',
        'FQ-148655':'Primary occupation industry',
        'FQ-148570':'Residential Postcode',
        'FQ-148656':'Other (please specify)',
        'FQ-148649':'Name of the person who can verify your employment details',
        'FQ-148651':'Contact Number of the person above',
        'FQ-148587':'Are you an Australian Citizen or Permanent Resident?',
        'FQ-153943':'Proof of Identity (select from list)',
        'FQ-148790':'Victorian Drivers Licence',
        'FQ-151757':'Centrelink Customer Reference Num',
        'FQ-148798':'Medicare Card',
        'FQ-149039':'Australian Passport',
        'FQ-149125':'Foreign Passport Number',
        'FQ-148741':'Visa Type',
        'FQ-149048':'Visa Grant Number',
        'FQ-149126':'Nationality on passport',
        'FQ-151803':'Letter requesting COVID test',
        'FQ-151802':'Were you directed by the Department of Health or your employer to get tested?',
        'FQ-151782':'Were you tested for COVID-19?',
        'FQ-151784':'First Name (person tested)',
        'FQ-151785':'Surname (person tested)',
        'FQ-151786':'Date of Birth (person tested)',
        'FQ-148640':'Date of COVID-19 test',
        'FQ-148827':'How many hours of paid work have you missed due to isolating?'},
        inplace=True)

    return tipp_dvs_fr


def TIPP1_FR_format(df):    
    print('\nFORM RESPONSE SUBMITTED USING OLD TIPP FORM F-3315\n')

    # Create new dataframe with pivot containing attachment records
    attachments = format_df(df)

    # unpack attachment data
    attachments = unpack_data(attachments)
    
    # Rename question names
    attachments['Form Question Name'].replace({
        'FQ-125335':'Proof of Identity',
        'FQ-125377':'Evidence of Employment',
        'FQ-125380':'Additional Attachment',
        'FQ-129615':'Letter requesting COVID test'}, 
        inplace=True)

    # Make pivot table
    attach_pivot = attachments.pivot(index='Form Response Name', columns='Form Question Name', values=['filesize', 'name']).reset_index()

    # extract column names
    attach_pivot.columns = [f'{i}.{j}' for i, j in attach_pivot.columns]
    
    # rename columns
    attach_pivot.rename(columns={
        'Form Response Name.':'Form Response Name', 
        'filesize.Evidence of Employment':'Employment Evidence File Size (Bytes)',
        'filesize.Proof of Identity':'ID File Size (Bytes)', 
        'name.Evidence of Employment':'Employment Evidence File Name',
        'name.Proof of Identity':'ID File Name', 
        'filesize.Additional Attachment':'Additional Attachment File Size (Bytes)', 
        'filesize.Letter requesting COVID test':'Letter Requesting Test File Size (Bytes)',
        'name.Additional Attachment':'Additional Attachment File Name',
        'name.Letter requesting COVID test':'Letter Requesting Test File Name'},
        inplace=True)

    # Pivot and merge dfs
    old_tipp_df = func.pivot_frs(df, attach_pivot)

    # drop column names
    drop_qs3315 = [
        'FQ-125350',
        'FQ-125733',
        'FQ-125734',
        'FQ-125752',
        'FQ-125324',
        'FQ-125325',
        'FQ-125326',
        'FQ-125327',
        'FQ-125328',
        'FQ-125329',
        'FQ-125330',
        'FQ-125333',
        'FQ-125334',
        'FQ-125381',
        'FQ-125382',
        'FQ-125383',
        'FQ-125384',
        'FQ-125385',
        'FQ-125726',
        'FQ-125727', 
        'FQ-125732', 
        'FQ-165138', 
        'FQ-165139',
        'FQ-165140', 
        'FQ-165141',
        'FQ-165142',
        'FQ-125735',
        'FQ-125736',
        'FQ-125737',
        'FQ-129617',
        'FQ-129618',
        'FQ-125316',
        'FQ-125300',
        'FQ-125724',
        'FQ-125757',
        'FQ-125754',
        'FQ-125755',
        'FQ-125756',
        'FQ-125356',
        'FQ-125360',
        'FQ-125364',
        'FQ-125346',
        'FQ-125347',
        'FQ-125365',
        'FQ-125370',
        'FQ-125374',
        'FQ-125730',
        'FQ-125731',
        'FQ-125339',
        'FQ-125340',
        'FQ-125341',
        'FQ-130334']
    
    # drop Columns
    old_tipp_df.drop(drop_qs3315, axis=1, inplace=True)

    # rename columns
    old_tipp_df.rename(columns={
        'FQ-125313':'Title',
        'FQ-125332':'I am 17 years and older',
        'FQ-125335':'Proof of Identity',
        'FQ-125351':'I confirm that I have read and understood the Program Guidelines',
        'FQ-125315':'First Name',
        'FQ-125377':'Evidence of Employment',
        'FQ-125345':'Date of COVID-19 test',
        'FQ-125371':'Bank Account Name',
        'FQ-125314':'Last Name',
        'FQ-125380':'Additional Attachment',
        'FQ-125372':'BSB',
        'FQ-125354':'Date of Birth',
        'FQ-125331':'I have been tested for COVID-19 or I am required to care for someone who is required to self-isolate',
        'FQ-125373':'Account No',
        'FQ-125322':'I currently live in Victoria',
        'FQ-125750':'Is there anyone else in your household who is applying or has recently applied for this payment?',
        'FQ-125375':'BSB number and Account number are correct',
        'FQ-125323':'I am unable to work as a result of the requirement to self-isolate',
        'FQ-125758':'I am completing this application as a carer/legal guardian',
        'FQ-125751':'Has anyone else used these bank details when applying for payment under this program?',
        'FQ-125353':'I am not receiving any income, earnings or salary maintenance from work as a result of self-isolation at home',
        'FQ-125342':'Full name of person completing this application',
        'FQ-125722':'Are you self employed?',
        'FQ-125317':'Mobile', 
        'FQ-125361':'I have exhausted sick leave and carers leave entitlements',
        'FQ-125723':'ABN of business you are employed at', 
        'FQ-125362':'I am not receiving income support (JobKeeper or JobSeeker)',
        'FQ-125348':'Name of the business that you are primarily employed at', 
        'FQ-125318':'Email', 
        'FQ-125366':'Employer Street Address',
        'FQ-125753':'I agree that I may have to repay the funds if I do not self-isolate while waiting for my test results',
        'FQ-125367':'Employer Suburb/Town',
        'FQ-125301':'Residential Street Address',
        'FQ-125368':'Employer Postcode',
        'FQ-125302':'Residential Suburb/Town',
        'FQ-125948':'Primary occupation industry',
        'FQ-125304':'Residential Postcode',
        'FQ-125949':'Other (please specify)',
        'FQ-125369':'Name of the person who can verify your employment details',
        'FQ-125386':'Contact Number of the person above',
        'FQ-125728':'Are you an Australian Citizen or Permanent Resident?',
        'FQ-125321':'Proof of Identity (select from list)',
        'FQ-125298':'Victorian Drivers Licence', 'FQ-125749':'Victorian Learner Permit',
        'FQ-125319':'Victorian Seniors Card',
        'FQ-125320':'Victorian Proof of Age Card',
        'FQ-125355':'Medicare Card',
        'FQ-125357':'Australian Passport',
        'FQ-125729':'Foreign Passport Number',
        'FQ-125358':'Visa Type',
        'FQ-125359':'Visa Grant Number',
        'FQ-126173':'Nationality on passport',
        'FQ-129615':'Letter requesting COVID test',
        'FQ-129616':'Were you directed by the Department of Health or your employer to get tested?'}, 
        inplace=True)

    # drivers licence / learner permit manipulation
    old_tipp_df['Victorian Drivers Licence'] = old_tipp_df.loc[old_tipp_df['Victorian Drivers Licence'].notnull()].apply(lambda x: func.remove_chars(x['Victorian Drivers Licence']), axis=1)

    old_tipp_df['Victorian Learner Permit'] = old_tipp_df.loc[old_tipp_df['Victorian Learner Permit'].notnull()].apply(lambda x: func.remove_chars(x['Victorian Learner Permit']), axis=1)

    old_tipp_df['Victorian Drivers Licence'] = old_tipp_df['Victorian Drivers Licence'] + old_tipp_df['Victorian Learner Permit']

    old_tipp_df.drop('Victorian Learner Permit',axis=1,inplace=True)

    return old_tipp_df


def TIPPnonDVS_FR_attachments(df):
    print('\nNON-DVS FORMS SUBMITTED\n')
    # Create new dataframe with pivot containing attachment records

    # Create new dataframe with pivot containing attachment records
    attachments = format_df(df)

    # unpack attachment data
    attachments = unpack_data(attachments)
    
    # Rename question names
    attachments.replace({
        'FQ-176900':'Proof of Identity',
        'FQ-176899':'Proof of Identity',
        'FQ-176898':'Proof of Identity',
        'FQ-154113':'Proof of Identity',
        'FQ-154106':'Proof of Identity',
        'FQ-176895':'Proof of Identity',
        'FQ-176896':'Proof of Identity',
        'FQ-176897':'Proof of Identity',
        'FQ-154040':'Evidence of Employment',
        'FQ-154048':'Letter requesting COVID test'}, 
        inplace=True)

    return attachments


def TIPPnonDVS_FR_format(df, attachments):
    # Make pivot table
    attach_pivot = attachments.pivot(index='Form Response Name', columns='Form Question Name', values=['filesize', 'name']).reset_index()

    # extract column names
    attach_pivot.columns = [f'{i}.{j}' for i, j in attach_pivot.columns]
    
    # rename columns
    attach_pivot.rename(columns={
        'Form Response Name.':'Form Response Name', 
        'filesize.Proof of Identity':'ID File Size (Bytes)', 
        'name.Proof of Identity':'ID File Name', 
        'filesize.Evidence of Employment':'Employment Evidence File Size (Bytes)',
        'name.Evidence of Employment':'Employment Evidence File Name',
        'filesize.Letter requesting COVID test':'Letter Requesting Test File Size (Bytes)',
        'name.Letter requesting COVID test':'Letter Requesting Test File Name'},
        inplace=True)

    # Pivot and merge dfs
    nondvs_fr = func.pivot_frs(df, attach_pivot)

    # drop column names
    drop_qs3493 = [
        'FQ-153962', 
        'FQ-153985',
        'FQ-154011',
        'FQ-154010',
        'FQ-153987',
        'FQ-153988',
        'FQ-153989',
        'FQ-153990',
        'FQ-153991',
        'FQ-153992',
        'FQ-153993',
        'FQ-153996',
        'FQ-153997',
        'FQ-154001',
        'FQ-154002',
        'FQ-154003',
        'FQ-154004',
        'FQ-154005',
        'FQ-154006',
        'FQ-153983',
        'FQ-153976',
        'FQ-153977',
        'FQ-153984',
        'FQ-153964',
        'FQ-153965',
        'FQ-153966',
        'FQ-153978',
        'FQ-153979',
        'FQ-153980',
        'FQ-153982',
        'FQ-176813',
        'FQ-154114',
        'FQ-176849',
        'FQ-176851',
        'FQ-176852',
        'FQ-176856',
        'FQ-176853',
        'FQ-176854',
        'FQ-176855',
        'FQ-176870',
        'FQ-176872',
        'FQ-176873',
        'FQ-176874',
        'FQ-176875',
        'FQ-176876',
        'FQ-176878',
        'FQ-176879',
        'FQ-176880',
        'FQ-176881',
        'FQ-176882',
        'FQ-176883',
        'FQ-176877',
        'FQ-176884',
        'FQ-176885',
        'FQ-176886',
        'FQ-176887',
        'FQ-176889',
        'FQ-176890',
        'FQ-176891',
        'FQ-176892',
        'FQ-154107',
        'FQ-154108',
        'FQ-154109',
        'FQ-154110',
        'FQ-154101',
        'FQ-154102',
        'FQ-154103',
        'FQ-161806',
        'FQ-161807',
        'FQ-161808',
        'FQ-161809',
        'FQ-176858',
        'FQ-176859',
        'FQ-176860',
        'FQ-176862',
        'FQ-176861',
        'FQ-176864',
        'FQ-176865',
        'FQ-176866',
        'FQ-176869',
        'FQ-154025',
        'FQ-154042',
        'FQ-154035',
        'FQ-154046',
        'FQ-154026',
        'FQ-159853',
        'FQ-154036',
        'FQ-154037',
        'FQ-154038',
        'FQ-154053',
        'FQ-154017',
        'FQ-154018',
        'FQ-154013',
        'FQ-154014',
        'FQ-154015',
        'FQ-154020',
        'FQ-154021',
        'FQ-154022',
        'FQ-177321']

    # drop columns
    nondvs_fr.drop(drop_qs3493, axis=1, inplace=True)

    # Combine all object IDs for POI attachments and Centrelink Customer Num
    nondvs_fr['FQ-176900'] = (
        nondvs_fr['FQ-176900'] + 
        nondvs_fr['FQ-176899'] + 
        nondvs_fr['FQ-176898'] + 
        nondvs_fr['FQ-154113'] + 
        nondvs_fr['FQ-154106'] + 
        nondvs_fr['FQ-176895'] + 
        nondvs_fr['FQ-176896'] + 
        nondvs_fr['FQ-176897'])
    
    nondvs_fr['FQ-176888'] = (
        nondvs_fr['FQ-176888'] + 
        nondvs_fr['FQ-161805'])

    # Drop extra FQ ids
    xtra_ids = [
        'FQ-176899', 
        'FQ-176898',
        'FQ-154113',
        'FQ-154106',
        'FQ-176895',
        'FQ-176896',
        'FQ-176897',
        'FQ-161805']

    nondvs_fr.drop(xtra_ids, axis=1, inplace=True)

    # rename columns
    nondvs_fr.rename(columns={
        'FQ-158674':'Title', 
        'FQ-153995':'I am 17 years and older',
        'FQ-153963':'I confirm that I have read and understood the Program Guidelines',
        'FQ-153971':'First Name',
        'FQ-154040':'Evidence of Employment',
        'FQ-154050':'Bank Account Name',
        'FQ-153970':'Last Name',
        'FQ-154051':'BSB',
        'FQ-153974':'Date of Birth',
        'FQ-176900':'Proof of Identity', 
        'FQ-153994':'I have been tested for COVID-19 or I am required to care for someone who is required to self-isolate',
        'FQ-154052':'Account No',
        'FQ-153986':'I currently live in Victoria',
        'FQ-154054':'BSB number and Account number are correct',
        'FQ-153998':'I am unable to work as a result of the requirement to self-isolate',
        'FQ-154055':'Has anyone else used these bank details when applying for payment under this program?',
        'FQ-153999':'I am not receiving any income, earnings or salary maintenance from work as a result of self-isolation at home',
        'FQ-154016':'Full name of person completing this application', 
        'FQ-154049':'Are you self employed?',
        'FQ-153972':'Mobile',
        'FQ-154000':'I have exhausted sick leave and carers leave entitlements',
        'FQ-154032':'ABN of business you are employed at',
        'FQ-154008':'I am not receiving income support (JobKeeper or JobSeeker)',
        'FQ-154009':'I am not receiving another worker support payment, including the PLDP',
        'FQ-154024':'Name of the business that you are primarily employed at',
        'FQ-153973':'Email',
        'FQ-154027':'Employer Street Address',
        'FQ-154007':'I agree that I may have to repay the funds if I do not self-isolate while waiting for my test results',
        'FQ-154028':'Employer Suburb/Town', 
        'FQ-153967':'Residential Street Address',
        'FQ-154029':'Employer Postcode',
        'FQ-153968':'Residential Suburb/Town',
        'FQ-154033':'Primary occupation industry',
        'FQ-153969':'Residential Postcode',
        'FQ-154034':'Other (please specify)',
        'FQ-154030':'Name of the person who can verify your employment details',
        'FQ-154031':'Contact Number of the person above',
        'FQ-153975':'Are you an Australian Citizen or Permanent Resident?',
        'FQ-154114':'Proof of Identity (select from list)',
        'FQ-176850':'Victorian Drivers Licence',
        'FQ-176888':'Centrelink Customer Reference Num',
        'FQ-176871':'Medicare Card',
        'FQ-176857':'Australian Passport',
        'FQ-176867':'Foreign Passport Number',
        'FQ-153981':'Visa Type',
        'FQ-176863':'Visa Grant Number',
        'FQ-176868':'Nationality on passport',
        'FQ-154048':'Letter requesting COVID test',
        'FQ-154047':'Were you directed by the Department of Health or your employer to get tested?',
        'FQ-154041':'Were you tested for COVID-19?',
        'FQ-154043':'First Name (person tested)',
        'FQ-154044':'Surname (person tested)',
        'FQ-154045':'Date of Birth (person tested)',
        'FQ-154023':'Date of COVID-19 test',
        'FQ-154039':'How many hours of paid work have you missed due to isolating?',
        'FQ-154100':'Victorian Proof of Age Card',
        'FQ-154107':'Victorian Seniors Card'}, 
        inplace=True)

    return nondvs_fr


def LOGIN_IP_format(df):
    df.drop(['attributes.type', 'attributes.url'], axis=1, inplace=True)

    # Change 'EventDate' to datetime format
    df['EventDate'] = pd.to_datetime(df['EventDate'])
    df['EventDate'] = pd.to_datetime(df['EventDate'], unit='ms').dt.tz_convert(None) + pd.Timedelta(hours=10)

    df.rename(columns={'Username': 'User Username',
                            'SourceIp': 'Source IP',
                            'EventDate': 'Login Time'}, inplace=True)

    df = df[df['Status'] == 'Success'].copy()
    df.drop('Status', axis=1, inplace=True)

    return df


def OPPS_format(df):
    df.drop([
     'attributes.type', 'attributes.url', 'Form_Response__r.attributes.type',
     'Form_Response__r.attributes.url', 'Form_Response__r.Application__r.attributes.type',
     'Form_Response__r.Application__r.attributes.url', 'Form_Response__r.Opportunity__r.attributes.type',
     'Form_Response__r.Opportunity__r.attributes.url'], axis=1, inplace=True)

    df.rename(columns={'Form_Response__r.Name': 'Form Response Name'}, inplace=True)

    return df


def new_form_responses():
    temp_df = pd.merge(results[['Form Response Name', 'Submitted', 'User Username']], loginIP, how='left', on='User Username')
    temp_df['Time Diff'] = temp_df['Submitted'] - temp_df['Login Time']
    temp_df = temp_df[temp_df['Time Diff'] >= pd.Timedelta(0, 'm')].copy()
    temp_df = temp_df[temp_df['Time Diff'] < pd.Timedelta(12, 'h')].copy()

    min_vals = temp_df[['Form Response Name', 'Time Diff']].groupby('Form Response Name').min()
    min_vals.rename(columns={'Time Diff': 'Duration'}, inplace=True)

    temp_df = temp_df.merge(min_vals, how='left', left_on='Form Response Name', right_index=True)
    temp_df['Keep'] = temp_df['Time Diff'] == temp_df['Duration']
    temp_df = temp_df[temp_df['Keep'] == True].copy()

    temp_df.drop(['Submitted', 'User Username', 'Time Diff', 'Keep'], axis=1, inplace=True)
    results = pd.merge(results, temp_df, how='left', on='Form Response Name')
    print('New form responses: {0}'.format(len(results)))

    return results


def validation(master_list, results, frank):

    dict_freq = {'Count Username': 'User Username',
                'Count IP': 'Source IP',
                'Count Licence': 'Victorian Drivers Licence', 
                'Count Mobile': 'Mobile', 
                'Count Email': 'Email',
                'Count Bank': 'BSB and Account',
                'Count Medicare': 'Medicare Card',
                'Count Aus Passport': 'Australian Passport',
                'Count Visa': 'Visa Grant Number',
                'Count Foreign Passport': 'Foreign Passport Number'}

    print('Updating value counts for all applications\n')

    # Add frequency counts - all applications
    for key, value in tqdm(dict_freq.items()):
        item_freq = master_list['{0}'.format(value)].value_counts().to_dict()
        results['{0}'.format(key)] = results['{0}'.format(value)].map(item_freq)
        master_list['{0}'.format(key)] = master_list['{0}'.format(value)].map(item_freq)

    # Add frequency counts for new applications only
    dict_new = {
        'U COUNT': 'User Username',
        'IP COUNT': 'Source IP',
        'M COUNT': 'Mobile', 
        'E COUNT': 'Email',
        'B COUNT': 'BSB and Account'}

    print('Finding value counts for new applications\n')
    for key, value in tqdm(dict_new.items()):
        item_freq = results['{0}'.format(value)].value_counts().to_dict()
        results['{0}'.format(key)] = results['{0}'.format(value)].map(item_freq)

    # Add frank lookup on key ID fields
    frank_lookup = {
        'User Username': ['U SP', 'U REF', 'U ASSESS'],
        'Mobile': ['M SP', 'M REF', 'M ASSESS'],
        'Email': ['E SP', 'E REF', 'E ASSESS'],
        'BSB and Account': ['B SP', 'B REF', 'B ASSESS']}

    for key, value in tqdm(frank_lookup.items()):
        results = func.match_frank(results, frank, key, value)

    if 'ID File Size (Bytes)' in results.columns:
        pass
    else:
        results['ID File Size (Bytes)'] = ''

    results['ID-EE'] = results['ID File Size (Bytes)'] == results['Employment Evidence File Size (Bytes)']
    results['EE-CL'] = results['Employment Evidence File Size (Bytes)'] == results['Letter Requesting Test File Size (Bytes)']

    return results


def check_dups(results, master_list, col1, col2, fuz1):
    dups = func.get_potential_dupes(results, master_list, col1, col2)
    with_fuzzy_dupes = func.fuzzy_dupes(dups, fuz1)
    return with_fuzzy_dupes


def dup_check_and_save(valid_results):
    bank_dups = check_dups(valid_results, master_list, 'BSB and Account', 'Count Bank', 'BSB and Account')
    bank_dups[['Form Response Name', 'REF', 'Residential Street Address', 'Residential Suburb/Town', 'Title', 'Last Name', 'First Name', 'Bank Account Name', 'BSB and Account', 'Count Bank', 'Match Score', 'Avg Score', 'New FR']].to_csv(bank_dups_csv, index=False)

    user_dupes = check_dups(valid_results, valid_results, 'User Username', 'U COUNT', 'User Username')
    user_dupes[['Form Response Name', 'REF', 'Residential Street Address', 'Residential Suburb/Town', 'Title', 'Last Name', 'First Name', 'User Username', 'U COUNT', 'U REF', 'M REF', 'E REF', 'B REF', 'Match Score', 'Avg Score']].to_csv(user_dupes_csv, index=False)

    ip_dups = check_dups(valid_results, valid_results, 'Source IP', 'IP COUNT')
    ip_dups[['Form Response Name', 'REF', 'Residential Street Address', 'Residential Suburb/Town', 'Title', 'Last Name', 'First Name', 'Source IP', 'Browser', 'Platform', 'Country', 'Subdivision', 'City', 'IP COUNT', 'U REF', 'M REF', 'E REF', 'B REF', 'Match Score', 'Avg Score']].to_csv(ip_dups_csv, index=False)

    mobile_dupes = check_dups(valid_results, valid_results, 'Mobile', 'M COUNT', 'Mobile')
    mobile_dupes[['Form Response Name', 'REF', 'Residential Street Address', 'Residential Suburb/Town', 'Title', 'Last Name', 'First Name', 'Mobile', 'M COUNT', 'U REF', 'M REF', 'E REF', 'B REF', 'Match Score', 'Avg Score']].to_csv(mobile_dupes_csv, index=False)

    email_dupes = check_dups(valid_results, valid_results, 'Email', 'E COUNT', 'Email')
    email_dupes[['Form Response Name', 'REF', 'Residential Street Address', 'Residential Suburb/Town', 'Title', 'Last Name', 'First Name', 'Email', 'E COUNT', 'U REF', 'M REF', 'E REF', 'B REF', 'Match Score', 'Avg Score']].to_csv(email_dupes_csv, index=False)


def print_value_counts(attachments):
    attachments[['name', 'extension']] = attachments['name'].str.rsplit('.', n=1, expand=True)
    attachments['extension'] = attachments['extension'].str.lower()
    print('\nFile extension counts:')
    print(attachments['extension'].value_counts())
    return attachments


def extension_validation(attachments):
        invalid_extensions = ['mp4', 'mov', 'exe', 'mp3', 'm4a', 'wav', 'tdck', 'td', 'svg', 'tmp']
        invalid_attachments = attachments[attachments['extension'].isin(invalid_extensions)][['Form Response Name', 'Form Question Name', 'extension']].copy()
        return invalid_attachments 


## Function needs to be refactored - too many moving parts!
def add_results_columns(valid_results):
    valid_results['output'] = valid_results.apply(lambda r: func.add_comments(r), axis=1)
    valid_results[['SP', 'REF', 'ISU Comments']] = pd.DataFrame(valid_results['output'].tolist(), index=valid_results.index)
    return valid_results


def cancellations_file(valid_results):
    cancellations = valid_results[valid_results['ISU Comments'].notna()].copy()
    cancellations['Risk Mitigation'] = 'PINK456'
    return cancellations
    

def format_responses(cancellations):
    frs = cancellations[['Form Response Name', 'REF']].copy()
    frs['formatted'] = "'" + frs['Form Response Name'] + "'"
    return frs


def download_attachments(cancellations, attachments):
    for_download = pd.merge(cancellations[['REF', 'Form Response Name']], attachments, how='left', left_on='Form Response Name', right_on='Form Response Name')



    # session = requests.Session() # not used

    rest_api = RestApi(username=creds['login']['username'],
                    password=creds['login']['password'],
                    security_token=creds['login']['token'])

    func.get_attachments(date, for_download, rest_api)


def output_ISU_cancel(cancellations, oppcodes, date):
    cancellations = cancellations[['REF', 'Form Response Name', 'Risk Mitigation', 'ISU Comments']].copy()
    cancellations = cancellations.merge(oppcodes, how='left', on='Form Response Name')
    cancellations.to_excel('../ISU_{0} {1}b - Request to cancel opps.xlsx'.format(date[:-3], date[-3:]), index=False)


def save_results(results): 
    save_path = os.path.join('..', 'Current', date, '_Forms.csv')
    
    results.to_csv(save_path.format(date), index=False)
    print("Updated form response csv files exported (◕‿◕✿)\n")


if __name__ =='__main__':
    # used in sql queries
    date_string = input('Enter the date to retrieve records from formatted as YYYY-MM-DD e.g. 2021-04-17: ')

    # creates directory named by date and file name
    # use a different variable name 'date' is used by datetime library
    date = str(input('Enter date formatted as ddmmm (e.g. "12Jan"):    ')).strip() 
    if not os.path.exists(date):
        os.mkdir(date)

    master_list = read_master_list()

    frank = load_frankbook()
    
    # Current (original) TIPP form (F-3315)
    tipp1_fr_df = TIPP1_FR_data(sf, date_string) # all records submitted from the date entered to present
    if len(tipp1_fr_df) > 0:
        old_tipp_df = TIPP1_FR_format(tipp1_fr_df)

    # New DVS form (F-3464)
    tippdvs_fr_df = TIPPDVS_FR_data(date_string) # all records submitted from the date entered to present
    attachments1 = TIPPDVS_FR_attachments(tippdvs_fr_df)

    tipp_dvs_fr = TIPPDVS_FR_format(tippdvs_fr_df, attachments1)


    # New non-DVS form (F-3493)
    tippnondvs_fr_df = TIPPnonDVS_FR_data(date_string)
    if len(tippnondvs_fr_df) > 0:
        attachments2 = TIPPnonDVS_FR_attachments(tippnondvs_fr_df)
        nondvs_fr = TIPPnonDVS_FR_format(tippnondvs_fr_df)

        results = tipp_dvs_fr.append(nondvs_fr, ignore_index=True)
        attachments = attachments1.append(attachments2, ignore_index=True)

    else: 
        results = tipp_dvs_fr.copy()
        attachments = attachments1.copy()

    results_clean = func.clean_data(results)
    
    # Check which FR IDs already appear in the master list 
    # (submitted within last 5 days)
    results_clean = exclude_subset(master_list, results_clean)

    # Query IP/login data
    loginIP_df = LOGIN_IP_data(date_string)
    loginIP = LOGIN_IP_format(loginIP_df)

    results_clean = new_form_responses(results_clean, loginIP) 

    # Append results to master list
    master_list = master_list.append(results_clean, ignore_index=True)

    # result validation check
    valid_results = validation(master_list, results_clean, frank)

    # check for duplicate values and save results
    dup_check_and_save(valid_results)

    # save master list
    save_master_list(master_list)


    # After review, read files back in and include comments in output file
    print('\nPotential duplicate form responses exported to csv files - please review and remove unwanted groups')
    reply = str(input('Press y to continue when duplicates have been reviewed:    ')).lower().strip()
    if reply[0] == 'y':
        bank_dupes = pd.read_csv(bank_dups_csv, dtype=str)
        ip_dupes = pd.read_csv(ip_dups_csv, dtype=str)
        user_dupes = pd.read_csv(user_dupes_csv, dtype=str)
        mobile_dupes = pd.read_csv(mobile_dupes_csv, dtype=str)
        email_dupes = pd.read_csv(email_dupes_csv, dtype=str)

        # Create list of FRs for each dupe category
        bank_list = list(bank_dupes['Form Response Name'][bank_dupes['New FR'] == 'TRUE'])
        user_list = list(user_dupes['Form Response Name'])
        ip_list = list(ip_dupes['Form Response Name'])
        mobile_list = list(mobile_dupes['Form Response Name'])
        email_list = list(email_dupes['Form Response Name'])

        ### lists not used? 

    attachments = print_value_counts(attachments)

    # Updating to checking for specific valid extensions listed on application form
    invalid_attachments = extension_validation(attachments)
    invalid_attch_list = list(invalid_attachments['Form Response Name'])

    # add columns?
    add_results_columns(valid_results)

    # Reorder columns
    valid_results = func.order_df(valid_results)

    # Create cancellations file
    cancellations = cancellations_file(valid_results)

    # ??
    attachments.drop(['filesize'], axis=1, inplace=True)

    # format responses
    frs = format_responses(cancellations)

    fr_list = list(frs['formatted'])
    fr_list = "(" + ', '.join(fr_list) + ")"


    oppcodes_df = OPPS_data(fr_list)

    oppcodes = OPPS_format(oppcodes_df)

    # Download attachments
    download_attachments(cancellations, attachments)

    # Output ISU cancellation file
    output_ISU_cancel(cancellations, oppcodes, date)

    # Save results to csv
    save_results(valid_results)

    print('All done! (o゜▽゜)o☆\n')