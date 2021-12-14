import pandas as pd
from simple_salesforce import Salesforce
import json
from tqdm import tqdm
import re
import os
from SalesforceXytoolsCore import RestApi
import requests
from fuzzywuzzy import fuzz
from statistics import mean


# Define functions
def remove_chars(s):
    return re.sub('[^0-9]+','', str(s))

def clean_data(df):
    df = df.replace(to_replace="\r", value="", regex=True) 
    df = df.replace(to_replace="\n", value="", regex=True)
    df['Email'] = df['Email'].str.lower()
    df['Mobile'] = df.loc[df['Mobile'].notnull()].apply(lambda x: remove_chars(x['Mobile']), axis=1).str[:11]
    df['Victorian Drivers Licence'] = df.loc[df['Victorian Drivers Licence'].notnull()].apply(lambda x: remove_chars(x['Victorian Drivers Licence']), axis=1)
    df['Visa Grant Number'] = df.loc[df['Visa Grant Number'].notnull()].apply(lambda x: remove_chars(x['Visa Grant Number']), axis=1)
    df['Medicare Card'] = df.loc[df['Medicare Card'].notnull()].apply(lambda x: remove_chars(x['Medicare Card']), axis=1)
    df['Contact Number of the person above'] = df.loc[df['Contact Number of the person above'].notnull()].apply(lambda x: remove_chars(x['Contact Number of the person above']), axis=1).str[:11]
    df['Account No'] = df['Account No'].apply(remove_chars)
    df['BSB'] = df['BSB'].apply(remove_chars)
    for index, row in df.iterrows():
        df.loc[index,'BSB'] = '{0}-{1}'.format(row['BSB'][:3],row['BSB'][3:])
    df['BSB and Account'] = df['BSB'] + ' ' + df['Account No'].astype(str)
    return df

def pivot_frs(df0, attch):
    """Clean form response data returned by SOQL queries and return pivot"""

    # Drop attribute and url columns from query results
    drop_cols = [
        'Attachments',
        'attributes.type',
        'attributes.url',
        'Form_Response_Section__r.attributes.type',
        'Form_Response_Section__r.attributes.url',
        'Form_Response_Section__r.Form_Response__r.attributes.type',
        'Form_Response_Section__r.Form_Response__r.attributes.url',
        'Form_Response_Section__r.Form_Response__r.User__r.attributes.type',
        'Form_Response_Section__r.Form_Response__r.User__r.attributes.url',
        'Form_Question__r.attributes.type',
        'Form_Question__r.attributes.url',
        'Attachments.totalSize',
        'Attachments.done',
        'Attachments.records']
    df0.drop(drop_cols,axis=1,inplace=True)

    # Rename columns
    df0.rename(columns={
        'Form_Response_Section__r.Form_Response__r.Name':'Form Response Name', 
        'Answer__c':'Answer', 
        'Form_Response_Section__r.Form_Response__r.Submitted__c':'Submitted', 
        'Form_Response_Section__r.Form_Response__r.User__r.Username':'User Username', 
        'Form_Question__r.Name':'Form Question Name'}, inplace=True)

    # Change 'submitted' to datetime format and sort by date
    df0['Submitted'] = pd.to_datetime(df0['Submitted'])
    df0['Submitted'] = pd.to_datetime(df0['Submitted'], unit='ms').dt.tz_convert(None) + pd.Timedelta(hours=10)
    df0 = df0.sort_values(by='Submitted',ascending=True)

    # Check whether duplicate rows exist
    checkpoint = df0.duplicated(subset=['Form Response Name', 'Form Question Name']).any()
    all_rows = df0['Form Response Name'].count()

    # If TRUE remove completely duplicated rows
    # Multiple rows in GEMS extract with same FR-xxxxxx and FQ-xxxxxx (same values for 'Answer' and 'Submitted' causing issues creating pivot)
    if checkpoint == True:
        df0.drop_duplicates(subset=['Form Response Name', 'Answer', 'Form Question Name'], keep='last', inplace=True)
        total_rows = df0['Form Response Name'].count()
        print('After removing {0} duplicate rows, there are now {1} rows of data'.format((all_rows-total_rows),total_rows))
    else:
        print('No duplicates found')
        
    # Checking whether duplicate rows still exist - different answer value
    checkpoint = df0.duplicated(subset=['Form Response Name', 'Form Question Name']).any()
    if checkpoint == True:
        dups = df0[df0.duplicated(subset=['Form Response Name', 'Form Question Name'], keep=False)].copy()
        df0.drop_duplicates(subset=['Form Response Name', 'Form Question Name'], keep=False, inplace=True)
        dups.to_csv('Dups.csv')
        print('Duplicate rows exported to Dups.csv - please remove unwanted duplicate rows')
        reply = str(input('Press y to continue when duplicates have been removed:    ')).lower().strip()
        if reply[0] == 'y':
            dups = pd.read_csv('Dups.csv')
            df0 = df0.append(dups)
            del dups
            df0['Submitted'] = pd.to_datetime(df0['Submitted'])

    # Create new dataframe with pivot based on 'Form Response Name'
    df = df0.pivot(index='Form Response Name', columns='Form Question Name', values='Answer').reset_index()
    dfdates = df0[['Form Response Name','Submitted', 'User Username']].copy()
    dfdates.drop_duplicates(subset=['Form Response Name', 'Submitted'], keep='last', inplace=True)

    # Merge dataframes and return
    results = pd.merge(df, dfdates, how='left', left_on='Form Response Name', right_on='Form Response Name')
    results = pd.merge(results, attch, how='left', left_on='Form Response Name', right_on='Form Response Name')
    return results

def match_frank(df, frank, field_name, col_names):
    field_refs = list(df[field_name])
    matches = frank[frank[field_name].isin(field_refs)].copy()
    matches.drop_duplicates(subset=[field_name], keep='last', inplace=True)
    matches.rename(columns={'Updated Folder/Group Name': col_names[0], 
                            'Group (old ref)': col_names[1],
                            'Category': col_names[2]}, inplace=True)
    df = df.merge(matches[[field_name, col_names[0], col_names[1], col_names[2]]], how='left', left_on=field_name, right_on=field_name)
    return df

def isnull(val):
    return val != val

def get_potential_dupes(new_apps, all_apps, field_name, field_count):
    new_dupes = new_apps[new_apps[field_count] > 1]
    new_frs = list(new_dupes['Form Response Name'])
    field_vals = list(new_dupes[field_name])    
    matches = all_apps[all_apps[field_name].isin(field_vals)].copy()
    matches['New FR'] = matches['Form Response Name'].isin(new_frs)
    return matches

def fuzzy_dupes(dupes, field):
    dupe_list = list(dupes['{0}'.format(field)].unique())
    dupes['Temp Name'] = dupes['First Name'] + ' ' + dupes['Last Name']
    results = pd.DataFrame(columns=['{0}'.format(field), 'Match Score', 'Avg Score'])
    for item in dupe_list:
        fullname_list = list(dupes[dupes['{0}'.format(field)] == item]['Temp Name'])
        surname_list = list(dupes[dupes['{0}'.format(field)] == item]['Last Name'])
        address_list = list(dupes[dupes['{0}'.format(field)] == item]['Residential Street Address'])
        simple_ratio = []
        j = len(surname_list)-1
        for i in range(j):
            name_score = fuzz.token_set_ratio(str(fullname_list[j]).lower(), str(fullname_list[i]).lower())
            surname_score = fuzz.token_set_ratio(str(surname_list[j]).lower(), str(surname_list[i]).lower())
            address_score = fuzz.token_set_ratio(str(address_list[j]).lower(), str(address_list[i]).lower())
            # overall match score should be the max of surname and address
            simple_score = max(name_score, surname_score, address_score)
            simple_ratio.append(simple_score)
        min_simple_ratio = min(simple_ratio)
        avg_simple_ratio = mean(simple_ratio)
        results = results.append({'{}'.format(field): item,
                                  'Match Score': min_simple_ratio,
                                  'Avg Score': avg_simple_ratio}, ignore_index=True)
    results = pd.merge(dupes, results, how='left', on='{}'.format(field))
    field_dupes = results[(results['Match Score'] < 50) & (results['Avg Score'] < 92)].copy()
    field_dupes['REF'] = ''
    return field_dupes

def add_comments(row):
    invalid_employer = ['na', 'n/a', 'none', 'unemployed', 'centrelink', 'centerlink', 'not working',
                        'dont work', 'student', 'i am studying', 'dole', 'none atm', 'jobseeker', 
                        'unemployed not working', 'unemployed no job', 'job seeker', 'looking for work',
                        'unemployed no work', 'home duties', 'no job', 'n.a', 'n a', '', 'not employed']
    
    # list of employers commonly used for groups with falsified payslips
    employers_3035 = ['the boat house', 'boat house', 'the bosthouse', 'the boathouse', 'bost house', 'your crew', 'your crew labour hire', 'mj harris group', 'chard enterprise', 'chard enterprise pty ltd', 's & n contracting pty ltd', 's & n contracting', 's & n contracting ptyltd', 's&n contracting ptyltd', 'moulamein grain services pty ltd', 'j atkinson', 'greenhams pty ltd', 'saputo dairy australia', 'mackin labour', 'mandeville meat group pty ltd', 'mackin labour j atkinson', 'tsogbilguun battseren', 'chard enterprises']
    employers_4426 = ['proquest', 'true care', 'staff australia', 'chandler recruitment', 'Ampol retail private limited']
    employers_4415 = ['Westgate constructions', 'Basilandco', "Campbell's constructions", 
                      'Campbells constructions', 'Bunting labour services']
    employers_5001 = ['tkz jackhammering', 'cbuilt', 'c built']
    employers_6757 = ['whiteside autos', 'whiteside auto', 'white side auto', 'white side autos']
    employers_5947 = ['patel marketing group', 'movig pty ltd', 'stk solutions', 'stk solutions pty ltd']
    employers_12037 = ['jm landscape', 'jm scaffolding']
    comments = ['ISU']
    sp = ''
    ref = ''
    
    global bank_dupes, bank_list, user_dupes, user_list, ip_dupes, ip_list, mobile_dupes, mobile_list, email_dupes, email_list, invalid_attachments, invalid_attch_list

    if isnull(row['B REF']) == False:
        ref = row['B REF']
        comments.append('overlaps with group flagged for ISU review')
        if isnull(row['B SP']) == False:
            sp = row['B SP']
    elif isnull(row['U REF']) == False:
        ref = row['U REF']
        comments.append('overlaps with group flagged for ISU review')
        if isnull(row['U SP']) == False:
            sp = row['U SP']
    elif isnull(row['E REF']) == False:
        ref = row['E REF']
        comments.append('overlaps with group flagged for ISU review')
        if isnull(row['E SP']) == False:
            sp = row['E SP']
    elif isnull(row['M REF']) == False:
        ref = row['M REF']
        comments.append('overlaps with group flagged for ISU review (mobile overlap only)')
        if isnull(row['M SP']) == False:
            sp = row['M SP']
    else:
        ref = None
        sp = None

    if row['Form Response Name'] in bank_list:
        comments.append('same bank account provided for separate individuals')
        if ref == None:
            ref = bank_dupes['REF'][bank_dupes['Form Response Name'] == row['Form Response Name']].values[0]
    
    if row['Form Response Name'] in ip_list:
        comments.append('applications for separate individuals lodged from same IP address')
        if ref == None:
            ref = ip_dupes['REF'][ip_dupes['Form Response Name'] == row['Form Response Name']].values[0]

    if row['Form Response Name'] in user_list:
        comments.append('same user account lodging applications for separate individuals')
        if ref == None:
            ref = user_dupes['REF'][user_dupes['Form Response Name'] == row['Form Response Name']].values[0]
    
    if row['Form Response Name'] in email_list:
        comments.append('same email provided for separate individuals')
        if ref == None:
            ref = email_dupes['REF'][email_dupes['Form Response Name'] == row['Form Response Name']].values[0]
    
    if row['Form Response Name'] in mobile_list:
        comments.append('same mobile number provided for separate individuals')
        if ref == None:
            ref = mobile_dupes['REF'][mobile_dupes['Form Response Name'] == row['Form Response Name']].values[0]

    if row['ID-EE'] == True:
        comments.append('ID and EE are the same file')

    if row['EE-CL'] == True:
        comments.append('EE and COVID letter are the same file')

    if any(BSB_risk in row['BSB'] for BSB_risk in ['670-864', '082-991', '939-200']):
        comments.append('high risk BSB ({0})'.format(row['BSB']))

    # removing due to large number of false positives/low rate of integrity issues with individual applications based on this flag alone
    #if row['Country'] != 'Australia':
    #    comments.append('IP address outside of Aus')

    if row['Form Response Name'] in invalid_attch_list:
        comments.append('invalid file extension: {0} is "{1}"'.format(invalid_attachments['Form Question Name'][invalid_attachments['Form Response Name'] == row['Form Response Name']].values[0], 
                                                                      invalid_attachments['extension'][invalid_attachments['Form Response Name'] == row['Form Response Name']].values[0]))

    if isinstance(row['Name of the business that you are primarily employed at'], str):
        if row['Name of the business that you are primarily employed at'].lower() in invalid_employer:
            comments.append('invalid employer details: "{0}"'.format(row['Name of the business that you are primarily employed at']))

    if isinstance(row['Name of the business that you are primarily employed at'], str):
        if 'china bar' in row['Name of the business that you are primarily employed at'].lower():
            comments.append('China Bar payslip')
            ref = '1118.1'
            sp = 'COVID TIPP > #065.2'

    if isinstance(row['Name of the business that you are primarily employed at'], str):
        if any(employer in row['Name of the business that you are primarily employed at'] for employer in employers_4426):
            comments.append('check payslip')
            ref = '4426'

    if isinstance(row['Name of the business that you are primarily employed at'], str):
        if any(employer in row['Name of the business that you are primarily employed at'].lower() for employer in employers_5001):
            comments.append('check payslip')
            ref = '5001'

    if isinstance(row['Name of the business that you are primarily employed at'], str):
        if any(employer in row['Name of the business that you are primarily employed at'].lower() for employer in employers_5947):
            comments.append('check payslip')
            ref = '5947'

    if isinstance(row['Name of the business that you are primarily employed at'], str):
        if any(employer in row['Name of the business that you are primarily employed at'].lower() for employer in employers_6757):
            comments.append('check payslip')
            ref = '6757'

    if isinstance(row['Name of the business that you are primarily employed at'], str):
        if any(employer in row['Name of the business that you are primarily employed at'].lower() for employer in employers_3035):
            comments.append('check payslip')
            ref = '3035'

    if isinstance(row['Name of the business that you are primarily employed at'], str):
        if any(employer in row['Name of the business that you are primarily employed at'] for employer in employers_4415):
            comments.append('check payslip')
            ref = '4415'
            sp = 'COVID TIPP > #101'

    if isinstance(row['Name of the business that you are primarily employed at'], str):
        if any(employer in row['Name of the business that you are primarily employed at'].lower() for employer in employers_12037):
            comments.append('check payslip')
            ref = '12037'

    if isinstance(row['Employment Evidence File Name'], str):
        if 'statutory' in row['Employment Evidence File Name'].lower() and '.doc' in row['Employment Evidence File Name'].lower():
            if row['Are you self employed?'] == 'No':
                comments.append('Word doc statutory declaration')
    
    if isinstance(row['Employment Evidence File Name'], str):
        if 'Rental Relief Grant' in row['Employment Evidence File Name']:
            comments.append('COVID rent relief guidelines attached as EE')

    if isinstance(row['Employment Evidence File Name'], str):
        if 'Test-Isolation-Payment-Guidelines' in row['Employment Evidence File Name']:
            comments.append('TIPP guidelines attached as EE')

    if isinstance(row['Letter Requesting Test File Name'], str):
        if 'Screenshot_2020-10-13-10-53-43-02.jpg' in row['Letter Requesting Test File Name']:
            comments.append('same screenshot provided for multiple unrelated applicants')
            ref = '536'

    if len(comments) == 1:
        comments = None
        return sp, ref, comments
        
    else:
        comment_string = ' - '.join(comments)
        return sp, ref, comment_string
    
def order_df(df):
    col_list = ['SP', 'REF', 'ISU Comments', 'Form Response Name', 'Submitted',
            'User Username', 'U COUNT', 'U SP', 'U REF', 'U ASSESS', 'Count Username',
            'Login Time', 'Source IP', 'IP COUNT', 'Count IP', 'Browser', 'Platform', 
            'Country', 'Subdivision', 'City', 'PostalCode', 'Duration',
            'Victorian Drivers Licence', 'Count Licence', 'Residential Street Address',
            'Residential Suburb/Town', 'Residential Postcode', 'Title', 'Last Name',
            'First Name', 'Mobile', 'M COUNT', 'M SP', 'M REF', 'M ASSESS',
            'Count Mobile', 'Email', 'E COUNT', 'E SP', 'E REF', 'E ASSESS', 'Count Email',
            'Bank Account Name', 'BSB', 'Account No', 'BSB and Account', 'B COUNT', 'B SP', 
            'B REF', 'B ASSESS', 'Count Bank', 'Victorian Seniors Card', 'Victorian Proof of Age Card',
            'Proof of Identity (select from list)', 'I currently live in Victoria',
            'I am unable to work as a result of the requirement to self-isolate',
            'I have been tested for COVID-19 or I am required to care for someone who is required to self-isolate',
            'I am 17 years and older', 'Proof of Identity', 'Evidence of Employment', 'Additional Attachment',
            'Full name of person completing this application',
            'Date of COVID-19 test', 'Name of the business that you are primarily employed at',
            'I confirm that I have read and understood the Program Guidelines',
            'I am not receiving any income, earnings or salary maintenance from work as a result of self-isolation at home',
            'Date of Birth', 'Medicare Card', 'Count Medicare',
            'Australian Passport', 'Count Aus Passport', 'Visa Type', 'Visa Grant Number', 'Count Visa',
            'I have exhausted sick leave and carers leave entitlements',
            'I am not receiving income support (JobKeeper or JobSeeker)',
            'Employer Street Address', 'Employer Suburb/Town', 'Employer Postcode',
            'Name of the person who can verify your employment details',
            'Contact Number of the person above', 'BSB number and Account number are correct',
            'Are you self employed?', 'ABN of business you are employed at',
            'Are you an Australian Citizen or Permanent Resident?',
            'Foreign Passport Number', 'Count Foreign Passport', 'Nationality on passport',
            'Has anyone else used these bank details when applying for payment under this program?',
            'I agree that I may have to repay the funds if I do not self-isolate while waiting for my test results',
            'I am completing this application as a carer/legal guardian',
            'Primary occupation industry', 'Other (please specify)', 'Letter requesting COVID test',
            'Were you directed by the Department of Health or your employer to get tested?',
            'ID File Name', 'ID File Size (Bytes)', 
            'Employment Evidence File Name', 'Employment Evidence File Size (Bytes)', 
            'Additional Attachment File Name', 'Additional Attachment File Size (Bytes)', 
            'Letter Requesting Test File Name', 'Letter Requesting Test File Size (Bytes)',
            'ID-EE', 'EE-CL']

    for col in col_list:
        if col in df.columns:
            pass
        else:
            df[col] = ''

    df = df[col_list]

    return df

def get_attachments(date, dframe, rest_api):
    SAVE_DIR = date
    print("\nDownloading attachments now! (ง'̀-'́)ง \n")

    for index, row in tqdm(dframe.iterrows(), total=len(dframe)):
        if isnull(row['REF']) == False:
            file_name = '{0} - {1} - {2}'.format(row['REF'], row['Form Response Name'], row['Form Question Name'])
        else:
            file_name = '{0} - {1}'.format(row['Form Response Name'], row['Form Question Name'])

        sobject = '{0}/Body'.format(row['url'])

        request = rest_api.call_rest(method='GET',
                                    path=sobject,
                                    params={})

        try:
            with open(os.path.join(SAVE_DIR, file_name + '.{0}'.format(row['extension'])), mode='wb') as f:
                f.write(request.content)
        except:
            with open(os.path.join(SAVE_DIR, file_name + '.txt'), mode='wb') as f:
                f.write(request.content)
    
    print('\nAttachment download complete\n')


