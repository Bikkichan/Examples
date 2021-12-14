import pandas as pd

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


def TIPP1_FR_format(attachments):
    
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

    return attach_pivot

def pivot_frs(df, attch):
    # Clean form response data returned by SOQL queries and return pivot
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

    df.drop(drop_cols,axis=1,inplace=True)

    # Rename columns
    df.rename(columns={
        'Form_Response_Section__r.Form_Response__r.Name': 'Form Response Name', 
        'Answer__c': 'Answer', 
        'Form_Response_Section__r.Form_Response__r.Submitted__c': 'Submitted', 
        'Form_Response_Section__r.Form_Response__r.User__r.Username': 'User Username', 
        'Form_Question__r.Name': 'Form Question Name'}, inplace=True)

    # Change 'submitted' to datetime format and sort by date
    df['Submitted'] = pd.to_datetime(df['Submitted'])
    df['Submitted'] = pd.to_datetime(df['Submitted'], unit='ms').dt.tz_convert(None) + pd.Timedelta(hours=10)
    df = df.sort_values(by='Submitted',ascending=True)

    # Check whether duplicate rows exist
    checkpoint = df.duplicated(subset=['Form Response Name', 'Form Question Name']).any()
    all_rows = df['Form Response Name'].count()

    # If TRUE remove completely duplicated rows
    # Multiple rows in GEMS extract with same FR-xxxxxx and FQ-xxxxxx (same values for 'Answer' and 'Submitted' causing issues creating pivot)
    if checkpoint == True:
        df.drop_duplicates(subset=['Form Response Name', 'Answer', 'Form Question Name'], keep='last', inplace=True)
        total_rows = df['Form Response Name'].count()
        print('After removing {0} duplicate rows, there are now {1} rows of data'.format((all_rows-total_rows),total_rows))
    else:
        print('No duplicates found')
        
    # Checking whether duplicate rows still exist - different answer value
    checkpoint = df.duplicated(subset=['Form Response Name', 'Form Question Name']).any()
    if checkpoint == True:
        dups = df[df.duplicated(subset=['Form Response Name', 'Form Question Name'], keep=False)].copy()
        df.drop_duplicates(subset=['Form Response Name', 'Form Question Name'], keep=False, inplace=True)
        dups.to_csv('Dups.csv')
        print('Duplicate rows exported to Dups.csv - please remove unwanted duplicate rows')
        reply = str(input('Press y to continue when duplicates have been removed:    ')).lower().strip()
        if reply[0] == 'y':
            dups = pd.read_csv('Dups.csv')
            df = df.append(dups)
            del dups
            df['Submitted'] = pd.to_datetime(df['Submitted'])

    # Create new dataframe with pivot based on 'Form Response Name'
    df = df.pivot(index='Form Response Name', columns='Form Question Name', values='Answer').reset_index()
    dfdates = df[['Form Response Name','Submitted', 'User Username']].copy()
    dfdates.drop_duplicates(subset=['Form Response Name', 'Submitted'], keep='last', inplace=True)

    # Merge dataframes and return
    results = pd.merge(df, dfdates, how='left', left_on='Form Response Name', right_on='Form Response Name')
    results = pd.merge(results, attch, how='left', left_on='Form Response Name', right_on='Form Response Name')
    return results


def add_tipp_formating(old_tipp_df):
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