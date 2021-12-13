import sf_data as sf
import data_format as format

## Querying Salesforce for data
date_string = input('Enter the date (YYYY-MM-DD): ')

query = sf.TIPP1_FR_data(date_string)

# tipp1_fr_df
df = sf.query_sf(query)


## Formating Salesforce Data
# Create new dataframe with pivot containing attachment records
attachments = format.format_df(df)

# unpack attachment data
attachments = format.unpack_data(attachments)

attach_pivot = format.TIPP1_FR_format(attachments)

old_tipp_df = format.pivot_frs(df, attachments)

old_tipp_df = format.add_tipp_formating(old_tipp_df)