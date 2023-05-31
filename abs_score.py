import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import pycountry
import numpy as np

# convert country names to ISO 3166-1 alpha-2 or alpha-3 codes -- might not be needed anymore
def convert_country_name_to_iso(country_name, code_type='alpha-2'):
    try:
        country = pycountry.countries.search_fuzzy(country_name)[0]
        if code_type == 'alpha-2':
            return country.alpha_2
        elif code_type == 'alpha-3':
            return country.alpha_3
        else:
            return None
    except LookupError:
        return None

# Define the scope and credentials for accessing the Google Spreadsheet
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

# Authenticate and open the Google Spreadsheet
gc = gspread.authorize(credentials)
spreadsheet = gc.open_by_key('google spreadsheet key')

# Load the "ABSLevels" tab as a pandas DataFrame
abs_levels_sheet = spreadsheet.worksheet('ABSLevels')
abs_levels_data = abs_levels_sheet.get_all_records()
abs_levels_df = pd.DataFrame(abs_levels_data)
#replace empty strings by nan
abs_levels_df = abs_levels_df.replace('', np.nan)

# Load the "MissingFoods20230508" tab as a pandas DataFrame
missing_foods_sheet = spreadsheet.worksheet('Curation20230517')
missing_foods_data = missing_foods_sheet.get_all_records()
missing_foods_df = pd.DataFrame(missing_foods_data)
#replace empty strings by nan
missing_foods_df = missing_foods_df.replace('', np.nan)
print(missing_foods_data)

# Load the result tab
results_sheet = spreadsheet.worksheet('resultsABS')
#results_data = missing_foods_sheet.get_all_records()
#results_df = pd.DataFrame(missing_foods_data)

# Iterate over the rows in the "MissingFoods20230508" DataFrame
for index, row in missing_foods_df.iterrows():
    specimen_location = row['Specimen Origin Location']
    origin_variety = row['Country of origin/Variety']
    origin_species = row['Country of origin/ Species level']
    abs_score_specimen_location = 4
    abs_score_origin_variety = 4
    abs_score_origin_species = 4
    
    # Check if the specimen location is not empty
    if pd.notna(specimen_location):
        if specimen_location != 'unknown':
            # Find the corresponding ABS score in the "ABSLevels" DataFrame based on the country code
            abs_score = abs_levels_df.loc[abs_levels_df['Country code'] == specimen_location, 'ABS score'].values
            if pd.notna(abs_score):
                abs_score_specimen_location = abs_score
    # if both country of originin are empty, keep the abs of the specimen location - can be simplify if unknown == 4
    if not pd.notna(origin_variety) and not pd.notna(origin_species):
        abs_score_origin_variety = abs_score_specimen_location
        abs_score_origin_species = abs_score_specimen_location
    else:    
        # Look at Country of Origin/diversity
        if pd.notna(origin_variety): 
            country_list = origin_variety.split(',')
            # Find the corresponding ABS score in the "ABSLevels" DataFrame based on the country code list and keep the highest
            abs_score_origin_variety = abs_levels_df.loc[abs_levels_df['Country name'].isin(country_list), 'ABS score'].max()
        else:
            #if country of originin is empty, keep the abs of the specimen location - remove if we want to keep 4
            abs_score_origin_variety = abs_score_specimen_location
         # Look at Country of Origin/diversity
        if pd.notna(origin_species):
            country_list = origin_species.split(',')
            # Find the corresponding ABS score in the "ABSLevels" DataFrame based on the country code list and keep the highest
            abs_score_origin_species = abs_levels_df.loc[abs_levels_df['Country name'].isin(country_list), 'ABS score'].max()
        else:
            #if country of originin is empty, keep the abs of the specimen location - remove if we want to keep 4
            abs_score_origin_species = abs_score_specimen_location



    # Update the value in the "MissingFoods20230508" DataFrame under "Specimen ABS Status"
    missing_foods_df.at[index, 'Specimen ABS Status'] = max(abs_score_specimen_location, abs_score_origin_variety, abs_score_origin_species)

# replace nan for json export
missing_foods_df = missing_foods_df.replace(np.nan, '')

# Clear the existing "MissingFoods20230508" tab and write the updated data
results_sheet.clear()
results_sheet.update([missing_foods_df.columns.values.tolist()] + missing_foods_df.values.tolist())

# Print the updated "MissingFoods20230508" DataFrame
print("Updated MissingFoods20230508:")
#print(missing_foods_df)