# -*- coding: utf-8 -*-
"""
Created on Tue Feb  6 15:04:19 2018

@author: Rebecca Davies

Script imports requested variables from Census API for given geometries. 
Geometries are specified as FIPS codes, variables are specified by name and 
data set. THe current version requests ACS 2016 5-year data from the detailed 
tables for all specified variables for all counties in all states.
https://www.census.gov/data/developers/data-sets.html
"""

# Import relevant libraries
import pandas as pd
import copy
# Library that facilitates using the Census API https://github.com/ljwolf/cenpy
import cenpy as cen

# Set path
data_path = ''

# Function that retrieves data from Census API
def get_data(fips, geo_name, geos='block group', api=True, apikey):
    
    # Variables (can be set up as 'if' statement to have different variables
    # sets for different scenarios/applications)
    cols=['B01001_004','B01001_005','B01001_006','B01001_007',
          'B01001_001','B01001_002','B01001_026','B01001_003',     
          'B01001_008','B01001_009','B01001_010','B01001_011',
          'B01001_012','B01001_013','B01001_014','B01001_015',
          'B01001_016','B01001_017','B01001_018','B01001_019',
          'B01001_020','B01001_021','B01001_022','B01001_023',
          'B01001_024','B01001_025','B01001_027','B01001_028',
          'B01001_029','B01001_030','B01001_031','B01001_032',
          'B01001_033','B01001_034','B01001_035','B01001_036',
          'B01001_037','B01001_038','B01001_039','B01001_040',
          'B01001_041','B01001_042','B01001_043','B01001_044',
          'B01001_045','B01001_046','B01001_047','B01001_048',
          'B01001_049','B03002_001','B03002_002','B03002_003',
          'B03002_004','B03002_005','B03002_006','B03002_007',
          'B03002_008','B03002_009','B03002_012','B03002_013',
          'B03002_014','B03002_015','B03002_016','B03002_017',
          'B03002_018','B03002_019','B23025_001','B23025_002',
          'B23025_006','B23025_003','B23025_004','B23025_005',
          'B23025_007','B19001_001','B08301_001','B08301_002',
          'B19001_002','B19001_003','B19001_004','B19001_005',
          'B19001_006','B19001_007','B19001_008','B19001_009',
          'B19001_010','B19001_011','B19001_012','B19001_013',
          'B19001_014','B19001_015','B19001_016','B19001_017',
          'B19013_001','B19055_001','B19055_002','B19055_003',
          'B19057_001','B19057_002','B19057_003','B19083_001',
          'C17002_001','C17002_002','B17024_004','B17024_005',
          'B17024_017','B17024_018','B17024_030','B17024_031',
          'B17024_043','B17024_044','B17024_056','B17024_057',
          'B17024_069','B17024_070','B17024_082','B17024_083',
          'B17024_095','B17024_096','B17024_108','B17024_109',
          'B17024_121','B17024_122','C17002_004','C17002_006',
          'C17002_005','C17002_007','C17002_008','C17002_003',
          'B08301_003','B08301_004','B08301_010','B08301_016',
          'B08301_017','B08301_018','B08301_019','B08301_020',
          'B08301_021','B08013_001','B08134_001','B05002_001',
          'B05002_002','B05002_013','B05002_014','B05002_021']
    if api:
        # Sort out the column names, add 'E' for estimates
        cols_ests = [i+'E' for i in cols]
        # Add 'M' for margin of error values
        cols_moes = [i+'M' for i in cols]
        # Combine estimates and MOE to request 
        cols_all_data = cols_ests + cols_moes
        cols_all_data = list(set(cols_all_data))  # get the unique column names
        cols_all = copy.copy(cols_all_data)
        # Add GEO_ID to requested columns
        cols_all.extend(['GEO_ID'])
        # API connection, input your API key - can be acquired from census website
        api_key = apikey  # Insert census API key
        api_database = 'ACSDT5Y2016'  # ACS 5 yr 2016 detailed tables currently selected
        # Establish connection to database
        api_conn = cen.base.Connection(api_database)
        # Pull column info from API
        cols_detail = api_conn.variables.ix[cols_all].label.to_dict()
        cols_detail = pd.DataFrame.from_dict(cols_detail, orient='index')
        # Pull the data from the API
        data = pd.DataFrame()
        # Extract state and county from FIPS codes provided to function
        state = x[0:2]
        county = x[2:]
        data = data.append(api_conn.query(cols_all, geo_unit=geos, geo_filter={'state':state, 'county':county}, apikey = api_key))
        # Convert dataframe index to the clean FIPS code
        index = data.GEO_ID
        # Remove prefix to block group ID
        if geos=='block group':
            index = index.str.replace('1500000US','')
        data.index = index
        # Ensure data is in numeric format
        data[cols_all_data] = data[cols_all_data].apply(pd.to_numeric)
        # Organize output dataframes and add multiindex column headers
        output_ests = data[cols_ests]
        output_moes = data[cols_moes]
    return output_ests, output_moes, cols_detail

# Import file with all state and county fips codes 
fipsMaster = pd.read_csv(data_path+'fips_codes.csv')
# Rename column with excess characters
fipsMaster.rename(columns={'\ufeffSummary Level':'Summary_Level'}, inplace=True)
# Select only county-level codes
fipsCounty = fipsMaster[fipsMaster['Summary_Level']==50]
# Convert FIPS codes to strings and add preceeding zeros
fipsCounty['State Code (FIPS)'] = fipsCounty['State Code (FIPS)'].astype(str).str.zfill(2)
fipsCounty['County Code (FIPS)'] = fipsCounty['County Code (FIPS)'].astype(str).str.zfill(3) 
# Combine state and county codes into one column/string
fipsCounty['stcy'] = fipsCounty['State Code (FIPS)'] + fipsCounty['County Code (FIPS)']

# Create output data frame
output = pd.DataFrame()

# For each state/county FIPS code combo, request data from API
for x in fipsCounty['stcy']: 
    #output
    output_ests, output_moes, cols_detail = get_data(fips=x,geo_name=x,
                                                     geos='block group', api=True)
    # Add data for state/county combo to master CSV
    output = output.append(output_ests) 

# Write master output file to disk
output.to_csv(data_path+'acs_data.csv')    