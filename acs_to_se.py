# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 14:40:33 2018

@author: Rebecca Davies

Script imports ACS data downloaded from Census API, modifies ACS variables to  
match Social Explorer variables, and combines ACS data with scored BNA block 
groups for entire US.
"""

# Import relevant libraries
import pandas as pd
import glob

# Set file path
path = ''

# Combine multiple ACS data files into one, downloaded in batches from Census API
allFiles = glob.glob(path + "/acs_data/acs_sheets/*.csv")
frame = pd.DataFrame()
list_ = []
for file_ in allFiles:
    sheet = pd.read_csv(file_,index_col=None, header=0)
    list_.append(sheet)
df_dem = pd.concat(list_)

# Import attribute file with geographic information for all US census blocks
# US block group shapefile downloaded as 2010 state block group files from Census 
# website and merged into one shapefile using QGIS
geobg = pd.read_csv(path + '/acs_data/bg_shapefiles/bg_us.csv')

# Combine block group attribute file and ACS data by block group ID
df = pd.merge(df_dem, geobg, how='left', left_on='GEO_ID', right_on='GEOID10')

# Remove extra variables that were incurred on ACS data download
df.drop(['B01001_004E.1', 'B01001_005E.1', 'B01001_006E.1',
 'B01001_007E.1', 'B23025_002E.1', 'B23025_006E.1',
 'B23025_003E.1', 'B23025_003E.2', 'B23025_004E.1',
 'B23025_005E.1', 'C17002_001E.1', 'C17002_002E.1', 'GEOID10'], axis=1, inplace=True, errors='ignore')

# Combine and rename ACS variables to match Social Explorer variables
df['SE_T001_001'] = df['B01001_001E']
df['SE_T002_001'] = df['B01001_001E']
# Convert square meters to square miles 
# 1 square mile = 2,589,988.11 square meters
df['SE_T002_003'] = df['ALAND10']/2589988
df['SE_T003_001'] = (df['ALAND10'] + df['AWATER10'])/2589988
df['SE_T003_002'] = df['ALAND10']/2589988
df['SE_T003_003'] = df['AWATER10']/2589988
df['SE_T004_001'] = df['B01001_001E']
df['SE_T004_002'] = df['B01001_002E']
df['SE_T004_003'] = df['B01001_026E']
df['SE_T007_001'] = df['B01001_001E']
df['SE_T007_002'] = df['B01001_003E'] + df['B01001_027E']
df['SE_T007_003'] = df['B01001_004E'] + df['B01001_028E']
df['SE_T007_004'] = df['B01001_005E'] + df['B01001_029E']
df['SE_T007_005'] = df['B01001_006E'] + df['B01001_030E']
df['SE_T007_006'] = df['B01001_007E'] + df['B01001_008E'] + df['B01001_009E'] + \
                    df['B01001_010E'] + df['B01001_031E'] + df['B01001_032E'] + \
                    df['B01001_033E'] + df['B01001_034E']
df['SE_T007_007'] = df['B01001_011E'] + df['B01001_012E'] + df['B01001_035E'] + df['B01001_036E']
df['SE_T007_008'] = df['B01001_013E'] + df['B01001_014E'] + df['B01001_037E'] + df['B01001_038E']
df['SE_T007_009'] = df['B01001_015E'] + df['B01001_016E'] + df['B01001_039E'] + df['B01001_040E']
df['SE_T007_010'] = df['B01001_017E'] + df['B01001_018E'] + df['B01001_019E'] + \
                    df['B01001_041E'] + df['B01001_042E'] + df['B01001_043E']
df['SE_T007_011'] = df['B01001_020E'] + df['B01001_021E'] + df['B01001_022E'] + \
                    df['B01001_044E'] + df['B01001_045E'] + df['B01001_046E']
df['SE_T007_012'] = df['B01001_023E'] + df['B01001_024E'] + df['B01001_047E'] + df['B01001_048E']
df['SE_T007_013'] = df['B01001_025E'] + df['B01001_049E']
df['SE_T014_001'] = df['B03002_001E']
df['SE_T014_002'] = df['B03002_002E']
df['SE_T014_003'] = df['B03002_003E']
df['SE_T014_004'] = df['B03002_004E']
df['SE_T014_005'] = df['B03002_005E']
df['SE_T014_006'] = df['B03002_006E']
df['SE_T014_007'] = df['B03002_007E']
df['SE_T014_008'] = df['B03002_008E']
df['SE_T014_009'] = df['B03002_009E']
df['SE_T014_010'] = df['B03002_012E']
df['SE_T014_011'] = df['B03002_013E']
df['SE_T014_012'] = df['B03002_014E']
df['SE_T014_013'] = df['B03002_015E']
df['SE_T014_014'] = df['B03002_016E']
df['SE_T014_015'] = df['B03002_017E']
df['SE_T014_016'] = df['B03002_018E']
df['SE_T014_017'] = df['B03002_019E']
df['SE_T033_001'] = df['B23025_001E']
df['SE_T033_002'] = df['B23025_002E']
df['SE_T033_003'] = df['B23025_006E']
df['SE_T033_004'] = df['B23025_003E']
df['SE_T033_005'] = df['B23025_004E']
df['SE_T033_006'] = df['B23025_005E']
df['SE_T033_007'] = df['B23025_007E']
df['SE_T036_001'] = df['B23025_002E']
df['SE_T036_002'] = df['B23025_006E']
df['SE_T036_003'] = df['B23025_003E']
df['SE_T037_001'] = df['B23025_003E']
df['SE_T037_002'] = df['B23025_004E']
df['SE_T037_003'] = df['B23025_005E']
df['SE_T056_001'] = df['B19001_001E']
df['SE_T056_002'] = df['B19001_002E']
df['SE_T056_003'] = df['B19001_003E']
df['SE_T056_004'] = df['B19001_004E']
df['SE_T056_005'] = df['B19001_005E']
df['SE_T056_006'] = df['B19001_006E']
df['SE_T056_007'] = df['B19001_007E']
df['SE_T056_008'] = df['B19001_008E']
df['SE_T056_009'] = df['B19001_009E']
df['SE_T056_010'] = df['B19001_010E']
df['SE_T056_011'] = df['B19001_011E']
df['SE_T056_012'] = df['B19001_012E']
df['SE_T056_013'] = df['B19001_013E']
df['SE_T056_014'] = df['B19001_014E']
df['SE_T056_015'] = df['B19001_015E']
df['SE_T056_016'] = df['B19001_016E']
df['SE_T056_017'] = df['B19001_017E']
df['SE_T057_001'] = df['B19013_001E']
df['SE_T078_001'] = df['B19055_001E']
df['SE_T078_002'] = df['B19055_002E']
df['SE_T078_003'] = df['B19055_003E']
df['SE_T080_001'] = df['B19057_001E']
df['SE_T080_002'] = df['B19057_002E']
df['SE_T080_003'] = df['B19057_003E']
df['SE_T157_001'] = df['B19083_001E']
df['SE_T117_001'] = df['C17002_001E']
df['SE_T117_002'] = df['C17002_002E']
df['SE_T117_003'] = df['B17024_004E'] + df['B17024_017E'] + df['B17024_030E'] + \
                    df['B17024_043E'] + df['B17024_056E'] + df['B17024_069E'] + \
                    df['B17024_082E'] + df['B17024_095E'] + df['B17024_108E'] + \
                    df['B17024_121E']
df['SE_T117_004'] = df['B17024_005E'] + df['B17024_018E'] + df['B17024_031E'] + \
                    df['B17024_044E'] + df['B17024_057E'] + df['B17024_070E'] + \
                    df['B17024_083E'] + df['B17024_096E'] + df['B17024_109E'] + \
                    df['B17024_122E']
df['SE_T117_005'] = df['C17002_004E'] + df['C17002_005E']
df['SE_T117_006'] = df['C17002_006E'] + df['C17002_007E']
df['SE_T117_007'] = df['C17002_008E']
df['SE_T118_001'] = df['C17002_001E']
df['SE_T118_002'] = df['C17002_002E'] + df['C17002_003E']
df['SE_T118_003'] = df['C17002_004E'] + df['C17002_005E'] + df['C17002_006E'] + df['C17002_007E']
df['SE_T118_004'] = df['C17002_002E'] + df['C17002_003E'] + df['C17002_004E'] + \
                    df['C17002_005E'] + df['C17002_006E'] + df['C17002_007E']
df['SE_T118_005'] = df['C17002_008E']
df['SE_T128_001'] = df['B08301_001E']
df['SE_T128_002'] = df['B08301_002E']
df['SE_T128_009'] = df['B08301_003E']
df['SE_T128_010'] = df['B08301_004E']
df['SE_T128_003'] = df['B08301_010E'] + df['B08301_016E']
df['SE_T128_004'] = df['B08301_017E']
df['SE_T128_005'] = df['B08301_018E']
df['SE_T128_006'] = df['B08301_019E']
df['SE_T128_007'] = df['B08301_020E']
df['SE_T128_008'] = df['B08301_021E']
df['SE_T147_001'] = df['B08013_001E']/df['B08134_001E']
df['SE_T133_001'] = df['B05002_001E']
df['SE_T133_002'] = df['B05002_002E']
df['SE_T133_003'] = df['B05002_013E']
df['SE_T133_004'] = df['B05002_014E']
df['SE_T133_005'] = df['B05002_021E']

# Remove ACS variables, leaving just Social Explorer variables
df.drop(['B01001_001E', 'B01001_002E', 'B01001_026E', 'B01001_003E',
         'B01001_004E', 'B01001_005E', 'B01001_006E', 'B01001_007E',
         'B01001_008E', 'B01001_009E', 'B01001_010E', 'B01001_011E',
         'B01001_012E', 'B01001_013E', 'B01001_014E', 'B01001_015E',
         'B01001_016E', 'B01001_017E', 'B01001_018E', 'B01001_019E',
         'B01001_020E', 'B01001_021E', 'B01001_022E', 'B01001_023E',
         'B01001_024E', 'B01001_025E', 'B01001_027E', 'B01001_028E',
         'B01001_029E', 'B01001_030E', 'B01001_031E', 'B01001_032E',
         'B01001_033E', 'B01001_034E', 'B01001_035E', 'B01001_036E',
         'B01001_037E', 'B01001_038E', 'B01001_039E', 'B01001_040E',
         'B01001_041E', 'B01001_042E', 'B01001_043E', 'B01001_044E',
         'B01001_045E', 'B01001_046E', 'B01001_047E', 'B01001_048E',
         'B01001_049E', 'B03002_001E', 'B03002_002E', 'B03002_003E',
         'B03002_004E', 'B03002_005E', 'B03002_006E', 'B03002_007E',
         'B03002_008E', 'B03002_009E', 'B03002_012E', 'B03002_013E',
         'B03002_014E', 'B03002_015E', 'B03002_016E', 'B03002_017E',
         'B03002_018E', 'B03002_019E', 'B23025_001E', 'B23025_002E',
         'B23025_006E', 'B23025_003E', 'B23025_004E', 'B23025_005E',
         'B23025_007E', 'B23025_002E', 'B23025_006E', 'B23025_003E',
         'B23025_003E', 'B23025_004E', 'B23025_005E', 'B19001_001E',
         'B19001_002E', 'B19001_003E', 'B19001_004E', 'B19001_005E',
         'B19001_006E', 'B19001_007E', 'B19001_008E', 'B19001_009E',
         'B19001_010E', 'B19001_011E', 'B19001_012E', 'B19001_013E',
         'B19001_014E', 'B19001_015E', 'B19001_016E', 'B19001_017E',
         'B19013_001E', 'B19055_001E', 'B19055_002E', 'B19055_003E',
         'B19057_001E', 'B19057_002E', 'B19057_003E', 'B19083_001E',
         'C17002_001E', 'C17002_002E', 'B17024_004E', 'B17024_005E',
         'B17024_017E', 'B17024_018E', 'B17024_030E', 'B17024_031E',
         'B17024_043E', 'B17024_044E', 'B17024_056E', 'B17024_057E',
         'B17024_069E', 'B17024_070E', 'B17024_082E', 'B17024_083E',
         'B17024_095E', 'B17024_096E', 'B17024_108E', 'B17024_109E',
         'B17024_121E', 'B17024_122E', 'C17002_004E', 'C17002_006E',
         'C17002_005E', 'C17002_007E', 'C17002_008E', 'C17002_001E',
         'C17002_002E', 'C17002_003E', 'B08301_001E', 'B08301_002E',
         'B08301_003E', 'B08301_004E', 'B08301_010E', 'B08301_016E', 
         'B08301_017E', 'B08301_018E', 'B08301_019E', 'B08301_020E',
         'B08301_021E', 'B08013_001E', 'B08134_001E', 'B05002_001E',
         'B05002_002E', 'B05002_013E', 'B05002_014E', 'B05002_021E'], axis=1, inplace=True)
    
# Import block group BNA scores
scores = pd.read_csv(path+'/bg_bna_scores.csv')

# Combine ACS data with block group BNA scores
score_acs = pd.merge(df, scores, how='outer', left_on='GEO_ID', right_on='BLKGRP_ID')

# Identify tracts that have a BNA score but aren't listed in ACS data
diff = set(scores.BLKGRP_ID)-set(df.GEO_ID)

# Rename columns to match Social Explorer output
score_acs.rename(columns={'OVERALL_SC':'AVG_OVERALL_SCORE', 'ALAND10':'Geo_AREALAND', 'AWATER10':'Geo_AREAWATER', 'BLKGRP_ID':'BLOCKGRP_ID'}, inplace=True)

# Save block groups with BNA scores and ACS data to file
score_acs.to_csv(path + 'ACS_MASTER_2018.csv', index=False)