# -*- coding: utf-8 -*-
"""
Created on Wed Feb 14 15:56:08 2018

@author: Rebecca Davies

Script combines BNA scores from blocks 
into block groups for all 50 states.

Inputs: Scored BNA blocks as individual Excel files for every city
Outputs: CSV file containing block groups for all scored BNA cities 
"""

# Import relevant libraries
import pandas as pd
import os

# Set path
path = ''

# Function that translates block-level data to block-group level data
# Inputs are 1) state file containing all blocks and BNA scores, 2) crosswalk
# that translates blocks to block groups
def convert_blocks(stateFile, crosswalk):
    
    # Only retain relevant columns  
    stateBlockScore= stateFile[['city', 'state', 'BLOCKID10', 'OVERALL_SC']]
    
    # At least one file had a different column name for Block Group Number, 
    # so this corrects for that possibility
    if 'BLKGRP_#' in crosswalk.columns:
        crosswalk.rename(columns={'BLKGRP_#':'BLKGRP#'}, inplace=True)
    # Only retain relevant columns
    crossSelect = crosswalk[['GEOID_VALID', 'PLACE_ID', 'BLKGRP#', 'BLKGRP_ID', 'MATCH_KEY',
               'PLACENAME']]    
        
    '''
    Although block numbers are unique, some appear twice
    This is because they are associated with the periphary of at least two
    BNA cities. As a result, they can have different BNA scores associated with 
    different BNA cities.
    '''
    
    # Set index to block number for both data sets
    stateBlockScore.set_index('BLOCKID10', inplace=True)  
    crossSelect.set_index('GEOID_VALID', inplace=True)
   
    # Join stateFile and crosswalk on block number (index)
    stateFull = stateBlockScore.join(crossSelect, how='outer')
    
    # Collapse blocks into block groups, average BNA scores across constituent blocks
    # Blocks with NA values are ignored by the averaging function, e.g. blocks with scores of 
    # {5, 7, NA} will average to 6, but blocks with scores of {5, 7, 0} will average to 4
    stateBG = stateFull.groupby(['BLKGRP_ID', 'PLACENAME', 'PLACE_ID', 'BLKGRP#', 'MATCH_KEY', 'state', 'city']).mean().reset_index()

    # Return block groups with associated BNA score, per state
    return stateBG

# Create master dataframe to store all output
master = pd.DataFrame()

# Run through all input BNA scored block files, organized by city and then state
list_of_files = {}
for (dirpath, dirnames, filenames) in os.walk(path+'/bna'):
    for filename in filenames:
        if filename.endswith('.xlsx'): 
            list_of_files[filename] = os.sep.join([dirpath, filename])
            
# All states
listofstates = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL',
                'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA',
                'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE',
                'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

# Combine files for each state to input in conversion functino above
for state in listofstates[0:1]:
    
    # Identify state currently processing
    print(state)
    
    # Import state crosswalk of 2010 blocks to block groups, w/ place names 
    crosswalk = pd.read_excel(path + '/crosswalk/'+state+'_CensusBlockMaster.xlsx')
    
    # Select all files that contain state abbreviation in file name
    statedict = {k:v for (k,v) in list_of_files.items() if state in v}

    # Create dataframe to hold state data
    stateGrid = pd.DataFrame()
    
    # Loop through each city BNA file for the state
    for file in statedict.values():
        
        # Import city file with BNA block-level scores
        stateFile = pd.read_excel(file)
        # Add column with BNA city name to file, based on folder name 
        stateFile['city'] = os.path.basename(file[:-5])
        # Add column with BNA state name to file, based on state abbreviation
        stateFile['state'] = state
        
        # Identify city currently being processed
        print(os.path.basename(file[:-5]))
        
        # Add city file to state data
        stateGrid = stateGrid.append(stateFile)        
        
    # Run conversion function to match blocks to block groups and avg BNA score
    bgConverted = convert_blocks(stateGrid, crosswalk)
    
    # Identify block groups without BNA scores
    missing = bgConverted[bgConverted.OVERALL_SC.isnull()]
    
    # Only keep block groups with BNA scores
    bgScored = bgConverted[~bgConverted.BLKGRP_ID.isin(missing.BLKGRP_ID)]
    
    # Add state data to master file
    master = master.append(bgScored, ignore_index=True) 
    
    # Identify when state has been fully processed
    print('Done ' + state)

# Save master file to disk
master.to_csv(path + '/bg_bna_scores.csv', index=False)


