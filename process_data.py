#!/usr/bin/python3
'#!/usr/bin/python3 -OO' # @todo use this instead

'''
'''

# @todo update doc string

###########
# Imports #
###########

import json
import tqdm
import datetime
import pandas as pd
import numpy as np
import multiprocessing as mp
from pandarallel import pandarallel

from misc_utilities import *

# @todo verify that all the imports above are used 

###########
# Globals #
###########

pandarallel.initialize(nb_workers=mp.cpu_count(), progress_bar=False, verbose=0)

ECOMMERCE_DATA_XLSX_FILE_LOCATION = './data/online_retail_II.xlsx'

#####################################
# General Data Processing Utilities #
#####################################

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        else:
            return super(CustomEncoder, self).default(obj)

##################################################
# Application Specific Data Processing Utilities #
##################################################

def clean_ecommerce_data(ecommerce_df: pd.DataFrame) -> pd.DataFrame:
    ecommerce_df = ecommerce_df[ecommerce_df['Customer ID'] == ecommerce_df['Customer ID']]
    ecommerce_df = ecommerce_df.astype({'CustomerID': int}, copy=False)
    assert len(ecommerce_df[ecommerce_df.isnull().any(axis=1)])==0, 'Raw data contains NaN'    
    return ecommerce_df

def generate_output_files(ecommerce_df: pd.DataFrame) -> None:
    # @todo fill this in
    return

##########
# Driver #
##########

@debug_on_error
def process_data() -> None:
    ecommerce_df = pd.read_excel(ECOMMERCE_DATA_XLSX_FILE_LOCATION)
    ecommerce_df = clean_ecommerce_data(ecommerce_df)
    generate_output_files(ecommerce_df)
    return

if __name__ == '__main__':
    process_data()
 
