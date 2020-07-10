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

OUTPUT_JSON_FILE_LOCATION = './docs/processed_data.json'

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
    '''Destructive.'''
    ecommerce_df = ecommerce_df.rename(columns={'Customer ID': 'CustomerID'}, copy=False) # make column naming convention consistent 
    ecommerce_df.drop(ecommerce_df[ecommerce_df['CustomerID'] != ecommerce_df['CustomerID']].index, inplace=True)
    ecommerce_df = ecommerce_df.astype({'CustomerID': int}, copy=False)
    assert len(ecommerce_df[ecommerce_df.isnull().any(axis=1)])==0, 'Raw data contains NaN'    
    return ecommerce_df

def process_ecommerce_data_rowwise(ecommerce_df: pd.DataFrame) -> pd.DataFrame:
    '''Destructive.'''
    ecommerce_df['InvoiceTotal'] = ecommerce_df.parallel_apply(lambda row: row.Quantity * row.Price, axis=1)
    return ecommerce_df

def generate_day_of_week_dict(ecommerce_df: pd.DataFrame) -> dict:
    day_index_to_string_map = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
    ecommerce_df = ecommerce_df.copy(deep=False)
    ecommerce_df['DayOfWeek'] = ecommerce_df.InvoiceDate.parallel_map(datetime.datetime.weekday)
    day_of_week_df = ecommerce_df.groupby('DayOfWeek').agg({
        'Invoice': 'nunique', 
        'StockCode': 'nunique',
        'Quantity': 'sum',
        'InvoiceTotal': 'sum',
        'CustomerID': 'nunique',
    }).rename(columns={
        'Invoice': 'UniqueInvoiceCount', 
        'StockCode': 'UniquePurchasedItemCount',
        'Quantity': 'TotalQuantity',
        'InvoiceTotal': 'TotalPurchaseAmount',
        'CustomerID': 'UniqueCustomerCount', 
    }, copy=False)
    day_of_week_dict = {day_index_to_string_map[day_of_week_index]:data for day_of_week_index, data in day_of_week_df.to_dict(orient='index').items()}
    return day_of_week_dict

def generate_output_dict(ecommerce_df: pd.DataFrame) -> dict:
    day_of_week_data = generate_day_of_week_dict(ecommerce_df)
    output_dict = {
        'day_of_week_data': day_of_week_data,
    }
    return output_dict

##########
# Driver #
##########

@debug_on_error
def process_data() -> None:
    ecommerce_df = pd.read_excel(ECOMMERCE_DATA_XLSX_FILE_LOCATION)
    ecommerce_df = clean_ecommerce_data(ecommerce_df)
    ecommerce_df = process_ecommerce_data_rowwise(ecommerce_df)
    output_dict = generate_output_dict(ecommerce_df)
    with open(OUTPUT_JSON_FILE_LOCATION, 'w') as file_handle:
        json.dump(output_dict, file_handle, indent=4)
    return

if __name__ == '__main__':
    process_data()
 
