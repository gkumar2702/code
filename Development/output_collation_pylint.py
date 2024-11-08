'''
Author - Mu Sigma
Updated: 15 Dec 2020
Version: 2
Tasks: Script for collating the output from Storm Level Models
Environment: Composer-0001
Run-time environments: Pyspark,SparkR and python 3.7 callable
'''

# standard library imports
import ast
import logging
from datetime import date, datetime
import warnings
import pandas as pd
from configparser import ConfigParser, ExtendedInterpolation

# Setup logs
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# config file
CONFIGPARSER = ConfigParser(interpolation=ExtendedInterpolation())
CONFIGPARSER.read('config_storm.ini')
logging.info('Config File Loaded')
logging.info('Config File Sections %s', CONFIGPARSER.sections())

# Reading configurations from specified config files
try:
    PATH = CONFIGPARSER['OUTPUT_COLLATION']['OP_PATH']
    OP_BQ_SCHEMA = CONFIGPARSER['OUTPUT_COLLATION']['OP_BQ_SCHEMA']
    OP_BQ_TAB = CONFIGPARSER['OUTPUT_COLLATION']['OP_BQ_TAB']
    PROJECT_ID = CONFIGPARSER['OUTPUT_COLLATION']['PROJECT_ID']
except:
    raise Exception("Config file failed to load")

CURRENT_DATE = date.today()
logging.info('Current Datetime %s \n', CURRENT_DATE)
YEAR_MONTH = pd.to_datetime(CURRENT_DATE).strftime('%Y-%m')
logging.info('Current Year Month %s \n', YEAR_MONTH)
TODAY = pd.to_datetime(CURRENT_DATE).strftime('%Y-%m-%d')
logging.info('Todays Date %s \n', TODAY)

# Reading the input files
try:
    DF2 = pd.read_csv(PATH + '/' + YEAR_MONTH + '/' + TODAY + '/' + "Outages_Prediction.csv")
except:
    raise Exception("Outages Predictions CSV not found in the specified directory")
try:
    DF3 = pd.read_csv(PATH + '/' + YEAR_MONTH + '/' + TODAY + '/' + "Predicted_Cust_Qty.csv")
except:
    raise Exception("Predicted_Cust_Qty CSV not found in the specified directory")

# Merging the input files
FINAL_DF = DF2.merge(DF3, how='left', left_on='Date', right_on='Date')
SHAPE = FINAL_DF.shape[0]
if SHAPE != 0:
    pass
else:
    raise Exception('Collated output from PCA contains 0 rows')

# Renaming the columns
FINAL_DF.rename(columns={'Predicted_Cust_Qty':'Customer_quantity',
                         'predicted_Number_of_OUTAGES':'Number_of_outages'}, inplace=True)

# Creating the Number of days ahead column
CURRENT_DATE = date.today()
FINAL_DF['Today'] = CURRENT_DATE
FINAL_DF['Number_of_days_ahead'] = 0

for i in FINAL_DF.index:
    FINAL_DF['Date'][i] = datetime.strptime(FINAL_DF['Date'][i], '%Y-%m-%d').date()
    FINAL_DF['Number_of_days_ahead'][i] = (FINAL_DF['Date'][i] - FINAL_DF['Today'][i]).days

# Replacing 1 & 2 with 'One day ahead' and 'Two day ahead'
FINAL_DF.loc[(FINAL_DF.Number_of_days_ahead == 1), 'Number_of_days_ahead'] = 'One day ahead'
FINAL_DF.loc[(FINAL_DF.Number_of_days_ahead == 2), 'Number_of_days_ahead'] = 'Two days ahead'

# Keeping only required columns and making changes to the order of the columns
FINAL_DF = FINAL_DF.drop(['Today'], axis=1)
FINAL_DF = FINAL_DF[['Date', 'Number_of_days_ahead',
                     'Number_of_outages', 'Customer_quantity',
                     'Customers_LL_95', 'Customers_UL_95', 'Outages_LL_95', 'Outages_UL_95']]

FINAL_DF['Number_of_outages'] = FINAL_DF['Number_of_outages'].round(0).astype(int)

# Writing the output to the big query table
OP_BQ_TAB_PATH = """{schema}.{output_table}""".format(schema=OP_BQ_SCHEMA,
                                                      output_table=OP_BQ_TAB)
FINAL_DF.to_gbq(OP_BQ_TAB_PATH, project_id=PROJECT_ID,chunksize=None, 
                reauth=False, if_exists='append', auth_local_webserver=False)
