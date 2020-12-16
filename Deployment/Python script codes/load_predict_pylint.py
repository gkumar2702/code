'''
Author: Mu Sigma
Updated: 16 Dec 2020
Version: 2
Tasks : Load hypertuned Random forest model to predict total time for restoration
and provide ETR's dataset and provided 0002 anbalytics locations
'''

# standard library imports
import pickle
import logging
from pytz import timezone
import datetime as dt
from datetime import datetime, date, timedelta
import pandas as pd
from pandas.io import gbq
import numpy as np
from configparser import ConfigParser, ExtendedInterpolation

# third party import
import gcsfs

# Setup logs
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# read config file
CONFIGPARSER = ConfigParser(interpolation=ExtendedInterpolation())
CONFIGPARSER.read('config_ETR.ini')
logging.info('Config File Loaded')
logging.info('Config File Sections %s', CONFIGPARSER.sections())

def QC_CHECK_SHAPE_AND_COLUMNS(df):
    '''
    Input - Dataframe with operations/addtion of features/columns or joins performed
    Output - Log Info using shape of dataframe and columns present
    '''
    logging.info('****QC Check**** \n')
    logging.info('Shape of the DataFrame %s \n', df.shape)
    logging.info('Columns present in the DataFrame: %s \n', list(df.columns))
    return

## **Read OMS Live Curated Dataset**
BUCKET_NAME = CONFIGPARSER['LOAD_AND_PREDICT']['STAGING_BUCKET']
logging.info('Staging Bucket %s', BUCKET_NAME)

try:
    DF_ADS_FINAL = pd.read_csv(BUCKET_NAME)
except:
    raise Exception('Final curated dataset not found')

DF_ADS_FINAL = DF_ADS_FINAL.loc[:, ~DF_ADS_FINAL.columns.str.contains('^Unnamed')]
DF_ADS_FINAL = DF_ADS_FINAL.loc[:, ~DF_ADS_FINAL.columns.str.contains('^c0')]

logging.info('OMS LIVE CURATED DATASET LOADED \n')
logging.info('No of NAs if any: %s \n', DF_ADS_FINAL.isnull().values.any())
QC_CHECK_SHAPE_AND_COLUMNS(DF_ADS_FINAL)

# Read Storm Profiles Data
BUCKET_NAME = CONFIGPARSER['LOAD_AND_PREDICT']['STORM_PROFILE_BUCKET']

DF_ADS_FINAL['CREATION_DATETIME'] = pd.to_datetime(
    DF_ADS_FINAL['CREATION_DATETIME'], errors='coerce')
DF_ADS_FINAL['Date'] = DF_ADS_FINAL['CREATION_DATETIME'].dt.date

UNIQUE_DATES = DF_ADS_FINAL[['Date']]
UNIQUE_DATES.drop_duplicates(subset=['Date'], keep='first', inplace=True)
UNIQUE_DATES['Date'] = UNIQUE_DATES['Date'].apply(lambda x: x.strftime('%Y%m%d'))
UNIQUE = UNIQUE_DATES['Date'].to_list()

logging.info('Dates for which strom profiles will be read: %s \n', UNIQUE)

STORM_PROFILES_LOCATION = BUCKET_NAME + '/Storm_Profiles/'
logging.info('Location of Storm Profiles %s \n', STORM_PROFILES_LOCATION)
STORM_PROFILES_FILES = []

try:
    for i in UNIQUE:
        FILENAME = STORM_PROFILES_LOCATION + 'storm_profiles_{}.csv'.format(i)
        STORM_PROFILES_FILES.append(pd.read_csv(FILENAME))
except:
    raise Exception('Storm Profiles Data not found')

STORMPROFILES_DF = pd.concat(STORM_PROFILES_FILES)
STORMPROFILES_DF.reset_index(drop=True, inplace=True)
STORMPROFILES_DF = STORMPROFILES_DF.loc[:, ~STORMPROFILES_DF.columns.str.contains('^Unnamed')]
STORMPROFILES_DF = STORMPROFILES_DF.loc[:, ~STORMPROFILES_DF.columns.str.contains('_c0')]
STORMPROFILES_DF = STORMPROFILES_DF[['timestamp', 'Location', 'clusters']]
STORMPROFILES_DF.rename({'timestamp' : 'Date', 'Location' : 'Marker_Location',
                         'clusters' : 'Cluster_ID'}, axis=1, inplace=True)
logging.info('Pre-processing Storm Info Done \n')
QC_CHECK_SHAPE_AND_COLUMNS(STORMPROFILES_DF)

def rename_storm_info(row):
    """
    Input - Cluster Number
    Output - Full description and name of the clsuter after profling
    """
    cluster_profile = ''
    if row == 'Cluster1':
        cluster_profile = 'Hot Days with Sudden Rain'
    if row == 'Cluster2':
        cluster_profile = 'Strong Breeze with Sudden Rain'
    if row == 'Cluster3':
        cluster_profile = 'Thunderstorms'
    if row == 'Cluster4':
        cluster_profile = 'Chilly Day with Chances of Snow'
    if row == 'Cluster5':
        cluster_profile = 'Strong Chilled Breeze with Chances of Snow'
    if row == 'Cluster6':
        cluster_profile = 'Hot Days with Chance of Rain'
    
    return cluster_profile

STORMPROFILES_DF['Cluster_ID'] = STORMPROFILES_DF['Cluster_ID'].apply(rename_storm_info)

def remove_spaces(string):
    '''
    Input - Maker name with spaces
    Output - Marker name without space
    Example i/p, o/p - Marker 1, Marker1
    '''
    return string.replace(" ", "")


STORMPROFILES_DF['Marker_Location'] = STORMPROFILES_DF.apply(lambda x: remove_spaces(x['Marker_Location']), axis=1)

# merge storm profiles with final dataframe
DF_ADS_FINAL['Date'] = pd.to_datetime(DF_ADS_FINAL['Date'])
STORMPROFILES_DF['Date'] = pd.to_datetime(STORMPROFILES_DF['Date'])
DF_ADS_FINAL = DF_ADS_FINAL.merge(STORMPROFILES_DF, how='left',
                                  left_on=['Date', 'Marker_Location'],
                                  right_on=['Date', 'Marker_Location'])


logging.info('Cluster Profiles Added \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_ADS_FINAL)

## **Load Hyper Tuned RF model**
try:
    RF_MODEL = pd.read_pickle(CONFIGPARSER['LOAD_AND_PREDICT']['MODEL_LOCATION'])
except:
    raise Exception('Model Object not loaded')
logging.info("Model Loaded \n")

MODEL_FEATURES = CONFIGPARSER['LOAD_AND_PREDICT']['MODEL_FEATURES']
try:
    FEATURES_DF = pd.read_csv(MODEL_FEATURES)
except:
    raise Exception('Model Features not loaded')

FEATURE_LIST = list(FEATURES_DF.Features_List)
logging.info('Features Loaded \n')
logging.info('Name of the features present %s \n', FEATURE_LIST)

# Feature Pre-Processing before it is sent to the Model
DF_ADS_FINAL_V1 = DF_ADS_FINAL.copy(deep=True)

DF_ADS_FINAL_V1['POWER_OUT_CLUE_FLG_False'] = DF_ADS_FINAL_V1['POWER_OUT_CLUE_FLG'].apply(
    lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['ST_OCCURN_FLG_False'] = DF_ADS_FINAL_V1['ST_OCCURN_FLG'].apply(
    lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['WIRE_OCCURN_FLG_False'] = DF_ADS_FINAL_V1['WIRE_OCCURN_FLG'].apply(
    lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['FUSE_OCCURN_FLG_False'] = DF_ADS_FINAL_V1['FUSE_OCCURN_FLG'].apply(
    lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['ST_OCCURN_FLG_True'] = DF_ADS_FINAL_V1['ST_OCCURN_FLG'].apply(
    lambda row: 1 if (row is True) else 0)
DF_ADS_FINAL_V1['PUBLIC_SAFETY_OCCURN_FLG_True'] = DF_ADS_FINAL_V1['PUBLIC_SAFETY_OCCURN_FLG'].apply(lambda row: 1 if (row is True) else 0)
DF_ADS_FINAL_V1['NO_CAUSE_FLG_False'] = DF_ADS_FINAL_V1['NO_CAUSE_FLG'].apply(
    lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['ANIMAL_CAUSE_FLG_True'] = DF_ADS_FINAL_V1['ANIMAL_CAUSE_FLG'].apply(
    lambda row: 1 if (row is True) else 0)
DF_ADS_FINAL_V1['DAY_FLAG_True'] = DF_ADS_FINAL_V1['DAY_FLAG'].apply(
    lambda row: 1 if (row is True) else 0)
DF_ADS_FINAL_V1['UG_CAUSE_FLG_False'] = DF_ADS_FINAL_V1['UG_CAUSE_FLG'].apply(
    lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['POLE_CLUE_FLG_False'] = DF_ADS_FINAL_V1['POLE_CLUE_FLG'].apply(
    lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['TREE_CAUSE_FLG_True'] = DF_ADS_FINAL_V1['TREE_CAUSE_FLG'].apply(
    lambda row: 1 if (row is True) else 0)
DF_ADS_FINAL_V1['ANIMAL_CAUSE_FLG_False'] = DF_ADS_FINAL_V1['ANIMAL_CAUSE_FLG'].apply(
    lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['TREE_CAUSE_FLG_False'] = DF_ADS_FINAL_V1['TREE_CAUSE_FLG'].apply(
    lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['PUBLIC_SAFETY_OCCURN_FLG_False'] = DF_ADS_FINAL_V1['PUBLIC_SAFETY_OCCURN_FLG'].apply(lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['POWER_OUT_CLUE_FLG_True'] = DF_ADS_FINAL_V1['POWER_OUT_CLUE_FLG'].apply(
    lambda row: 1 if (row is True) else 0)
DF_ADS_FINAL_V1['CITY_NAM_NO_CITY'] = DF_ADS_FINAL_V1['CITY_NAM'].apply(
    lambda row: 1 if (row is 'NO_CITY') else 0)

logging.info("Preprocessing Done \n")

Y_TEST_PRED = RF_MODEL.predict(DF_ADS_FINAL_V1[FEATURE_LIST])
Y_TEST_PRED = np.exp(Y_TEST_PRED)
Y_TEST_PRED = list(Y_TEST_PRED)
logging.info('Predicted Values Are %s', Y_TEST_PRED)


def business_layer_add_addtional_time(predicted_values):
    '''
    Input - Prediction of Outages in minutes
    Output - If predicted  values are less than 1440 min 
    Add 45 mins to predictions, Else Add 360 mins to predictions
    '''
    new_pred_values = []
    for i in range(len(predicted_values)):
        if predicted_values[i] <= 1440:
            new_pred_values.append(predicted_values[i] + 45)
        elif predicted_values[i] > 1440:
            new_pred_values.append(predicted_values[i] + 360)
        else :
            new_pred_values.append(predicted_values[i])
        
    return new_pred_values

Y_TEST_PRED = business_layer_add_addtional_time(Y_TEST_PRED)
DF_ADS_FINAL['Predicted_TTR'] = Y_TEST_PRED
logging.info('Business Logic Added \n')
logging.info('Predicted ETRs after business logic %s \n', Y_TEST_PRED)
logging.info('Predicted ETRs added to final dataframe \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_ADS_FINAL)

def created_predicted_etr(creation_datetime, time_in_minutes):
    """
    This function calculates the ETR timestamp using creation datetime
    and time for restoration in minutes

    Function returns ETR timestamp

    Args:
        creation_datetime - Outage Creation Datetime
		time_in_minutes - TTR in minutes
    """
    newtime = creation_datetime + timedelta(minutes=time_in_minutes)
    newtime = newtime.strftime("%Y-%m-%d %H:%M:%S %z")
    return newtime

DF_ADS_FINAL['CREATION_DATETIME'] = pd.to_datetime(DF_ADS_FINAL['CREATION_DATETIME'])
DF_ADS_FINAL['Restoration_Period'] = round(DF_ADS_FINAL['Predicted_TTR'], 0)
DF_ADS_FINAL['Predicted_ETR'] = DF_ADS_FINAL.apply(
    lambda row: created_predicted_etr(row['CREATION_DATETIME'], row['Predicted_TTR']), axis=1)
DF_ADS_FINAL['Predicted_ETR'] = pd.to_datetime(DF_ADS_FINAL['Predicted_ETR'])
DF_ADS_FINAL['Predicted_ETR'] = DF_ADS_FINAL['Predicted_ETR'].dt.round('10min')
DF_ADS_FINAL['CREATION_DATETIME'] = DF_ADS_FINAL['CREATION_DATETIME'].apply(
    lambda row: row.strftime("%Y/%m/%d %H:%M:%S"))
DF_ADS_FINAL['Predicted_ETR'] = DF_ADS_FINAL['Predicted_ETR'].apply(
    lambda row: row.strftime("%Y/%m/%d %H:%M:%S"))

logging.info('Final ETRs Created \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_ADS_FINAL)

# Final Pre-processing to Write Outputs in correct Format
DF_ADS_FINAL = DF_ADS_FINAL[['OUTAGE_ID', 'INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID',
                             'DNI_EQUIP_TYPE', 'CREATION_DATETIME', 'Predicted_ETR',
                             'Restoration_Period', 'Cluster_ID']]

DF_ADS_FINAL.rename({'CREATION_DATETIME' : 'Creation_Time',
                     'Predicted_ETR' : 'Estimated_Restoration_Time',
                     'Restoration_Period' : 'ETR','Cluster_ID' : 'Weather_Profile'}, axis=1, inplace=True)


# Read and Add Insertion Time to Outages
DF_PRED = DF_ADS_FINAL.copy(deep=True)
DF_PRED['Last_Updated'] = datetime.now().strftime("%Y-%m-%d %H:%M")

DF_PRED.to_gbq(CONFIGPARSER['SETTINGS']['BQ_IPL_PREDICTIONS'], project_id=CONFIGPARSER['SETTINGS']['PROJECT_ID'],
                    chunksize=None, reauth=False, if_exists='append', auth_local_webserver=False,
                    table_schema=None, location=None, progress_bar=True, credentials=None)

DF_PRED.to_gbq(CONFIGPARSER['SETTINGS']['BQ_IPL_LIVE_PREDICTIONS'], project_id=CONFIGPARSER['SETTINGS']['PROJECT_ID'],
                    chunksize=None, reauth=False, if_exists='replace', auth_local_webserver=False,
                    table_schema=None, location=None, progress_bar=True, credentials=None)

logging.info('Prediction Live path %s', CONFIGPARSER['LOAD_AND_PREDICT']['PREDICTION_LIVE'])
DF_ADS_FINAL.to_csv(CONFIGPARSER['LOAD_AND_PREDICT']['PREDICTION_LIVE'], index=False)

YEAR_MONTH = datetime.now(timezone('US/Eastern')).strftime('%Y-%m')
CURRENT_DATE = datetime.now(timezone('US/Eastern')).strftime('%Y-%m-%d')
CURRENT_DATE_HOUR = datetime.now(timezone('US/Eastern')).strftime('%Y%m%d%H%M')
logging.info('Year Month in Eastern Time Zone %s', YEAR_MONTH)
logging.info('Current Month in Eastern Time Zone %s', CURRENT_DATE)
logging.info('Current Date & Hour in Eastern Time Zone %s \n', CURRENT_DATE_HOUR)

FILENAME = CONFIGPARSER['LOAD_AND_PREDICT']['PREDICTION_BACKUP'] + '{}/{}/TTR_predictions_{}.csv'.format(YEAR_MONTH, CURRENT_DATE, CURRENT_DATE_HOUR)
logging.info('Backup Storage Predictions Storage Path: %s \n', FILENAME)

DF_ADS_FINAL.to_csv(FILENAME, index=False)
