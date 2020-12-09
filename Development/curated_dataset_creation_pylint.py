'''
Author: Mu Sigma
Updated: 09 Dec 2020
Version: 2
Tasks: Python script to create curated dataset and features like
Outages in LAST N HOURS, CAUSE, OCCURN, clue day flags
- Removed re-predictions loop
Schedule: At the end of every 30 minutes
'''

# standard library import
import os
import ast
import math
from datetime import datetime, timedelta
import logging
from configparser import ConfigParser, ExtendedInterpolation
import pandas as pd
from pandas.io import gbq
import subprocess

# third party import
import geopy.distance

# Setup logs
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# read config file
CONFIGPARSER = ConfigParser(interpolation=ExtendedInterpolation())
CONFIGPARSER.read('confignew0001.ini')
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

## **Read OMS Weather Source curated dataset**
BUCKET_NAME = CONFIGPARSER['CURATED_DATA']['CURATED_DATA_STAGING_PATH']
DF_OMSDS = pd.read_csv(BUCKET_NAME)

DF_OMSDS = DF_OMSDS.loc[:, ~DF_OMSDS.columns.str.contains('^Unnamed')]
DF_OMSDS = DF_OMSDS.loc[:, ~DF_OMSDS.columns.str.contains('^_c0')]
DF_OMSDS['CREATION_DATETIME'] = pd.to_datetime(DF_OMSDS['CREATION_DATETIME'],
                                               errors='coerce')
DF_OMSDS['Date'] = DF_OMSDS['CREATION_DATETIME'].dt.date
logging.info('OMS LIVE Dataframe Loaded \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_OMSDS)

## **Priority Queuing Feature**
# 1. Rank based on simple customer quantity as mentioned by Business
# (live rankings to be followed,  numerical feature)
DF_OMSDS.sort_values(by=['CREATION_DATETIME'], inplace=True)
DF_OMSDS.reset_index(drop=True, inplace=True)
DF_OMSDS['ENERGIZED_DATETIME'] = DF_OMSDS.ENERGIZED_DATETIME.fillna('1900-01-01')
DF_OMSDS['ENERGIZED_DATETIME'] = pd.to_datetime(DF_OMSDS['ENERGIZED_DATETIME'])
DF_OMSDS['ENERGIZED_DATETIME'] = DF_OMSDS['ENERGIZED_DATETIME'].apply(
    lambda row: row.strftime("%Y-%m-%d"))
DF_OMSDS = DF_OMSDS[DF_OMSDS.ENERGIZED_DATETIME == '1900-01-01']

SHAPE = DF_OMSDS.shape[0]
if SHAPE != 0:
    pass
else:
    raise Exception('No new Outages, All outages are already ENERGIZED for')

DF_OMSDS['Priority_Customer_Qty'] = DF_OMSDS['CUST_QTY'].rank(method='dense', ascending=False)

# 2. Rank based on the factor of distance from centroid and customer quantity
#(live rankings to be followed,approach #2, numerical feature)
DF_OMSDS['LIVE_OUTAGE'] = DF_OMSDS.OUTAGE_ID.nunique()
DF_OMSDS['Center_LAT'] = (DF_OMSDS.LAT)/(DF_OMSDS.LIVE_OUTAGE)
DF_OMSDS['Center_LONG'] = (DF_OMSDS.LONG)/(DF_OMSDS.LIVE_OUTAGE)

def cal_distance_from_center_lat_long(lat, long, center_lat, center_long):
    '''
    Input - Latitude, Longitude , Center latitude and Center longitude
    Output - Distance from center latitude 
    '''
    if math.isnan(lat)|math.isnan(long)|math.isnan(center_lat)|math.isnan(center_long):
        return None
    else:
        coords1 = [lat, long]
        coords2 = [center_lat, center_long]
        return geopy.distance.distance(coords1, coords2).miles

DF_OMSDS['Dis_From_Live_Centriod'] = DF_OMSDS.apply(lambda x: cal_distance_from_center_lat_long(
    x['LAT'], x['LONG'], x['Center_LAT'], x['Center_LONG']), axis=1)
DF_OMSDS['Dis_From_Live_Centriod'] = DF_OMSDS['Dis_From_Live_Centriod'].apply(pd.to_numeric,
                                                                              errors='coerce')
DF_OMSDS['Dis_From_Live_Centriod_div_Cust_qty'] = (
    DF_OMSDS['Dis_From_Live_Centriod'])/(DF_OMSDS['CUST_QTY'])
DF_OMSDS['Priority_Dist_Customer_Qty'] = DF_OMSDS['Dis_From_Live_Centriod_div_Cust_qty'].rank(
    method='max', ascending=True)
DF_OMSDS.drop(['Center_LAT',
               'Center_LONG', 'Dis_From_Live_Centriod', 'LIVE_OUTAGE'], axis=1, inplace=True)
logging.info('Priority Queuing Features Added \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_OMSDS)

## **Read output dataset and filter for Predicted Flag**
DF_FIN = DF_OMSDS.copy(deep=True)

try:
    DF_PRED = CONFIGPARSER['CURATED_DATA']['IPL_PREDICTIONS_QUERY']
    DF_PRED = gbq.read_gbq(DF_PRED, project_id=CONFIGPARSER['SETTINGS']['PROJECT_ID'])
    PREDICTIONS = list(DF_PRED['OUTAGE_ID'].unique())
    DF_OMSDS['OUTAGE_ID'] = DF_OMSDS['OUTAGE_ID'].astype(str)
    DF_OMSDS['OUTAGE_ID'] = DF_OMSDS['OUTAGE_ID'].str.replace(' ', '')
    DF_FINAL = DF_OMSDS[~DF_OMSDS['OUTAGE_ID'].isin(PREDICTIONS)]
    DF_FINAL.reset_index(drop=True, inplace=True)
except:
    DF_FINAL = DF_OMSDS

## **Copy filtered flag back in DF_FIN**    
DF_FIN = DF_FINAL.copy(deep=True)

SHAPE = DF_FINAL.shape[0]
if SHAPE != 0:
    pass
else:
    raise Exception('No new Outages,  All outages are already predicted for')

logging.info('Filtered for Predicted Outages \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_FINAL)


## **Add Number of Outages for CLUE,  CAUSE,  OCCURN**
# convert to datetime columns for operations
DF_FINAL['CREATION_DATETIME'] = pd.to_datetime(DF_FINAL['CREATION_DATETIME'], errors='coerce')
DF_FIN['CREATION_DATETIME'] = pd.to_datetime(DF_FIN['CREATION_DATETIME'], errors='coerce')

# extract date from datetime column
DF_FINAL['Date'] = DF_FINAL['CREATION_DATETIME'].dt.date
DF_FIN['Date'] = DF_FIN['CREATION_DATETIME'].dt.date

DF_NO_OF_OUTAGES = DF_FINAL.groupby(['Date'], as_index=False).agg({'POWER_OUT_CLUE_FLG':'sum',
                                                                   'OPEN_DEVICE_CLUE_FLG':'sum',
                                                                   'IVR_CLUE_FLG':'sum',
                                                                   'ANIMAL_CAUSE_FLG':'sum',
                                                                   'WIRE_OCCURN_FLG':'sum'})

DF_NO_OF_OUTAGES.rename(columns={'POWER_OUT_CLUE_FLG':'NO_OF_POWER_OUT_CLUE_PER_DAY',
                                 'OPEN_DEVICE_CLUE_FLG':'NO_OF_OPEN_DEVICE_CLUE_PER_DAY',
                                 'IVR_CLUE_FLG':'NO_OF_IVR_CLUE_PER_DAY',
                                 'ANIMAL_CAUSE_FLG':'NO_OF_ANIMAL_CAUSE_PER_DAY',
                                 'WIRE_OCCURN_FLG':'NO_OF_WIRE_OCCURN_PER_DAY'}, inplace=True)

try:
    DF_CLUE_COUNT = pd.read_csv(CONFIGPARSER['CURATED_DATA']['CLUE_COUNT_CSV'])
except:
    DF_NO_OF_OUTAGES.to_csv(CONFIGPARSER['CURATED_DATA']['CLUE_COUNT_CSV'], index=False)

RECORD_DATE = (datetime.today()-timedelta(days=1)).date()
logging.info('Type of the record date %s \n', type(RECORD_DATE))

DF_CLUE_COUNT['Date'] = pd.to_datetime(DF_CLUE_COUNT.Date).dt.date
logging.info('Type of the clue count date columns %s \n', type(DF_CLUE_COUNT.Date[0]))

DF_CLUE_COUNT_CURRENT = DF_CLUE_COUNT[DF_CLUE_COUNT.Date >= RECORD_DATE]
DF_CLUE_COUNT_CURRENT.reset_index(drop=True, inplace=True)
logging.info('Check if Clue Count Dataframe is empty: %s \n', DF_CLUE_COUNT_CURRENT.empty)

DF_CLUE_COUNT = DF_CLUE_COUNT.append(DF_NO_OF_OUTAGES)
DF_CLUE_COUNT = DF_CLUE_COUNT.groupby(['Date'], as_index=False).agg({
    'NO_OF_POWER_OUT_CLUE_PER_DAY':'sum',
    'NO_OF_OPEN_DEVICE_CLUE_PER_DAY':'sum',
    'NO_OF_IVR_CLUE_PER_DAY':'sum',
    'NO_OF_ANIMAL_CAUSE_PER_DAY':'sum',
    'NO_OF_WIRE_OCCURN_PER_DAY':'sum'})

logging.info('Datatype of the date column: %s \n', type(DF_CLUE_COUNT.Date[0]))

DF_CLUE_COUNT.to_csv(CONFIGPARSER['CURATED_DATA']['CLUE_COUNT_CSV'], index=False)
DF_FIN = DF_FIN.merge(DF_CLUE_COUNT, how='left', left_on=['Date'], right_on=['Date'])
DF_FIN.reset_index(drop=True, inplace=True)

logging.info('CLUE, CAUSE, OCCURN codes added \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_FIN)

## **Change all columns to Flag values**
FINAL_LIST = ast.literal_eval(CONFIGPARSER.get("CURATED_DATA", "FLAG_LIST"))
FINAL_LIST = list(FINAL_LIST)
logging.info('Name of all FLAG COLUMNS LOADED: %s \n', FINAL_LIST)

MAPIN = {1:'True', 0:'False'}

for i in FINAL_LIST:
    DF_FIN[i] = DF_FIN[i].map(MAPIN)

# fillna's null values using forward fill method
DF_FIN.fillna(method='ffill', inplace=True)
DF_FIN['CITY_NAM'].fillna('NO_CITY', inplace=True)

DF_FIN['CREATION_DATETIME'] = DF_FINAL['CREATION_DATETIME'].apply(
    lambda row: row.strftime("%Y-%m-%d %H:%M:%S"))
DF_FIN['CREATION_DATETIME'] = pd.to_datetime(DF_FINAL['CREATION_DATETIME'], errors='coerce')

logging.info('Changed All Flag Columns to TRUE/FALSE \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_FIN)

## **Add outages in last N hours feature**
RECORD_DATE_OUTAGE = (datetime.today()-timedelta(days=2)).date()

try:
    PRED_OUTAGES = CONFIGPARSER.get('CURATED_DATA', 'PRED_OUTAGES_QUERY')+"'"+str(RECORD_DATE_OUTAGE)+"'"
    logging.info(PRED_OUTAGES)
    DF_PRED_OUTAGES = gbq.read_gbq(PRED_OUTAGES, project_id=CONFIGPARSER.get('SETTINGS', 'PROJECT_ID'))
    DF_PRED_OUTAGES.reset_index(drop=True, inplace=True)
except:
    DF_PRED_OUTAGES = pd.DataFrame()

DF_PRED_OUTAGES['Creation_Time'] = pd.to_datetime(DF_PRED_OUTAGES['Creation_Time'], errors='coerce')
DF_PRED_OUTAGES['Creation_Time'] = DF_PRED_OUTAGES['Creation_Time'].apply(
    lambda row: row.strftime("%Y-%m-%d %H:%M:%S"))
DF_PRED_OUTAGES['CREATION_DATETIME'] = pd.to_datetime(DF_PRED_OUTAGES['Creation_Time'], errors='coerce')
DF_PRED_OUTAGES.drop(['Creation_Time'], axis=1, inplace=True)

DF_FINAL_OUTAGE_COUNT = DF_FIN[['OUTAGE_ID', 'CREATION_DATETIME']]
DF_FINAL_OUTAGE_COUNT = DF_FINAL_OUTAGE_COUNT.append(DF_PRED_OUTAGES)
DF_FINAL_OUTAGE_COUNT.drop_duplicates(subset='OUTAGE_ID', keep='last', inplace=True)
DF_FINAL_OUTAGE_COUNT.reset_index(inplace=True)

def count_outage_minutes(group):
    '''
    Input - Dataframe grouped by a feature
    Output - Dataframe having groups with added outage minutes
    '''
    group = group.reset_index(drop=True)
    df_temp = DF_FINAL_OUTAGE_COUNT[['OUTAGE_ID', 'CREATION_DATETIME']]
    df_temp['minutes'] = (group['CREATION_DATETIME'][0]-DF_FINAL_OUTAGE_COUNT['CREATION_DATETIME']).dt.total_seconds().div(60)
    df_temp = df_temp[df_temp.minutes > 0]
    group['Outages_in_last_1hr'] = len(df_temp[df_temp.minutes <= 60])
    group['Outages_in_last_2hr'] = len(df_temp[df_temp.minutes <= 120])
    group['Outages_in_last_3hr'] = len(df_temp[df_temp.minutes <= 180])
    group['Outages_in_last_4hr'] = len(df_temp[df_temp.minutes <= 240])
    group['Outages_in_last_5hr'] = len(df_temp[df_temp.minutes <= 300])
    group['Outages_in_last_6hr'] = len(df_temp[df_temp.minutes <= 360])
    group['Outages_in_last_7hr'] = len(df_temp[df_temp.minutes <= 420])
    group['Outages_in_last_8hr'] = len(df_temp[df_temp.minutes <= 480])
    group['Outages_in_last_9hr'] = len(df_temp[df_temp.minutes <= 540])
    group['Outages_in_last_10hr'] = len(df_temp[df_temp.minutes <= 600])
    return group

def grouping_fn_minutes(df):
    '''
    Input - Dataframe
    Output - Dataframe grouped by outage id
    '''
    liveoutage = df.groupby(['OUTAGE_ID'], as_index=False).apply(count_outage_minutes)
    return liveoutage

LIVE_OUTAGES = grouping_fn_minutes(DF_FIN)
LIVE_OUTAGES.reset_index(drop=True, inplace=True)
logging.info('Added Outages in last N features to Analytical Dataset \n')
QC_CHECK_SHAPE_AND_COLUMNS(LIVE_OUTAGES)

## **Write curated dataset to CSV's**
if 'DOWNSTREAM_CUST_QTY' not in LIVE_OUTAGES:
    LIVE_OUTAGES['DOWNSTREAM_CUST_QTY'] = LIVE_OUTAGES['CUST_QTY']

LIVE_OUTAGES['KVA_VAL'] = LIVE_OUTAGES['DOWNSTREAM_KVA_VAL']
LIVE_OUTAGES.fillna(method='ffill', inplace=True)

logging.info('Path to CSV %s', CONFIGPARSER['CURATED_DATA']['LIVE_OUTAGES_BACKUP_CSV'])
LIVE_OUTAGES.to_csv(CONFIGPARSER['CURATED_DATA']['LIVE_OUTAGES_BACKUP_CSV'], index=False)

logging.info('Path to CSV %s', CONFIGPARSER['CURATED_DATA']['LIVE_OUTAGES_PATH']+'IPL_OMS_LIVE_Data_'+datetime.today().strftime('%Y%m%d%H%M')+'.csv')
LIVE_OUTAGES.to_csv(CONFIGPARSER['CURATED_DATA']['LIVE_OUTAGES_PATH']+'IPL_OMS_LIVE_Data_'+datetime.today().strftime('%Y%m%d%H%M')+'.csv', index=False)
logging.info('Column Names in LIVE OUTAGES: %s', list(LIVE_OUTAGES.columns))

## **Write to Big Query Tables**
LIVE_OUTAGES.columns = LIVE_OUTAGES.columns.str.replace('.', '_')
CURATED_QUERY = 'Select * from ' + CONFIGPARSER['SETTINGS']['BQ_CURATED_DATASET']
CURATED_DATA = gbq.read_gbq(CURATED_QUERY, project_id=CONFIGPARSER['SETTINGS']['PROJECT_ID'])
logging.info('Big Query table loaded \n')

CURATED_DATA.append(LIVE_OUTAGES)
logging.info('Big query table appended \n')
CURATED_DATA.drop_duplicates(['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE'], keep='last', inplace=True)

logging.info('Curated Dataset Big query table name %s', CONFIGPARSER['SETTINGS']['BQ_CURATED_DATASET'])

CURATED_DATA.to_gbq(CONFIGPARSER['SETTINGS']['BQ_CURATED_DATASET'], project_id=CONFIGPARSER['SETTINGS']['PROJECT_ID'],
                    chunksize=None, reauth=False, if_exists='append', auth_local_webserver=False,
                    table_schema=None, location=None, progress_bar=True, credentials=None)
logging.info('Final Big Query table created \n')
