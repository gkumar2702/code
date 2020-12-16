'''
Author - Mu Sigma
Updated: 16 Dec 2020
Version 2
Tasks: This scripts collects data from Weather Source Location
Adds it to OMS LIVE PREPROCESSED DATA
Schedule: At the end of every 32 minutes
Input : LIVE OMS PREPROCESSED DATA
Output : LIVE OMS PREPROCESSED DATA WITH WEATHER VARIABLES
'''

# standard library imports
import sys
import os
from datetime import date, datetime
import datetime as dt
import logging
import pandas as pd
import numpy as np
from configparser import ConfigParser, ExtendedInterpolation
from google.cloud import storage
from pandas.io import gbq

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

## **Read curated OMS data**
DATA_COLLATION_STAGING_PATH = CONFIGPARSER['DATA_COLLATION']['DATA_COLLATION_STAGING_PATH']
logging.info('Data Collation Staging Path: %s \n', DATA_COLLATION_STAGING_PATH)

try:
    DF_OMS_LIVE = pd.read_csv(DATA_COLLATION_STAGING_PATH)
except:
    raise Exception('OMS preprocessed data not found')

DF_OMS_LIVE = DF_OMS_LIVE.loc[:, ~DF_OMS_LIVE.columns.str.contains('^Unnamed')]
DF_OMS_LIVE = DF_OMS_LIVE.loc[:, ~DF_OMS_LIVE.columns.str.contains('^_c0')]
logging.info('OMS Dataframe Loaded \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_OMS_LIVE)

## **Dynamic reading of files from Weather Source Storage Location**
DF_OMS_LIVE['CREATION_DATETIME'] = pd.to_datetime(
    DF_OMS_LIVE['CREATION_DATETIME'], errors='coerce')
DF_OMS_LIVE['Date'] = DF_OMS_LIVE['CREATION_DATETIME'].dt.date

UNIQUE_DATES = DF_OMS_LIVE[['Date']]
UNIQUE_DATES.drop_duplicates(subset=['Date'], keep='first', inplace=True)
UNIQUE_DATES['Date'] = UNIQUE_DATES['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
UNIQUE = UNIQUE_DATES['Date'].to_list()
logging.info('Unique Dates for files: %s \n', UNIQUE)

# read weather source data from big query
WSFILES = pd.DataFrame()

try:
    for i in UNIQUE:
        WS_LOCATION = CONFIGPARSER['DATA_COLLATION']['WS_QUERY'].format(i)
        WSFILES = WSFILES.append(gbq.read_gbq(WS_LOCATION, project_id=CONFIGPARSER['SETTINGS']['PROJECT_ID']))
    WS_DF = WSFILES.drop_duplicates(['timestamp', 'Location'], keep='last')
except:
    raise Exception('Weather Data not found in big query')

## **Weather Source Weather data cleaning**
WS_DF['Date'] = pd.to_datetime(WS_DF['timestamp']).dt.date
WS_DF['Location'] = WS_DF['Location'].astype(str)
logging.info('Weather Source Data Loaded \n')
QC_CHECK_SHAPE_AND_COLUMNS(WS_DF)

## **Add Weather Source features**
# Add range for columns with negative values
WS_DF['tempRange'] = WS_DF['tempMax'] - WS_DF['tempMin']
WS_DF['windSpdRange'] = WS_DF['windSpdMax'] - WS_DF['windSpdMin']
WS_DF['sfcPresRange'] = WS_DF['sfcPresMax'] - WS_DF['sfcPresMin']
WS_DF['cldCvrRange'] = WS_DF['cldCvrMax'] - WS_DF['cldCvrMin']
WS_DF['relHumRange'] = WS_DF['relHumMax'] - WS_DF['relHumMin']

## Add ratio for columns which dont have negative values
WS_DF['relHumRatio'] = WS_DF['relHumMax'] / WS_DF['relHumMin']
WS_DF['sfcPresRatio'] = WS_DF['sfcPresMax'] / WS_DF['sfcPresMin']

## data qc check for nulls
WS_DF = WS_DF.replace([np.inf, -np.inf], np.nan)
nulls = WS_DF.isnull().sum()
DF_NULLS = pd.DataFrame({'Feature': nulls.index, 'VALUES': nulls.values})
DF_NULLS[DF_NULLS.VALUES >= 1]
logging.info('Features to Weather Source Data Added')
QC_CHECK_SHAPE_AND_COLUMNS(WS_DF)

## **Weather Source OMS weather mapping**
# make mapping consistent 
def marker_weather_mapping(marker_name):
    '''
    Input - Marker name with IPL%
    Output - Only Maker names with no IPL
    Example i/p, o/p - IPL_Marker1, Marker1
    '''
    name = marker_name[4:]
    return name

def remove_spaces(string):
    '''
    Input - Maker name with spaces
    Output - Marker name without space
    Example i/p, o/p - Marker 1, Marker1
    '''
    return string.replace(" ", "")

WS_DF['Location'] = WS_DF.apply(lambda x: marker_weather_mapping(x['Location']), axis=1)
DF_OMS_LIVE['Marker_Location'] = DF_OMS_LIVE.apply(lambda x: remove_spaces(x['Marker_Location']), axis=1)
DF_OMS_LIVE['Date'] = pd.to_datetime(DF_OMS_LIVE['Date'], errors='coerce')
WS_DF['Date'] = pd.to_datetime(WS_DF['Date'], errors='coerce')
WS_DF.drop(['Latitude', 'Longitude', 'timestamp', 'timestampInit', 'Job_Update_Time'], axis=1, inplace=True)
logging.info('Merging Weather Source and OMS Live Data OLD OMS shape %s \n', DF_OMS_LIVE.shape)

DF_OMS_LIVE = pd.merge(DF_OMS_LIVE, WS_DF, how='left',
                       left_on=['Date', 'Marker_Location'], right_on=['Date', 'Location'])
DF_OMS_LIVE.drop(['Date'], axis=1, inplace=True)
logging.info('Live OMS data merged with Weather Souce Weather Data \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_OMS_LIVE)

## **Renaming weather attributes**
def create_wind_direction(x_wind_direction):
    '''
    Input - Wind direction in degrees
    Output - Wind direction classes based on directions
    '''
    if(x_wind_direction >= 1) & (x_wind_direction < 45):
        direction = 'N-E-N'
    elif(x_wind_direction >= 45) & (x_wind_direction < 90):
        direction = 'N-E-E'
    elif(x_wind_direction >= 90) & (x_wind_direction < 180):
        direction = 'S-E-E'
    elif(x_wind_direction >= 135) & (x_wind_direction < 180):
        direction = 'S-E-S'
    elif(x_wind_direction >= 180) & (x_wind_direction < 225):
        direction = 'S-W-S'
    elif(x_wind_direction >= 225) & (x_wind_direction < 270):
        direction = 'S-W-W'
    elif(x_wind_direction >= 270) & (x_wind_direction < 315):
        direction = 'N-W-W'
    elif(x_wind_direction >= 315) & (x_wind_direction < 360):
        direction = 'N-W-N'
    else:
        direction = None

    return direction

DF_OMS_LIVE['WIND_DIRECTION'] = DF_OMS_LIVE['windDirAvg'].apply(create_wind_direction)

def create_weekend_flag(x_flag):
    '''
	Input - Weekday Number 1,2,3,4,5,6,7
	Output - WEEKEND 1, 0
    '''
    if x_flag >= 5:
        flag = 1
    else:
        flag = 0
    return flag

DF_OMS_LIVE['weekday'] = pd.to_datetime(DF_OMS_LIVE['CREATION_DATETIME']).dt.dayofweek
DF_OMS_LIVE['weekend_flag'] = DF_OMS_LIVE['weekday'].apply(create_weekend_flag)
DF_OMS_LIVE.drop(['weekday', 'Location'], axis=1, inplace=True)
logging.info('Final Dataset Created \n')
QC_CHECK_SHAPE_AND_COLUMNS(DF_OMS_LIVE)

## **Write OMS Live and added Weather Source Dataset for Curated Processing**
OMS_LIVE_PATH = CONFIGPARSER['DATA_COLLATION']['OMS_LIVE_PATH']
logging.info('OMS LIVE PATH %s \n', OMS_LIVE_PATH)

DF_OMS_LIVE.to_csv(OMS_LIVE_PATH, index=False)
