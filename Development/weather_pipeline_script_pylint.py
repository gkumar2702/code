'''
Authored by Musigma
Updated: 15 Dec 2020
Tasks: Weather data for next n hours for big query tables
Description: PYHTON SCRIPT TO CREATE THE BIG QUERY
BACKEND TABLES FOR THE WEATHER VIEW DASHBOARD
Environment: Composer-0001
Run-time environments: Pyspark,SparkR and python callable
'''

# standard library imports
import logging
import datetime as dt
from datetime import date, timedelta, datetime
from configparser import ConfigParser, ExtendedInterpolation
import pandas as pd
import numpy as np
from pandas.io import gbq
from pytz import timezone

# third party import
from google.cloud import storage

# Setup logs
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# read config file
CONFIGPARSER = ConfigParser(interpolation=ExtendedInterpolation())
CONFIGPARSER.read('config_storm.ini')
logging.info('Config File Loaded')
logging.info('Config File Sections %s', CONFIGPARSER.sections())

# Fetching hourly weather data for 3 days
DF_WEATHER = CONFIGPARSER['WEATHER_PIPELINE']['WEATHER_DATA_FETCH_QUERY']
DF_WEATHER = gbq.read_gbq(DF_WEATHER, project_id=CONFIGPARSER['WEATHER_PIPELINE']['PROJECT_ID'])
DF_WEATHER.drop_duplicates(subset=['timestamp','Location'],keep='first',inplace=True)
logging.info('BQ Table Loaded \n')
logging.info('Shape of the weather data %s \n', DF_WEATHER.shape)

DF_WEATHER['timestamp'] = pd.to_datetime(DF_WEATHER['timestamp'], format='%Y-%m-%d %H:%M:%S',
                                         errors='coerce')
DF_WEATHER['timestamp'] = DF_WEATHER['timestamp'].dt.tz_convert('US/Eastern')
DF_WEATHER.drop(['Date1'], axis=1, inplace=True)
DF_WEATHER['Date'] = DF_WEATHER['timestamp'].dt.date
DF_WEATHER.sort_values(['timestamp', 'Location'], inplace=True, ascending=False)
DF_WEATHER.reset_index(drop=True, inplace=True)
CURRENT_DATE_HOUR = datetime.now(timezone('US/Eastern')).strftime('%Y-%m-%d %H')
CURRENT_DATE_HOUR = datetime.strptime(CURRENT_DATE_HOUR, '%Y-%m-%d %H')
logging.info('Current Date Hour %s \n', CURRENT_DATE_HOUR)

SIX_HOURS_FROM_NOW = CURRENT_DATE_HOUR + timedelta(hours=6)
SIX_HOURS_FROM_NOW = SIX_HOURS_FROM_NOW.strftime('%Y-%m-%d %H:%M:%S%z')
TWELVE_HOURS_FROM_NOW = CURRENT_DATE_HOUR + timedelta(hours=12)
TWELVE_HOURS_FROM_NOW = TWELVE_HOURS_FROM_NOW.strftime('%Y-%m-%d %H:%M:%S%z')
TWETYFOUR_HOUR_FROM_NOW = CURRENT_DATE_HOUR + timedelta(hours=24)
TWETYFOUR_HOUR_FROM_NOW = TWETYFOUR_HOUR_FROM_NOW.strftime('%Y-%m-%d %H:%M:%S%z')
logging.info('Six Hours From Now %s \n', SIX_HOURS_FROM_NOW)
logging.info('Twelve Hours From Now %s \n', TWELVE_HOURS_FROM_NOW)
logging.info('Twentyfour Hour From Now %s \n', TWETYFOUR_HOUR_FROM_NOW)

CURRENT_DATE_HOUR = CURRENT_DATE_HOUR.strftime('%Y-%m-%d %H:%M:%S')
logging.info('Current date hour after conversion %s \n', CURRENT_DATE_HOUR)

# list to create time filters
TIMEFILTER_LIST = ['Next 6 Hours', 'Next 12 Hours', 'Next 24 Hours']

# filter for next 6 hours, 12 hours and 24 hours
NEXT6HOURS = DF_WEATHER[(DF_WEATHER['timestamp'] > CURRENT_DATE_HOUR) &
                        (DF_WEATHER['timestamp'] <= SIX_HOURS_FROM_NOW)]
NEXT6HOURS['timestamp'] = NEXT6HOURS['timestamp'].astype(str)
NEXT12HOURS = DF_WEATHER[(DF_WEATHER['timestamp'] > CURRENT_DATE_HOUR) &
                         (DF_WEATHER['timestamp'] <= TWELVE_HOURS_FROM_NOW)]
NEXT12HOURS['timestamp'] = NEXT12HOURS['timestamp'].astype(str)
NEXT24HOURS = DF_WEATHER[(DF_WEATHER['timestamp'] > CURRENT_DATE_HOUR) &
                         (DF_WEATHER['timestamp'] <= TWETYFOUR_HOUR_FROM_NOW)]
NEXT24HOURS['timestamp'] = NEXT24HOURS['timestamp'].astype(str)

# filter and create unique timefilters for next 6, 12, 24 hours 
NEXT6HOURS = list(NEXT6HOURS['timestamp'].unique())
logging.info('Next 6 hours %s \n', NEXT6HOURS)
NEXT12HOURS = list(NEXT12HOURS['timestamp'].unique())
logging.info('Next 12 hours %s \n', NEXT12HOURS)
NEXT24HOURS = list(NEXT24HOURS['timestamp'].unique())
logging.info('Next 24 hours %s \n', NEXT24HOURS)

FILTER_DF = pd.DataFrame({'Filter_ID': TIMEFILTER_LIST[0], 'timestamp': NEXT6HOURS})
FILTER_DF_1 = pd.DataFrame({'Filter_ID': TIMEFILTER_LIST[1], 'timestamp': NEXT12HOURS})
FILTER_DF_2 = pd.DataFrame({'Filter_ID': TIMEFILTER_LIST[2], 'timestamp': NEXT24HOURS})
logging.info('Dataframe Filtered \n')

FINAL_DF = pd.DataFrame()
FINAL_DF = FINAL_DF.append([FILTER_DF, FILTER_DF_1, FILTER_DF_2])
FINAL_DF.reset_index(drop=True, inplace=True)

FINAL_DF['timestamp'] = pd.to_datetime(FINAL_DF['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
DF_WEATHER['timestamp'] = pd.to_datetime(DF_WEATHER['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

# Write to big query
DF_WEATHER.to_gbq(CONFIGPARSER['WEATHER_PIPELINE']['BQ_DASHBOARD_PATH'], project_id=CONFIGPARSER['WEATHER_PIPELINE']['PROJECT_ID'],
                  chunksize=None, reauth=False, if_exists='replace', auth_local_webserver=False,
                  table_schema=None, location=None, progress_bar=True, credentials=None)

FINAL_DF.to_gbq(CONFIGPARSER['WEATHER_PIPELINE']['BQ_TIMEFILTER_PATH'], project_id=CONFIGPARSER['WEATHER_PIPELINE']['PROJECT_ID'],
                chunksize=None, reauth=False, if_exists='replace', auth_local_webserver=False,
                table_schema=None, location=None, progress_bar=True, credentials=None)
logging.info('BQ Table Written')
