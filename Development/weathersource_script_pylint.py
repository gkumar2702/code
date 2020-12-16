'''
Author - Mu Sigma
Updated: 15 Dec 2020
Version: 2
Tasks:Script for getting the weather source data for Indianapolis Power & Light
Environment: Composer-0002
Run-time environments: Pyspark,SparkR and python 3.7 callable
'''

# standard library imports
import logging
import re
from datetime import datetime, timedelta, date, timedelta
import warnings
import json
from pandas.io import gbq
import pandas as pd
from pandas.io.json import json_normalize
from sklearn.preprocessing import StandardScaler
import requests #to get info from server
from pytz import timezone #date-time conversion
from configparser import ConfigParser, ExtendedInterpolation

# setup logs
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

# configuration setup 
try:
    WS_LOCATION = CONFIGPARSER['SETTINGS_WEATHER_DAILY']['WS_LOCATION']
    PROJECT_ID = CONFIGPARSER['SETTINGS_WEATHER_DAILY']['PROJECT_ID']
    HIST_OP = CONFIGPARSER['SETTINGS_WEATHER_DAILY']['HIST_OP']
    FORC_OP = CONFIGPARSER['SETTINGS_WEATHER_DAILY']['FORC_OP']
except:
    raise Exception("Config file failed to load")

# defining dates to be used for weather data pull
CURRENT_DATE = date.today()
logging.info('Current Datetime %s \n', CURRENT_DATE)
TODAY = pd.to_datetime(CURRENT_DATE).strftime('%Y-%m-%d')
logging.info('Todays Date %s \n', TODAY)
TOMORROW1 = CURRENT_DATE + timedelta(1)
logging.info("Tomorrow's Date %s \n", TOMORROW1)
DAYAFTER1 = CURRENT_DATE + timedelta(2)
logging.info("Day after tomorrow's Date %s \n", DAYAFTER1)
YESTERDAY = CURRENT_DATE - timedelta(1)
logging.info("Yesterday's Date %s \n", YESTERDAY)
YESTERDAY1 = CURRENT_DATE - timedelta(2)
logging.info('Day before yesterday Date %s \n', YESTERDAY1)

# reading weather data for today
TODAY_DATA = gbq.read_gbq(WS_LOCATION.format(TODAY), project_id=PROJECT_ID)
TODAY_DATA.drop_duplicates(['timestamp', 'Location'], keep='last', inplace=True)
TODAY_DATA.reset_index(drop=True, inplace=True)
SHAPE = TODAY_DATA.shape[0]
if SHAPE != 0:
    logging.info('Weather data for today imported')
    pass
else:
    logging.info("Today's weather data not available")


# reading weather data for tomorrow
try:
    NEW_DATA = gbq.read_gbq(WS_LOCATION.format(TOMORROW1), project_id=PROJECT_ID)
except:
    raise Exception("Failed to load Weather data for tomorrow from Bigquery table")
NEW_DATA.drop_duplicates(['timestamp', 'Location'], keep='last', inplace=True)
NEW_DATA.reset_index(drop=True, inplace=True)
SHAPE = NEW_DATA.shape[0]
if SHAPE != 0:
    logging.info('Weather data for tomorrow imported')
    pass
else:
    logging.info("Tomorrow's weather data not available")

# reading weather data for day after tomorrow
try:
    NEW_DATA2 = gbq.read_gbq(WS_LOCATION.format(DAYAFTER1), project_id=PROJECT_ID)
except:
    raise Exception("Failed to load Weather data for "\
                    "day after tomorrow from Bigquery table")
NEW_DATA2.drop_duplicates(['timestamp', 'Location'], keep='last', inplace=True)
NEW_DATA2.reset_index(drop=True, inplace=True)
SHAPE = NEW_DATA2.shape[0]
if SHAPE != 0:
    logging.info('Weather data for day after tomorrow imported')
    pass
else:
    logging.info("Weather data for day after tomorrow not available")

# reading weather data for yesterday
try:
    HIST_DATA = gbq.read_gbq(WS_LOCATION.format(YESTERDAY), project_id=PROJECT_ID)
except:
    raise Exception("Failed to load Weather data for yesterday from Bigquery table")
HIST_DATA.drop_duplicates(['timestamp', 'Location'], keep='last', inplace=True)
HIST_DATA.reset_index(drop=True, inplace=True)
SHAPE = HIST_DATA.shape[0]
if SHAPE != 0:
    logging.info('Weather data for yesterday imported')
    pass
else:
    logging.info("Weather data for yesterday not available")


# reading weather data for day before yesterday
try:
    HIST_DATA1 = gbq.read_gbq(WS_LOCATION.format(YESTERDAY1), project_id=PROJECT_ID)
except:
    raise Exception("Failed to load Weather data for day before yesterday from Bigquery table")
HIST_DATA1.drop_duplicates(['timestamp', 'Location'], keep='last', inplace=True)
HIST_DATA1.reset_index(drop=True, inplace=True)
SHAPE = HIST_DATA1.shape[0]
if SHAPE != 0:
    logging.info('Weather data for day before yesterday imported')
    pass
else:
    logging.info("Weather data for day before yesterday not available")


# appending Historical data of both days together
DF_HIST = HIST_DATA.append(HIST_DATA1)
DF_HIST.reset_index(drop=True, inplace=True)
SHAPE = DF_HIST.shape[0]
if SHAPE != 0:
    pass
else:
    logging.info("Historical weather data not present")

# appending forecast data of both days together
NEW_DATA2 = NEW_DATA2.append(TODAY_DATA)
DF_FORC = NEW_DATA.append(NEW_DATA2)
DF_FORC.reset_index(drop=True, inplace=True)
logging.info('Weather data appended')
SHAPE = DF_FORC.shape[0]
if SHAPE != 0:
    pass
else:
    raise Exception("Forecast weather data not available")

# correcting names of Marker locations
for i in range(len(DF_HIST)):
    DF_HIST['Location'][i] = DF_HIST['Location'][i].replace('IPL_', '')
    DF_HIST['Location'][i] = re.sub(r"([0-9]+(\.[0-9]+)?)",r" \1 ", DF_HIST['Location'][i]).strip()
for i in range(len(DF_FORC)):
    DF_FORC['Location'][i] = DF_FORC['Location'][i].replace('IPL_', '')
    DF_FORC['Location'][i] = re.sub(r"([0-9]+(\.[0-9]+)?)",r" \1 ", DF_FORC['Location'][i]).strip()    
DF_HIST = DF_HIST.drop(['Job_Update_Time', 'timestampInit'], axis=1)
DF_FORC = DF_FORC.drop(['Job_Update_Time', 'timestampInit'], axis=1)

# renaming columns
DF_HIST.rename(columns = {'Latitude':'latitude', 'Longitude' : 'longitude'}, inplace = True)
DF_FORC.rename(columns = {'Latitude':'latitude', 'Longitude' : 'longitude'}, inplace = True)
DF_HIST = DF_HIST.loc[:, ~DF_HIST.columns.str.contains('^Unnamed')]
DF_FORC = DF_FORC.loc[:, ~DF_FORC.columns.str.contains('^Unnamed')]

# breaking and saving the weather data according to days
TODAY_DATE = datetime.now()
FORECAST_NEXT_DATE = (TODAY_DATE + timedelta(days=1)).strftime('%Y-%m-%d')
FORECAST_END_DATE = (TODAY_DATE + timedelta(days=2)).strftime('%Y-%m-%d')
PAST_START_DATE = (TODAY_DATE - timedelta(days=2)).strftime('%Y-%m-%d')
PAST_END_DATE = (TODAY_DATE - timedelta(days=1)).strftime('%Y-%m-%d')
TODAY_DATE = TODAY_DATE.strftime('%Y-%m-%d')

# Creating a list of days for naming the output files
DATE_LIST = [PAST_START_DATE, PAST_END_DATE, TODAY_DATE, FORECAST_NEXT_DATE, FORECAST_END_DATE]
DAY_LIST = ['DAY_B_YDAY', 'YDAY', 'TODAY', 'TOM', 'DAY_A_TOM' ]

# writing the weather data for day before yesterday and yesterday
DF_HIST['timestamp'] = DF_HIST['timestamp'].dt.date
DF_FORC['timestamp'] = DF_FORC['timestamp'].dt.date
logging.info('The location of weather files are :')
for i in range(0, 2):
    temp_df = DF_HIST[DF_HIST['timestamp'].astype(str) == DATE_LIST[i]]
    loc = HIST_OP + '_' + DAY_LIST[i] + '.csv'
    temp_df.to_csv(loc, mode = 'w', index=False)
    logging.info(loc)

# writing the weather data for today, tomorrow and day after tomorrow
for i in range(2, 5):
    temp_df = DF_FORC[DF_FORC['timestamp'].astype(str) == DATE_LIST[i]]
    loc = FORC_OP + '_' + DAY_LIST[i] + '.csv'
    temp_df.to_csv(loc, mode = 'w', index=False)
    logging.info(loc)
