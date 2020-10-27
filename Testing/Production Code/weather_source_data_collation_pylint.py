# ## **Import Required packages**

'''
This scripts collects data from Weather SOurce Locations and ADDS it to OMS LIVE PREPROCESSED DATA
'''

from datetime import date, datetime
import datetime as dt
import logging
import pandas as pd
import numpy as np
from pandas.io import gbq
from google.cloud import storage
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, SparkSession


SC = SparkContext.getOrCreate()
SPARK = SparkSession(SC)

logging.basicConfig(level=logging.INFO)


# ## **Read curated OMS data**

# In[15]:


DF_OMS_LIVE = SPARK.read.format('CSV').option("header", "true").option("inferSchema", "true").option("delimiter", ",").load('gs://aes-analytics-0002-curated/Outage_Restoration/Live_Data_Curation/OMS/OMS_Live_Data.csv').toPandas()
DF_OMS_LIVE = DF_OMS_LIVE.loc[:, ~DF_OMS_LIVE.columns.str.contains('^Unnamed')]


# ## **Dynamic reading of files from Weather Source Storage Location**

# In[16]:


DF_OMS_LIVE['CREATION_DATETIME'] = pd.to_datetime(
    DF_OMS_LIVE['CREATION_DATETIME'], errors='coerce', utc=True)
DF_OMS_LIVE['Date'] = DF_OMS_LIVE['CREATION_DATETIME'].dt.date

UNIQUE_DATES = DF_OMS_LIVE[['Date']]
UNIQUE_DATES.drop_duplicates(subset=['Date'], keep='first', inplace=True)
UNIQUE_DATES['Date'] = UNIQUE_DATES['Date'].apply(lambda x: x.strftime('%Y%m%d'))
UNIQUE = UNIQUE_DATES['Date'].to_list()
logging.info(UNIQUE)

WS_LOCATION = 'gs://aes-datahub-0002-raw/Weather/weather_source/USA/Indianapolis/'
WSFILES = []

for i in UNIQUE:
    year_month = pd.to_datetime(i).strftime('%Y-%m')
    current_date = pd.to_datetime(i).strftime('%Y-%m-%d')
    filename = WS_LOCATION+year_month+'/forecast_data/'+current_date+'/weathersource_daily_{}.csv'.format(i)
    logging.info(filename)
    WSFILES.append(SPARK.read.format('CSV').option("header", "true").option("inferSchema", "true").option("delimiter", ", ").load(
        filename).toPandas())

WS_DF = pd.concat(WSFILES)
WS_DF.reset_index(drop=True, inplace=True)

WS_DF = WS_DF.loc[:, ~WS_DF.columns.str.contains('^Unnamed')]

# ## **Weather Source Weather data cleaning**

WS_DF['Date'] = pd.to_datetime(WS_DF['timestamp']).dt.date
WS_DF['Location'] = WS_DF['Location'].astype(str)

logging.info(WS_DF.shape)

# ## **Add Weather Source features**

## Add range for columns with negative values

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

logging.info(WS_DF.head(2))

# ## **Weather Source OMS weather mapping**

DF_OMS_LIVE['Date'] = pd.to_datetime(DF_OMS_LIVE['Date'], errors='coerce')
WS_DF['Date'] = pd.to_datetime(WS_DF['Date'], errors='coerce')
WS_DF.drop(['latitude', 'longitude', 'timestamp', 'timestampInit'], axis=1, inplace=True)

logging.info(DF_OMS_LIVE.shape)
logging.info(WS_DF.shape)

DF_OMS_LIVE = pd.merge(DF_OMS_LIVE, WS_DF, how='left',
                       left_on=['Date', 'Marker_Location'], right_on=['Date', 'Location'])
DF_OMS_LIVE.drop(['Date'], axis=1, inplace=True)
logging.info(DF_OMS_LIVE.shape)


# ## **Renaming weather attributes**

def create_wind_direction(x_wind_direction):
    '''
Input - Wind direction columns
Output - Wind direction classes
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
	Input - Weekday Number
	Output - WEEKEND Flag
    '''
    if x_flag >= 5:
        flag = 1
    else:
        flag = 0
    return flag

DF_OMS_LIVE['weekday'] = pd.to_datetime(DF_OMS_LIVE['CREATION_DATETIME']).dt.dayofweek
DF_OMS_LIVE['weekend_flag'] = DF_OMS_LIVE['weekday'].apply(create_weekend_flag)

DF_OMS_LIVE.drop(['weekday'], axis=1, inplace=True)

# ## **Write OMS Live and added Weather Source Dataset for Curated Processing**

logging.disable(logging.CRITICAL)
DF_OMS_LIVE.drop(['Location'], axis=1, inplace=True)
DF_OMS_LIVE.to_csv('gs://aes-analytics-0002-curated/Outage_Restoration/Live_Data_Curation/weather'\
                   '-source/OMS_weather-source_Live_Data.csv', index=False)
