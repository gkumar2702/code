#!/usr/bin/env python
# coding: utf-8

# # **Import Required packages**

# In[ ]:


import os
import math
import warnings
import operator
import pandas as pd
import numpy as np
import datetime as dt

from datetime import date, timedelta
from datetime import datetime
from google.cloud import storage

from pandas.io import gbq
from datetime import date, timedelta
from datetime import datetime
from google.cloud import storage
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')
pd.options.mode.chained_assignment = None  # default='warn'


import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# # **Read curated OMS data**

# In[ ]:


df_oms_live=spark.read.format('CSV').option("header","true").option("inferSchema","true").option("delimiter",",").load(
    'gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/OMS/OMS_Live_Data.csv').toPandas()

df_oms_live = df_oms_live.loc[:, ~df_oms_live.columns.str.contains('^Unnamed')]


# # **Dynamic reading of files from darksky**

# In[ ]:


df_oms_live['CREATION_DATETIME'] = pd.to_datetime(df_oms_live['CREATION_DATETIME'],errors='coerce',utc=True)
df_oms_live['Date'] = df_oms_live['CREATION_DATETIME'].dt.date

unique_dates = df_oms_live[['Date']]
unique_dates.drop_duplicates(subset=['Date'], keep='first', inplace=True)
unique_dates['Date'] = unique_dates['Date'].apply(lambda x: x.strftime('%Y%m%d'))
unique = unique_dates['Date'].to_list()
print(unique)

ws_location = 'gs://aes-datahub-0002-raw/Weather/weather_source/USA/Indianapolis/'
wsfiles = []

for i in unique:
    year_month = pd.to_datetime(i).strftime('%Y-%m')
    current_date= pd.to_datetime(i).strftime('%Y-%m-%d')
    filename = ws_location+year_month+'/forecast_data/'+current_date+'/weathersource_daily_{}.csv'.format(i)
    print(filename)         
    wsfiles.append(pd.read_csv(filename))

ws_df = pd.concat(wsfiles)
ws_df.reset_index(drop=True, inplace=True)


# # **Darksky weather data cleaning**

# In[ ]:


# ws_df=ws_df[['Date', 'Location', 'cloudCover', 'dewPoint', 'humidity', 'icon','precipIntensity',
                           # 'precipIntensityMax', 'precipType', 'pressure', 'temperatureMax', 'temperatureMin',
                           # 'visibility', 'windBearing', 'windGust', 'windSpeed']]


ws_df['Date']=pd.to_datetime(ws_df['timestamp']).dt.date
ws_df['Location']=ws_df['Location'].astype(str)

print(ws_df.shape)


# # **Darksky OMS weather mapping**

# In[ ]:


df_oms_live['Date'] = pd.to_datetime(df_oms_live['Date'],errors='coerce')
ws_df['Date'] = pd.to_datetime(ws_df['Date'],errors='coerce')

print(df_oms_live.shape)
print(ws_df.shape)

df_oms_live = pd.merge(df_oms_live, ws_df,how='left',left_on=['Date','Marker_Location'],right_on=['Date','Location'])
df_oms_live.drop(['Date'],axis=1,inplace=True)
print(df_oms_live.shape)


# # **Renaming Darksky weather attributes**

# In[ ]:


# df_oms_live.rename(columns = {'cloudCover' : 'CLOUDCOVER','dewPoint' : 'DEWPOINT','humidity' : 'HUMIDITY','icon' : 'ICON','precipIntensity' : 'PRECIPINTENSITY',
                              # 'precipIntensityMax' : 'PRECIPINTENSITYMAX','precipType' : 'PRECIPTYPE','pressure' : 'PRESSURE','temperatureMax' : 'TEMPERATUREMAX',
                              # 'temperatureMin' : 'TEMPERATUREMIN','visibility' : 'VISIBILITY','windBearing' : 'WINDBEARING','windGust' : 'WINDGUST',
                              # 'windSpeed' : 'WINDSPEED'}, inplace=True)


# # **Create wind Direction and season flags**

# In[ ]:



# df_oms_live['WIND_DIRECTION'] [(df_oms_live['WINDBEARING']>=1)&(df_oms_live['WINDBEARING']<45)]='N-E-N'
# df_oms_live['WIND_DIRECTION'][(df_oms_live['WINDBEARING']>=45)&(df_oms_live['WINDBEARING']<90)]='N-E-E'
# df_oms_live['WIND_DIRECTION'][(df_oms_live['WINDBEARING']>=90)&(df_oms_live['WINDBEARING']<135)]='S-E-E'
# df_oms_live['WIND_DIRECTION'][(df_oms_live['WINDBEARING']>=135)&(df_oms_live['WINDBEARING']<180)]='S-E-S'
# df_oms_live['WIND_DIRECTION'][(df_oms_live['WINDBEARING']>=180)&(df_oms_live['WINDBEARING']<225)]='S-W-S'
# df_oms_live['WIND_DIRECTION'][(df_oms_live['WINDBEARING']>=225)&(df_oms_live['WINDBEARING']<270)]='S-W-W'
# df_oms_live['WIND_DIRECTION'][(df_oms_live['WINDBEARING']>=270)&(df_oms_live['WINDBEARING']<315)]='N-W-W'
# df_oms_live['WIND_DIRECTION'][(df_oms_live['WINDBEARING']>=315)&(df_oms_live['WINDBEARING']<360)]='N-W-N'

# df_oms_live['WIND_DIRECTION'] = df_incidentdevicelocation_['WINDBEARING'].apply(lambda x: 'N-E-N' if (x >= 1)&(x < 45) else 0)

def create_wind_direction(x):
  if ((x>=1) & (x<45)):
      direction = 'N-E-N'
  elif ((x>=45) & (x<90)):
      direction = 'N-E-E'
  elif ((x>=90) & (x<180)):
      direction = 'S-E-E'
  elif ((x>=135) & (x<180)):
      direction = 'S-E-S'
  elif ((x>=180) & (x<225)):
      direction = 'S-W-S'
  elif ((x>=225) & (x<270)):
      direction = 'S-W-W' 
  elif ((x>=270) & (x<315)):
      direction = 'N-W-W'
  elif ((x>=315) & (x<360)):
      direction = 'N-W-N'
  else :
      direction = None
  
  return direction

#df_oms_live['WINDBEARING'] = df_oms_live['WINDBEARING'].apply(pd.to_numeric, errors='coerce')
df_oms_live['WIND_DIRECTION'] = df_oms_live['windDirAvg'].apply(create_wind_direction)


df_oms_live['MONTH']=pd.to_datetime(df_oms_live['CREATION_DATETIME']).dt.month
df_oms_live['SEASON']='NONE'
df_oms_live['SEASON'][((df_oms_live['MONTH']>=1)&(df_oms_live['MONTH']<=3))|(df_oms_live['MONTH']==12)]='WINTER'
df_oms_live['SEASON'][(df_oms_live['MONTH']>=4)&(df_oms_live['MONTH']<=6)]='SPRING'
df_oms_live['SEASON'][(df_oms_live['MONTH']>=7)&(df_oms_live['MONTH']<=9)]='SUMMER'
df_oms_live['SEASON'][(df_oms_live['MONTH']>=10)&(df_oms_live['MONTH']<=11)]='FALL'

df_oms_live.drop(['MONTH'],axis=1,inplace=True)

def create_weekend_flag(x):
  if (x>=5):
      flag = 1
  else :
      flag=0
  return flag

df_oms_live['weekday']=pd.to_datetime(df_oms_live['CREATION_DATETIME']).dt.dayofweek
df_oms_live['weekend_flag']= df_oms_live['weekday'].apply(create_weekend_flag)

df_oms_live.drop(['weekday'],axis=1,inplace=True)

# # **Write OMS Live and Darksky Dataset Curated**

# In[ ]:
df_oms_live.drop(['Location','latitude','longitude'],axis=1,inplace=True)
df_oms_live.drop(['_c0','_c1'],axis=1,inplace=True)

df_oms_live.to_csv('gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/weather-source/OMS_weather-source_Live_Data.csv',index=False)
