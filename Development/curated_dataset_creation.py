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

from pandas.io import gbq
from datetime import datetime

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


# # **Read OMS Dark-sky curated dataset**

# In[ ]:


bucket_name = 'gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/'

df_omsds=spark.read.format('CSV').option("header","true").option("inferSchema","true").option("delimiter",",").load(
    bucket_name + 'Dark-sky/OMS_Dark-sky_Live_Data.csv').toPandas()
#df_omsds = df_omsds.loc[:, ~df_omsds.columns.str.contains('^Unnamed')]


# # **Read Storm Profiles Data**

# In[ ]:


df_omsds['CREATION_DATETIME'] = pd.to_datetime(df_omsds['CREATION_DATETIME'],errors='coerce')
df_omsds['Date'] = df_omsds['CREATION_DATETIME'].dt.date

unique_dates = df_omsds[['Date']]
unique_dates.drop_duplicates(subset=['Date'], keep='first', inplace=True)
unique_dates['Date'] = unique_dates['Date'].apply(lambda x: x.strftime('%Y%m%d'))
unique = unique_dates['Date'].to_list()
print(unique)


storm_profiles_location = 'gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/Storm_Profiles/'
# storm_profiles_location = 'gs://aes-datahub-0001-curated/Outage_Restoration/Live_Data_Curation/Storm_Profiles/storm_profiles_20200622.csv'
storm_profiles_files = [] 

for i in unique:         
    filename = storm_profiles_location + 'storm_profiles_{}.csv'.format(i)         
    print(filename)         
    storm_profiles_files.append(pd.read_csv(filename))

stormprofiles_df = spark.read.format('CSV').option("header","true").option("inferSchema","true").option("delimiter",",").load(
    storm_profiles_location).toPandas()

stormprofiles_df = pd.concat(storm_profiles_files)
stormprofiles_df.reset_index(drop=True, inplace=True)
stormprofiles_df = stormprofiles_df.loc[:, ~stormprofiles_df.columns.str.contains('^Unnamed')]


# # **Storm Profiles Weather Data Cleaning**

# In[ ]:


stormprofiles_df=stormprofiles_df[['Date', 'Location', 'clusters']]
stormprofiles_df['Date']=pd.to_datetime(stormprofiles_df['Date'])
df_omsds['Date']=pd.to_datetime(df_omsds['Date'])
print(stormprofiles_df.shape)


# In[ ]:


df_omsds['Date'] = pd.to_datetime(df_omsds['Date'])
df_omsds = df_omsds.merge(stormprofiles_df,how='left',left_on=['Date','Marker_Location'],right_on=['Date','Location'])


# # **Change all columns to Flag values**

# In[ ]:


flg_list = list(df_omsds.filter(regex='FLG').columns)
day_flg_list = list(df_omsds.filter(regex='FLAG').columns)
prior_list = list(df_omsds.filter(regex='PRIORITY').columns)
final_list = flg_list + prior_list+day_flg_list
mapin = { 1: 'TRUE', 0: 'FALSE'}
for i in final_list:
    df_omsds[i] = df_omsds[i].map(mapin)


# ## **Read output dataset and filter for Predicted Flag**

# In[ ]:


try:    
    df_pred = 'SELECT OUTAGE_ID FROM aes-analytics-0002.mds_outage_restoration.IPL_LIVE_PREDICTIONS'
    df_pred = gbq.read_gbq(df_pred, project_id = "aes-analytics-0002")
    df_pred['PREDICTED_FLG'] = 1
    df_joined=pd.merge(df_omsds,df_pred,on=['OUTAGE_ID'],how='left')
    df_final=df_joined[pd.isnull(df_joined['PREDICTED_FLG'])][df_omsds.columns]
    
except:
    df_final=df_omsds
# # **Write curated dataset to Big query table**

# In[ ]:
if 'DOWNSTREAM_CUST_QTY' not in df_final:
    df_final['DOWNSTREAM_CUST_QTY']=df_final['CUST_QTY']
    
df_final['KVA_VAL']=df_final['DOWNSTREAM_KVA_VAL']

df_final.to_gbq('mds_outage_restoration.IPL_Live_Master_Dataset', project_id = 'aes-analytics-0002',
                chunksize=None, reauth=False, if_exists='replace', auth_local_webserver=False, table_schema=None,
                location=None, progress_bar=True, credentials=None)


# In[ ]:

# Backup
df_final.to_csv("gs://aes-datahub-0002-curated/Outage_Restoration/Historical_Data/BQ_backup/IPL_OMS_LIVE_Data.csv")


# In[ ]:




