#!/usr/bin/env python
# coding: utf-8

# ## **Import necessary packages**

"""
Load hypertuned Random forest model to predict total time
for restoration
"""

import pickle
import logging
import datetime as dt
from datetime import datetime, date, timedelta
from pytz import timezone
import pandas as pd
import numpy as np
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import gcsfs

logging.basicConfig(level=logging.INFO)
SC = SparkContext.getOrCreate()
SPARK = SparkSession(SC)

# ## **Read OMS Live Cureated Dataset**

BUCKET_NAME = 'gs://aes-analytics-0002-curated/Outage_Restoration/Staging/'

DF_ADS_FINAL = SPARK.read.format('CSV').option("header", "true").option(
    "inferSchema", "true").option("delimiter", ",").load(
        BUCKET_NAME + 'IPL_Live_Master_Dataset_ws.csv').toPandas()

DF_ADS_FINAL = DF_ADS_FINAL.loc[:, ~DF_ADS_FINAL.columns.str.contains('^Unnamed')]
logging.info(DF_ADS_FINAL.shape)
logging.info('\n')
logging.info("No of NA's if any")
logging.info('\n')
logging.info(DF_ADS_FINAL.isnull().values.any())


# ## **Read Storm Profiles Data**

# In[3]:


BUCKET_NAME = 'gs://aes-analytics-0002-curated/Outage_Restoration/Live_Data_Curation'

DF_ADS_FINAL['CREATION_DATETIME'] = pd.to_datetime(
    DF_ADS_FINAL['CREATION_DATETIME'], errors='coerce')
DF_ADS_FINAL['Date'] = DF_ADS_FINAL['CREATION_DATETIME'].dt.date

UNIQUE_DATES = DF_ADS_FINAL[['Date']]
UNIQUE_DATES.drop_duplicates(subset=['Date'], keep='first', inplace=True)
UNIQUE_DATES['Date'] = UNIQUE_DATES['Date'].apply(lambda x: x.strftime('%Y%m%d'))
UNIQUE = UNIQUE_DATES['Date'].to_list()
logging.info(UNIQUE)


STORM_PROFILES_LOCATION = BUCKET_NAME + '/Storm_Profiles/'
logging.info(STORM_PROFILES_LOCATION)
STORM_PROFILES_FILES = []

for i in UNIQUE:
    FILENAME = STORM_PROFILES_LOCATION + 'storm_profiles_{}.csv'.format(i)
    logging.info(FILENAME)
    STORM_PROFILES_FILES.append(SPARK.read.format('CSV').option("header", "true").option(
        "inferSchema", "true").option("delimiter", ",").load(FILENAME).toPandas())

STORMPROFILES_DF = SPARK.read.format('CSV').option("header", "true").option(
    "inferSchema", "true").option("delimiter", ",").load(STORM_PROFILES_LOCATION).toPandas()

STORMPROFILES_DF = pd.concat(STORM_PROFILES_FILES)
STORMPROFILES_DF.reset_index(drop=True, inplace=True)
STORMPROFILES_DF = STORMPROFILES_DF.loc[:, ~STORMPROFILES_DF.columns.str.contains('^Unnamed')]
STORMPROFILES_DF = STORMPROFILES_DF.loc[:, ~STORMPROFILES_DF.columns.str.contains('_c0')]
STORMPROFILES_DF = STORMPROFILES_DF[['timestamp', 'Location', 'clusters']]

STORMPROFILES_DF.rename({'timestamp' : 'Date', 'Location' : 'Marker_Location',
                         'clusters' : 'Cluster_ID'}, axis=1, inplace=True)
logging.info('Pro-processing Storm Info Done')


# In[4]:


def rename_storm_info(row):
    """
    This function maps the clusters to their
    full description

    Function returns corresponding cluster
    details

    Args:
        row - Cluster Number
    """
    if row == 'Cluster1':
        return 'Hot Days with Sudden Rain'
    if row == 'Cluster2':
        return 'Strong Breeze with Sudden Rain'
    if row == 'Cluster3':
        return 'Thunderstorms'
    if row == 'Cluster4':
        return 'Chilly Day with Chances of Snow'
    if row == 'Cluster5':
        return 'Strong Chilled Breeze with Chances of Snow'
    if row == 'Cluster6':
        return 'Hot Days with Chance of Rain'

STORMPROFILES_DF['Cluster_ID'] = STORMPROFILES_DF['Cluster_ID'].apply(rename_storm_info)

DF_ADS_FINAL['Date'] = pd.to_datetime(DF_ADS_FINAL['Date'])
STORMPROFILES_DF['Date'] = pd.to_datetime(STORMPROFILES_DF['Date'])
DF_ADS_FINAL = DF_ADS_FINAL.merge(STORMPROFILES_DF, how='left',
                                  left_on=['Date', 'Marker_Location'],
                                  right_on=['Date', 'Marker_Location'])


# In[5]:


logging.info(list(DF_ADS_FINAL.columns))


# ## **Load Hyper Tuned RF model**

# In[6]:

RF_MODEL = pd.read_pickle(r'gs://aes-analytics-0002-curated/Outage_Restoration/Model_object/Random_Forest_GridSearch_09172020.pkl')

logging.info("Model Loaded")


# In[7]:


BUCKET_NAME = 'gs://aes-analytics-0002-curated/Outage_Restoration/'

FEATURES_DF = SPARK.read.format('CSV').option("header", "true").option(
    "inferSchema", "true").option("delimiter", ",").load(
        BUCKET_NAME + 'Model_object/Random_Forest_GridSearch_09172020.csv').toPandas()

FEATURES_DF = FEATURES_DF.loc[:, ~FEATURES_DF.columns.str.contains('_c0')]
FEATURE_LIST = list(FEATURES_DF.Features_List)
logging.info(FEATURE_LIST)
logging.info("Features Loaded")


# ## **Feature Pre-Processing before it is sent to the Model**

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
DF_ADS_FINAL_V1['PUBLIC_SAFETY_OCCURN_FLG_True'] = \
DF_ADS_FINAL_V1['PUBLIC_SAFETY_OCCURN_FLG'].apply(lambda row: 1 if (row is True) else 0)
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
DF_ADS_FINAL_V1['PUBLIC_SAFETY_OCCURN_FLG_False'] = \
DF_ADS_FINAL_V1['PUBLIC_SAFETY_OCCURN_FLG'].apply(lambda row: 1 if (row is False) else 0)
DF_ADS_FINAL_V1['POWER_OUT_CLUE_FLG_True'] = DF_ADS_FINAL_V1['POWER_OUT_CLUE_FLG'].apply(
    lambda row: 1 if (row is True) else 0)
DF_ADS_FINAL_V1['CITY_NAM_NO_CITY'] = DF_ADS_FINAL_V1['CITY_NAM'].apply(
    lambda row: 1 if (row is 'NO_CITY') else 0)

logging.info("Preprocessing Done")

DF_ADS_FINAL_V1 = DF_ADS_FINAL_V1[FEATURE_LIST]

Y_TEST_PRED = RF_MODEL.predict(DF_ADS_FINAL_V1)
Y_TEST_PRED = np.exp(Y_TEST_PRED)
Y_TEST_PRED = list(Y_TEST_PRED)

DF_ADS_FINAL['Predicted_TTR'] = Y_TEST_PRED

logging.info(Y_TEST_PRED)


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


DF_ADS_FINAL = DF_ADS_FINAL[['OUTAGE_ID', 'INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID',
                             'DNI_EQUIP_TYPE', 'CREATION_DATETIME', 'Predicted_ETR',
                             'Restoration_Period', 'Cluster_ID']]
DF_ADS_FINAL.rename({'CREATION_DATETIME' : 'Creation_Time',
                     'Predicted_ETR' : 'Estimated_Restoration_Time',
                     'Restoration_Period' : 'ETR', 'Cluster_ID' : 'Weather_Profile'}, axis=1, inplace=True)

DF_ADS_FINAL.to_gbq('mds_outage_restoration.IPL_Predictions', project_id='aes-analytics-0002',
                    chunksize=None, reauth=False, if_exists='append', auth_local_webserver=False,
                    table_schema=None, location=None, progress_bar=True, credentials=None)

DF_ADS_FINAL.to_gbq('mds_outage_restoration.IPL_Live_Predictions', project_id='aes-analytics-0002',
                    chunksize=None, reauth=False, if_exists='replace', auth_local_webserver=False,
                    table_schema=None, location=None, progress_bar=True, credentials=None)

DF_ADS_FINAL.to_csv(
    'gs://aes-analytics-0002-curated/Outage_Restoration/OMS/ERT_live/ERT_predictions.csv')

YEAR_MONTH = datetime.now(timezone('US/Eastern')).strftime('%Y-%m')
CURRENT_DATE = datetime.now(timezone('US/Eastern')).strftime('%Y-%m-%d')
CURRENT_DATE_HOUR = datetime.now(timezone('US/Eastern')).strftime('%Y%m%d%H%M')
logging.info(YEAR_MONTH)
logging.info(CURRENT_DATE)
logging.info(CURRENT_DATE_HOUR)
logging.disable(logging.CRITICAL)

FILENAME = 'gs://aes-analytics-0002-curated/Outage_Restoration/OMS/Deliverables/ERTs/{}/{}/TTR_predictions_{}.csv'.format(YEAR_MONTH, CURRENT_DATE, CURRENT_DATE_HOUR)
logging.info(FILENAME)

DF_ADS_FINAL.to_csv(FILENAME, index=False)
