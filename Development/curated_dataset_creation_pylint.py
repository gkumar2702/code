'''fILE TO CREATE CURATED DATASET'''
from datetime import datetime
from datetime import timedelta
import pandas as pd
from pandas.io import gbq
import logging
logging.basicConfig(level=logging.INFO)
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
SC = SparkContext.getOrCreate()
SPARK = SparkSession(SC)
import geopy.distance
import math
# ## **Read OMS Weather Source curated dataset**
BUCKET_NAME = 'gs://aes-analytics-0001-curated/Outage_Restoration/Live_Data_Curation/'
DF_OMSDS = SPARK.read.format('CSV').option("header", "true").option("inferSchema", "true").option("delimiter", ",").load(BUCKET_NAME+'weather-source/OMS_weather-source_Live_Data.csv').toPandas()
DF_OMSDS = DF_OMSDS.loc[:, ~DF_OMSDS.columns.str.contains('^Unnamed')]
DF_OMSDS = DF_OMSDS.loc[:, ~DF_OMSDS.columns.str.contains('^_c0')]
DF_OMSDS['CREATION_DATETIME'] = pd.to_datetime(DF_OMSDS['CREATION_DATETIME'],
                                               errors='coerce')
DF_OMSDS['Date'] = DF_OMSDS['CREATION_DATETIME'].dt.date
logging.info(DF_OMSDS.head())
# ## **Priority Queuing Feature**
# 1. Rank based on simple customer quantity as mentioned by Business
#(live rankings to be followed,  numerical feature)
DF_OMSDS.sort_values(by=['CREATION_DATETIME'], inplace=True)
DF_OMSDS.reset_index(drop=True, inplace=True)
DF_OMSDS['ENERGIZED_DATETIME'] = DF_OMSDS.ENERGIZED_DATETIME.fillna('1900-01-01')
DF_OMSDS['ENERGIZED_DATETIME'] = pd.to_datetime(DF_OMSDS['ENERGIZED_DATETIME'])
DF_OMSDS['ENERGIZED_DATETIME'] = DF_OMSDS['ENERGIZED_DATETIME'].apply(
    lambda row: row.strftime("%Y-%m-%d"))
DF_OMSDS = DF_OMSDS[DF_OMSDS.ENERGIZED_DATETIME == '1900-01-01']
SHAPE = DF_OMSDS.shape[0]
if SHAPE == 0:
    raise Exception('No new Outages,  All outages are already ENERGIZED for')

DF_OMSDS['Priority_Customer_Qty'] = DF_OMSDS['CUST_QTY'].rank(method='dense', ascending=False)
# 2. Rank based on the factor of distance from centroid and customer quantity
#(live rankings to be followed,approach #2,numerical feature)
DF_OMSDS['LIVE_OUTAGE'] = DF_OMSDS.OUTAGE_ID.nunique()
DF_OMSDS['Center_LAT'] = (DF_OMSDS.LAT)/(DF_OMSDS.LIVE_OUTAGE)
DF_OMSDS['Center_LONG'] = (DF_OMSDS.LONG)/(DF_OMSDS.LIVE_OUTAGE)
logging.info(DF_OMSDS.head())

def cal_distance_from_center_lat_long(lat, long, center_lat, center_long):
    '''Takes lat, long, center_lat, center_long as argument and outputs the distance'''
    if math.isnan(lat)|math.isnan(long)|math.isnan(center_lat)|math.isnan(center_long):
        return None
    else:
        coords1 = [lat, long]
        coords2 = [center_lat, center_long]
        return geopy.distance.distance(coords1, coords2).miles

DF_OMSDS['Dis_From_Live_Centriod'] = DF_OMSDS.apply(lambda x: cal_distance_from_center_lat_long(x['LAT'], x['LONG'], x['Center_LAT'], x['Center_LONG']), axis=1)
DF_OMSDS['Dis_From_Live_Centriod'] = DF_OMSDS['Dis_From_Live_Centriod'].apply(pd.to_numeric,
                                                                              errors='coerce')
DF_OMSDS['Dis_From_Live_Centriod_div_Cust_qty'] = (
    DF_OMSDS['Dis_From_Live_Centriod'])/(DF_OMSDS['CUST_QTY'])
DF_OMSDS['Priority_Dist_Customer_Qty'] = DF_OMSDS['Dis_From_Live_Centriod_div_Cust_qty'].rank(
    method='max', ascending=True)
DF_OMSDS.drop(['Center_LAT',
               'Center_LONG', 'Dis_From_Live_Centriod', 'LIVE_OUTAGE'], axis=1, inplace=True)
DF_OMSDS.head()
# ## **Read output dataset and filter for Predicted Flag**
try:
    DF_PRED = 'SELECT OUTAGE_ID FROM aes-analytics-0001.mds_outage_restoration.IPL_Predictions'
    DF_PRED = gbq.read_gbq(DF_PRED, project_id="aes-analytics-0001")
    PREDICTIONS = list(DF_PRED['OUTAGE_ID'].unique())
    DF_OMSDS['OUTAGE_ID'] = DF_OMSDS['OUTAGE_ID'].astype(str)
    DF_OMSDS['OUTAGE_ID'] = DF_OMSDS['OUTAGE_ID'].str.replace(' ', '')
    DF_FINAL = DF_OMSDS[~DF_OMSDS['OUTAGE_ID'].isin(PREDICTIONS)]
    DF_FINAL.reset_index(drop=True, inplace=True)
except:
    DF_FINAL = DF_OMSDS

SHAPE = DF_FINAL.shape[0]
if SHAPE == 0:
    raise Exception('No new Outages,  All outages are already predicted for')

logging.info(DF_FINAL.head())
# ## **Add Number of Outages for CLUE,  CAUSE,  OCCURN**
DF_FINAL['CREATION_DATETIME'] = pd.to_datetime(DF_FINAL['CREATION_DATETIME'])
DF_FINAL['Date'] = DF_FINAL['CREATION_DATETIME'].dt.date

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

logging.info(DF_NO_OF_OUTAGES.head())
try:
    DF_CLUE_COUNT = pd.read_csv('gs://aes-analytics-0001-curated/Outage_Restoration/Staging/OMS_Clue_Flag_Record.csv')
except:
    DF_NO_OF_OUTAGES.to_csv('gs://aes-analytics-0001-curated/Outage_Restoration/Staging/OMS_Clue_Flag_Record.csv', index=False)

RECORD_DATE = (datetime.today()-timedelta(days=1)).date()
logging.info(type(RECORD_DATE))
DF_CLUE_COUNT['Date'] = pd.to_datetime(DF_CLUE_COUNT.Date).dt.date
logging.info(type(DF_CLUE_COUNT.Date[0]))
DF_CLUE_COUNT_CURRENT = DF_CLUE_COUNT[DF_CLUE_COUNT.Date >= RECORD_DATE]
DF_CLUE_COUNT_CURRENT.reset_index(drop=True, inplace=True)
logging.info(DF_CLUE_COUNT_CURRENT.empty)
DF_CLUE_COUNT = DF_CLUE_COUNT.append(DF_NO_OF_OUTAGES)
DF_CLUE_COUNT = DF_CLUE_COUNT.groupby(['Date'], as_index=False).agg({
    'NO_OF_POWER_OUT_CLUE_PER_DAY':'sum',
    'NO_OF_OPEN_DEVICE_CLUE_PER_DAY':'sum',
    'NO_OF_IVR_CLUE_PER_DAY':'sum',
    'NO_OF_ANIMAL_CAUSE_PER_DAY':'sum',
    'NO_OF_WIRE_OCCURN_PER_DAY':'sum'})
logging.info(type(DF_CLUE_COUNT.Date[0]))
logging.info(DF_CLUE_COUNT.head())
DF_CLUE_COUNT.to_csv('gs://aes-analytics-0001-curated/Outage_Restoration/Staging/OMS_Clue_Flag_Record.csv', index=False)
DF_FINAL = DF_FINAL.merge(DF_CLUE_COUNT, how='left', left_on=['Date'], right_on=['Date'])
DF_FINAL.reset_index(drop=True, inplace=True)
logging.info(DF_FINAL.head())
# ## **Change all columns to Flag values**
FLG_LIST = list(DF_FINAL.filter(regex='FLG').columns)
DAY_FLG_LIST = list(DF_FINAL.filter(regex='FLAG').columns)
PRIOR_LIST = list(DF_FINAL.filter(regex='PRIORITY').columns)
FINAL_LIST = FLG_LIST+PRIOR_LIST+DAY_FLG_LIST
MAPIN = {1:'True', 0:'False'}
for i in FINAL_LIST:
    DF_FINAL[i] = DF_FINAL[i].map(MAPIN)

DF_FINAL.fillna(method='ffill', inplace=True)
DF_FINAL['CITY_NAM'].fillna('NO_CITY', inplace=True)
DF_FINAL['CREATION_DATETIME'] = DF_FINAL['CREATION_DATETIME'].apply(
    lambda row: row.strftime("%Y-%m-%d %H:%M:%S"))
DF_FINAL['CREATION_DATETIME'] = pd.to_datetime(DF_FINAL['CREATION_DATETIME'],errors='coerce')
logging.info(DF_FINAL.head())
# ## **Add outages in last N hours feature**
RECORD_DATE_OUTAGE = (datetime.today()-timedelta(days=2)).date()
logging.info(RECORD_DATE_OUTAGE)

try:
    PRED_OUTAGES = 'SELECT OUTAGE_ID, Creation_Time FROM aes-analytics-0001.mds_outage_restoration.IPL_Predictions where creation_time>='+"'"+str(RECORD_DATE_OUTAGE)+"'"
    DF_PRED_OUTAGES = gbq.read_gbq(PRED_OUTAGES, project_id="aes-analytics-0001")
    DF_PRED_OUTAGES.reset_index(drop=True, inplace=True)
except:
    DF_PRED_OUTAGES = pd.DataFrame()

logging.info(DF_PRED_OUTAGES.head())
DF_PRED_OUTAGES['Creation_Time'] = pd.to_datetime(DF_PRED_OUTAGES['Creation_Time'],errors='coerce')
DF_PRED_OUTAGES['Creation_Time'] = DF_PRED_OUTAGES['Creation_Time'].apply(
    lambda row: row.strftime("%Y-%m-%d %H:%M:%S"))
DF_PRED_OUTAGES['CREATION_DATETIME'] = pd.to_datetime(DF_PRED_OUTAGES['Creation_Time'],errors='coerce')
DF_PRED_OUTAGES.drop(['Creation_Time'], axis=1, inplace=True)

DF_FINAL_OUTAGE_COUNT = DF_FINAL[['OUTAGE_ID', 'CREATION_DATETIME']]
DF_FINAL_OUTAGE_COUNT = DF_FINAL_OUTAGE_COUNT.append(DF_PRED_OUTAGES)
DF_FINAL_OUTAGE_COUNT.drop_duplicates(subset='OUTAGE_ID',keep='last',inplace=True)
DF_FINAL_OUTAGE_COUNT.reset_index(inplace=True)

def count_outage_minutes(group):
    '''takes group as argument and outputs group with added features'''
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
    '''takes df as input and gives liveoutage as output'''
    liveoutage = df.groupby(['OUTAGE_ID'], as_index=False).apply(count_outage_minutes)
    return liveoutage

LIVE_OUTAGES = grouping_fn_minutes(DF_FINAL)
LIVE_OUTAGES.reset_index(drop=True, inplace=True)
# ## **Write curated dataset to Big query table**
if 'DOWNSTREAM_CUST_QTY' not in LIVE_OUTAGES:
    LIVE_OUTAGES['DOWNSTREAM_CUST_QTY'] = LIVE_OUTAGES['CUST_QTY']

logging.disable(logging.CRITICAL)
LIVE_OUTAGES['KVA_VAL'] = LIVE_OUTAGES['DOWNSTREAM_KVA_VAL']
LIVE_OUTAGES.fillna(method='ffill', inplace=True)
LIVE_OUTAGES = LIVE_OUTAGES.loc[:, ~LIVE_OUTAGES.columns.str.contains('^Unnamed')]
LIVE_OUTAGES.to_csv("gs://aes-analytics-0001-curated/Outage_Restoration/Staging/IPL_Live_Master_Dataset_ws.csv", index=False)
LIVE_OUTAGES.to_csv("gs://aes-analytics-0001-curated/Outage_Restoration/Historical_Data/BQ_backup/IPL_OMS_LIVE_Data_"+datetime.today().strftime('%Y%m%d%H%M')+".csv", index=False)
