
# ## **Import Required Packages**

'''
This code is used to take live data from OMS (Outage Manament System)
And create analytical dataset
'''

import os
import math
import logging
import warnings
import operator
import time
import datetime as dt
from datetime import date, timedelta, datetime
import pandas as pd
import numpy as np
from pandas.io import gbq
from google.cloud import storage

import geopy.distance
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, SparkSession

logging.basicConfig(level=logging.INFO)

SC = SparkContext.getOrCreate()
SPARK = SparkSession(SC)


#  ## **Check all Live Files present in Bucket**
time.sleep(120)
CURRENT_DATE = datetime.today().strftime('%Y-%m-%d')
logging.info(CURRENT_DATE)
CLIENT = storage.Client()
BUCKET_NAME = 'aes-datahub-0002-raw'
BUCKET = CLIENT.get_bucket(BUCKET_NAME)

BLOBS = BUCKET.list_blobs(prefix='OMS/'+CURRENT_DATE)
DIRLIST = []

for blob in BLOBS:
    DIRLIST.append(str(blob.name))

#string matching to read tables
_MATCHING_INCIDENT = [s for s in DIRLIST if "INCIDENT_IPL" in s]
_MATCHING_LIVE_INCIDENT = [s for s in _MATCHING_INCIDENT if "HIS" not in s]
logging.info(_MATCHING_LIVE_INCIDENT)
logging.info('\n')

_MATCHING_INCIDENT_DEVICE = [s for s in DIRLIST if "INCIDENT_DEVICE_IPL" in s]
_MATCHING_LIVE_INCIDENT_DEVICE = [s for s in _MATCHING_INCIDENT_DEVICE if "HIS" not in s]
logging.info(_MATCHING_LIVE_INCIDENT_DEVICE)
logging.info('\n')

_MATCHING_LOCATION = [s for s in DIRLIST if "LOCATION_IPL" in s]
_MATCHING_LIVE_LOCATION = [s for s in _MATCHING_LOCATION if "HIS" not in s]
logging.info(_MATCHING_LIVE_LOCATION)


# ## **Read Live Files in Buckets**

BUCKET_NAME = 'gs://aes-datahub-0002-raw/'

logging.info(CURRENT_DATE)
logging.info('\n')

LIVE_INCIDENT_DEVICE = SPARK.read.format('CSV').option(
    "header", "true").option("inferSchema", "true").option("delimiter", ",").load(
        BUCKET_NAME+_MATCHING_LIVE_INCIDENT_DEVICE[-1]).toPandas()
logging.info(BUCKET_NAME+_MATCHING_LIVE_INCIDENT_DEVICE[-1])

LIVE_INCIDENT = SPARK.read.format('CSV').option(
    "header", "true").option("inferSchema", "true").option("delimiter", ",").load(
        BUCKET_NAME+_MATCHING_LIVE_INCIDENT[-1]).toPandas()
logging.info(BUCKET_NAME+_MATCHING_LIVE_INCIDENT[-1])

LIVE_LOCATION = SPARK.read.format('CSV').option(
    "header", "true").option("inferSchema", "true").option("delimiter", ",").load(
        BUCKET_NAME+_MATCHING_LIVE_LOCATION[-1]).toPandas()
logging.info(BUCKET_NAME+_MATCHING_LIVE_LOCATION[-1])

logging.info('\n')

FILE_READ_LIST = [BUCKET_NAME+_MATCHING_LIVE_INCIDENT_DEVICE[-1],
                  BUCKET_NAME+_MATCHING_LIVE_INCIDENT[-1], BUCKET_NAME+_MATCHING_LIVE_LOCATION[-1]]
CURRENT_FILE_READ = pd.DataFrame({'Filepath' : FILE_READ_LIST})


try:
    LAST_FILE_READ = pd.read_csv(
	'gs://aes-analytics-0002-curated/Outage_Restoration/Staging/Last_OMS_File.csv')
except:
    LAST_FILE_READ = pd.DataFrame()
    CURRENT_FILE_READ.to_csv(
	'gs://aes-analytics-0002-curated/Outage_Restoration/Staging/Last_OMS_File.csv', index=False)

if LAST_FILE_READ.empty:
    print("New Files Path's have been stored")
else:
    if ((CURRENT_FILE_READ.Filepath[0] == LAST_FILE_READ.Filepath[0]) and (
	CURRENT_FILE_READ.Filepath[1] == LAST_FILE_READ.Filepath[1]) and (
	    CURRENT_FILE_READ.Filepath[2] == LAST_FILE_READ.Filepath[2])):

        raise Exception('No new input data files from OMS')

CURRENT_FILE_READ.to_csv('gs://aes-analytics-0002-curated/Outage_Restoration/Staging/Last_OMS_File.csv', index=False)
## QC checks
logging.info("****QC Check****")
logging.info("Shape of Live Incident Device Table")
logging.info("%s", LIVE_INCIDENT_DEVICE.shape)
logging.info("\n")

SHAPE = LIVE_INCIDENT_DEVICE.shape[0]
if SHAPE == 0:
    raise Exception('Live Incident device table contains 0 rows')

logging.info("****QC Check****")
logging.info("Shape of Live Location Table")
logging.info(LIVE_LOCATION.shape)
logging.info("\n")

SHAPE = LIVE_LOCATION.shape[0]
if SHAPE == 0:
    raise Exception('Live location table contains 0 rows')

logging.info("****QC Check****")
logging.info("Shape of Live Incident Table")
logging.info(LIVE_INCIDENT.shape)
logging.info("\n")
SHAPE = LIVE_INCIDENT.shape[0]
if SHAPE == 0:
    raise Exception('Live incident contains 0 rows')

logging.info("****QC Check****")
logging.info("No of Distinct INCIDENT_ID in INCIDENT_DEVICE Table")
logging.info(LIVE_INCIDENT_DEVICE.INCIDENT_ID.nunique())
logging.info("\n")
logging.info("****QC Check****")
logging.info("No of Distinct INCIDENT_ID in LOCATION Table")
logging.info(LIVE_LOCATION.INCIDENT_ID.nunique())
logging.info("\n")
logging.info("****QC Check****")
logging.info("No of Distinct INCIDENT_ID in INCIDENT Table")
logging.info(LIVE_INCIDENT.INCIDENT_ID.nunique())
logging.info("\n")


# ## **Merge Files and Perform Data QC checks**
# merge INCIDENT_DEVICE_ID and LOCATION table

DF_INCIDENT_DEVICE_ = LIVE_INCIDENT_DEVICE.copy(deep=True)
DF_LOCATION_ = LIVE_LOCATION.copy(deep=True)
DF_INCIDENT_ = LIVE_INCIDENT.copy(deep=True)


# subset location tables to get required columns for analysis
DF_LOCATION_SUBSET = DF_LOCATION_[['INCIDENT_ID', 'LOCATION_ID', 'MAJ_OTG_ID',
                                   'CITY_NAM', 'OCCURN_CD', 'CAUSE_CD', 'ENERGIZED_DATETIME']]

# data quality qc
logging.info("****QC Check****")
logging.info("INCIDENT_DEVICE table before and after dropping duplicates at INCIDENT_ID, LOCATION_ID")
logging.info("%s %s", len(DF_INCIDENT_DEVICE_[['INCIDENT_ID', 'LOCATION_ID']]),
             len(DF_INCIDENT_DEVICE_[['INCIDENT_ID', 'LOCATION_ID']].drop_duplicates()))
logging.info("\n")
logging.info("****QC Check****")
logging.info("LOCATION table before and after dropping duplicates at INCIDENT_ID, LOCATION_ID")
logging.info("%s %s", len(DF_LOCATION_SUBSET[['INCIDENT_ID', 'LOCATION_ID']]),
             len(DF_LOCATION_SUBSET[['INCIDENT_ID', 'LOCATION_ID']].drop_duplicates()))
logging.info("\n")

DF_INCIDENTDEVICELOCATION_ = pd.merge(DF_INCIDENT_DEVICE_, DF_LOCATION_SUBSET,
                                      on=['INCIDENT_ID', 'LOCATION_ID'], how='left')

logging.info("****QC Check****")
logging.info("INICDENT_DEVICE, LOCATION table merged before and after dropping duplicates at INCIDENT_ID, LOCATION_ID")
logging.info("%s %s", len(DF_INCIDENTDEVICELOCATION_[['INCIDENT_ID', 'LOCATION_ID']]),
             len(DF_INCIDENTDEVICELOCATION_[['INCIDENT_ID', 'LOCATION_ID']].drop_duplicates()))
logging.info("\n")

SHAPE = DF_INCIDENTDEVICELOCATION_.shape[0]
if SHAPE == 0:
    raise Exception('Incident and device location merge contains 0 rows')


# ## **Apply Required Filters**

# customer quantity greater than 0
logging.info('Filter for customer quantity greater than 0')
logging.info("****QC Check****")
logging.info("Rows left after checking for INCIDENTS whose CUSTOMER QUANTITY IS > 0")
DF_INCIDENTDEVICELOCATION_ = DF_INCIDENTDEVICELOCATION_[(DF_INCIDENTDEVICELOCATION_.DOWNSTREAM_CUST_QTY > 0)]
logging.info(DF_INCIDENTDEVICELOCATION_.shape)
logging.info("\n")

SHAPE = DF_INCIDENTDEVICELOCATION_.shape[0]
if SHAPE == 0:
    raise Exception('Incident and device location merge contains 0 rows after CUST_QTY filter')

# equip_stn_no is not NCC and not null
logging.info('Filter for equp_stn_no is not NCC or not null')
logging.info("****QC Check****")
logging.info("Rows left after checking that EQUIP_STN_NO is not from <<NON CONNECTED CUSTOMERS>>")
DF_INCIDENTDEVICELOCATION_ = DF_INCIDENTDEVICELOCATION_[(DF_INCIDENTDEVICELOCATION_.EQUIP_STN_NO != '<NCC>') &
                                                        (DF_INCIDENTDEVICELOCATION_.EQUIP_STN_NO.notnull())]
logging.info(DF_INCIDENTDEVICELOCATION_.shape)
logging.info("\n")

SHAPE = DF_INCIDENTDEVICELOCATION_.shape[0]
if SHAPE == 0:
    raise Exception('Incident and device location merge contains 0 rows after EQUIP_STN_NO filter')

# removing NAN from DNI_EQUIP_TYPE, CIRCT_ID, STRCTUR_NO
logging.info('Removing NAN from DNI_EQIP_TYPE, CICRT_ID, STRCTUR_NO')
logging.info("****QC Check****")
logging.info("Rows left after checking CIRCT_ID is not 0 and not null, STRCTUR_NO is not null and DNI_EQIP_TYPE is not null")
DF_INCIDENTDEVICELOCATION_ = DF_INCIDENTDEVICELOCATION_[(DF_INCIDENTDEVICELOCATION_.CIRCT_ID != 0)]
DF_INCIDENTDEVICELOCATION_ = DF_INCIDENTDEVICELOCATION_[~DF_INCIDENTDEVICELOCATION_.CIRCT_ID.isnull()]
DF_INCIDENTDEVICELOCATION_ = DF_INCIDENTDEVICELOCATION_[~DF_INCIDENTDEVICELOCATION_.STRCTUR_NO.isnull()]
DF_INCIDENTDEVICELOCATION_ = DF_INCIDENTDEVICELOCATION_[~DF_INCIDENTDEVICELOCATION_.DNI_EQUIP_TYPE.isnull()]
logging.info(DF_INCIDENTDEVICELOCATION_.shape)
logging.info("\n")

SHAPE = DF_INCIDENTDEVICELOCATION_.shape[0]
if SHAPE == 0:
    raise Exception('Incident and device location merge contains 0 rows after ID filter')

# removing CLUE_CD which start with 0 but does not start with 00
logging.info('Removing CLUE_CD which start with 0 but do not start with 00')
logging.info("****QC Check****")
logging.info("Rows left after filtering for CLUE CODES which start with 0 but do not start with 00")
DF_INCIDENTDEVICELOCATION_ = DF_INCIDENTDEVICELOCATION_[(DF_INCIDENTDEVICELOCATION_.CLUE_CD.str[:1] == '0') &
                                                        (DF_INCIDENTDEVICELOCATION_.CLUE_CD.str[:2] != '00')]
DF_INCIDENTDEVICELOCATION_ = DF_INCIDENTDEVICELOCATION_[DF_INCIDENTDEVICELOCATION_.CLUE_CD != '01']
print(DF_INCIDENTDEVICELOCATION_.shape)
logging.info("\n")

SHAPE = DF_INCIDENTDEVICELOCATION_.shape[0]
if SHAPE == 0:
    raise Exception('Incident and device location merge contains 0 rows after Clue filter')

# removing occurence codes starting with cancel, found ok and duplicate
logging.info('Removing CLUE_CD which start with 0 but do not start with 00')
logging.info("****QC Check****")
logging.info("Rows left after removing OCCURN_CD which have descriptions starting with CANCEL, FOUND OK or DUPLICATE")
OCCUR_REMOV = [30003001, 33003301, 33003302, 34003400, 34003401, 34003402, 34003403, 34003404, 34003405, 34003406, 34003407, 34003408, 34003409, 35003500,
               35003501, 35003502, 35003503, 35003504, 35003505, 35003506, 35003507, 35003508, 36003600, 36003601, 36003602, 36003603, 36003604, 36003605,
               36003606, 36003607, 36003608, 37003703, 38003802, 38003803, 38003804, 38003807, 39003910, 41004100, 41004101, 41004102, 48004800, 48004802,
               48004803, 49004900, 49004901, 49004902, 50005000, 50005001, 50005002, 52005200, 52005201, 52005202, 52005203, 52005204, 52005205, 52005206,
               52005207, 53005300, 53005301, 53005302, 53005303, 53005304, 53005305, 53005306, 53005307, 53005308, 53005309, 53005310, 54005400, 54005401,
               54005402, 54005403, 54005404, 54005405, 34003410, 30003000, 36503650, 36503651, 36503652, 36503653, 36503654, 36503655, 36503656, 36503657,
               36503658]
DF_INCIDENTDEVICELOCATION_ = DF_INCIDENTDEVICELOCATION_[~(DF_INCIDENTDEVICELOCATION_.OCCURN_CD.isin(OCCUR_REMOV))]
logging.info(DF_INCIDENTDEVICELOCATION_.shape)
logging.info("\n")

SHAPE = DF_INCIDENTDEVICELOCATION_.shape[0]
if SHAPE == 0:
    raise Exception('ADS contains 0 rows after OCCURN_CD filter')


# ## **Aggregate Numerical Columns**
## START ADS CREATION FOR NUMERICAL COLUMNS AT INCIDENT LEVEL

# 1.1 Aggregate numerical columns at INCIDENT_ID level to keep all unique INCIDNET_ID's
DF_NUMERICAL = DF_INCIDENTDEVICELOCATION_.groupby(['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE'], as_index=False).agg({'CALL_QTY' : 'sum',
    'DOWNSTREAM_CUST_QTY' : 'sum', 'KVA_VAL' : 'max', 'DOWNSTREAM_KVA_VAL' : 'max', 'INCIDENT_DEVICE_ID' : 'max', 'CREATION_DATETIME' : 'min',
    'SUBST_ID' : 'min', 'LOCATION_ID' : 'max', 'ENERGIZED_DATETIME' : 'max'})

DF_NUMERICAL.rename(columns={'DOWNSTREAM_CUST_QTY' : 'CUST_QTY'}, inplace=True)

DF_NUMERICAL['INCIDENT_ID'] = DF_NUMERICAL['INCIDENT_ID'].astype(np.int64)
DF_NUMERICAL['CIRCT_ID'] = DF_NUMERICAL['CIRCT_ID'].astype(np.int64)

DF_NUMERICAL['OUTAGE_ID'] = DF_NUMERICAL.apply(lambda x: '%s%s%s%s' % (x['INCIDENT_ID'],
                                                                       x['STRCTUR_NO'],
                                                                       x['CIRCT_ID'],
                                                                       x['DNI_EQUIP_TYPE']),
                                                                       axis=1)

logging.info("****QC Check****")
logging.info("Shape of Numerical columns at 'INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE' Level")
logging.info(DF_NUMERICAL.shape)
logging.info('\n')

SHAPE = DF_NUMERICAL.shape[0]
if SHAPE == 0:
    raise Exception('ADS contains 0 rows after OCCURN_CD filter')


# ## **Create Day and Night Flags**
# 1.2 Create Day and Night Flags from CREATION_DATETIME columns

DF_NUMERICAL['CREATION_DATETIME'] = pd.to_datetime(DF_NUMERICAL['CREATION_DATETIME'], errors='coerce')
DF_NUMERICAL['ENERGIZED_DATETIME'] = pd.to_datetime(DF_NUMERICAL['ENERGIZED_DATETIME'], errors='coerce')

DF_NUMERICAL['DAY_FLAG'] = DF_NUMERICAL.CREATION_DATETIME.dt.hour.apply(lambda x: 1 if ((x >= 6) & (x < 18)) else 0)
logging.info(DF_NUMERICAL.head())


# ## **City, Priority Treatment**
DF_INCIDENTDEVICELOCATION_['PRIORITY_VAL_1.0'] = DF_INCIDENTDEVICELOCATION_['PRIORITY_VAL'].apply(lambda x: 1 if x == 1 else 0)
DF_INCIDENTDEVICELOCATION_['PRIORITY_VAL_2.0'] = DF_INCIDENTDEVICELOCATION_['PRIORITY_VAL'].apply(lambda x: 1 if x == 2 else 0)
DF_INCIDENTDEVICELOCATION_['PRIORITY_VAL_3.0'] = DF_INCIDENTDEVICELOCATION_['PRIORITY_VAL'].apply(lambda x: 1 if x == 3 else 0)
DF_INCIDENTDEVICELOCATION_['PRIORITY_VAL_5.0'] = DF_INCIDENTDEVICELOCATION_['PRIORITY_VAL'].apply(lambda x: 1 if x == 5 else 0)

DF_INCIDENTDEVICELOCATION_.drop(['PRIORITY_VAL'], axis=1, inplace=True)

DF_INCIDENTDEVICELOCATION_.CITY_NAM = DF_INCIDENTDEVICELOCATION_.CITY_NAM.apply(
    lambda x: 'INDIANAPOLIS' if(str(x).find('INDIAN') != -1) else x)
DF_INCIDENTDEVICELOCATION_.CITY_NAM = DF_INCIDENTDEVICELOCATION_.CITY_NAM.apply(
    lambda x: 'NO_CITY' if(x != x) else x)

# city treatment
def cat_city_treat(group):
    '''
	Input - Grouped CITY_NAME
	Output - SIngle CITY_NAME
    '''
    if group.CITY_NAM.nunique() > 1:
        x = group[group.CITY_NAM != 'NO_CITY'].CITY_NAM.unique()
        group.CITY_NAM = x[1]
        return group
    else:
        return group

DF_TREATED = DF_INCIDENTDEVICELOCATION_[['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE', 'CITY_NAM']]
DF_TREATED = DF_TREATED.groupby(['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE'], as_index=False).apply(cat_city_treat)
DF_TREATED.drop_duplicates(subset=['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE'], ignore_index=True, inplace=True)


# ## **Cause, Clue, Occurn Mapping**
# cause, occurn, clue mapping files

CLUEMAPPING = SPARK.read.format('CSV').option("header", "true").option("inferSchema", "true").option("delimiter", ",").load(
    'gs://aes-analytics-0002-curated/Outage_Restoration/Live_Data_Curation/Mapping_Tables/cluemapping.csv').toPandas()
OCCURNMAPPING = SPARK.read.format('CSV').option("header", "true").option("inferSchema", "true").option("delimiter", ",").load(
    'gs://aes-analytics-0002-curated/Outage_Restoration/Live_Data_Curation/Mapping_Tables/occurnmapping.csv').toPandas()
CAUSEMAPPING = SPARK.read.format('CSV').option("header", "true").option("inferSchema", "true").option("delimiter", ",").load(
    'gs://aes-analytics-0002-curated/Outage_Restoration/Live_Data_Curation/Mapping_Tables/causemapping.csv').toPandas()

DF_INCIDENTDEVICELOCATION_ = pd.merge(DF_INCIDENTDEVICELOCATION_, CLUEMAPPING, on=['CLUE_CD'], how='left')
DF_INCIDENTDEVICELOCATION_ = pd.merge(DF_INCIDENTDEVICELOCATION_, OCCURNMAPPING, on=['OCCURN_CD'], how='left')
DF_INCIDENTDEVICELOCATION_ = pd.merge(DF_INCIDENTDEVICELOCATION_, CAUSEMAPPING, on=['CAUSE_CD'], how='left')

DF_INCIDENTDEVICELOCATION_["CLUE_DESC"] = DF_INCIDENTDEVICELOCATION_["CLUE_DESC"].astype(str)
DF_INCIDENTDEVICELOCATION_["CAUSE_DESC"] = DF_INCIDENTDEVICELOCATION_["CAUSE_DESC"].astype(str)
DF_INCIDENTDEVICELOCATION_["OCCURN_DESC"] = DF_INCIDENTDEVICELOCATION_["OCCURN_DESC"].astype(str)

# segregation of clue code desc
DF_INCIDENTDEVICELOCATION_['POLE_CLUE_FLG'] = DF_INCIDENTDEVICELOCATION_.CLUE_DESC.apply(
    lambda x: 1 if (x.lower().find('pole') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['PART_LIGHT_CLUE_FLG'] = DF_INCIDENTDEVICELOCATION_.CLUE_DESC.apply(
    lambda x: 1 if (x.lower().find('part lights') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['EMERGENCY_CLUE_FLG'] = DF_INCIDENTDEVICELOCATION_.CLUE_DESC.apply(
    lambda x: 1 if (x.lower().find('emergency') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['POWER_OUT_CLUE_FLG'] = DF_INCIDENTDEVICELOCATION_.CLUE_DESC.apply(
    lambda x: 1 if (x.lower().find('power out') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['TREE_CLUE_FLG'] = DF_INCIDENTDEVICELOCATION_.CLUE_DESC.apply(
    lambda x: 1 if (x.lower().find('tree') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['WIRE_DOWN_CLUE_FLG'] = DF_INCIDENTDEVICELOCATION_.CLUE_DESC.apply(
    lambda x: 1 if (x.lower().find('wire down') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['IVR_CLUE_FLG'] = DF_INCIDENTDEVICELOCATION_.CLUE_DESC.apply(
    lambda x: 1 if (x.lower().find('ivr') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['EQUIPMENT_CLUE_FLG'] = DF_INCIDENTDEVICELOCATION_.CLUE_DESC.apply(
    lambda x: 1 if (x.find('EQUIPMENT') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['TRANSFORMER_CLUE_FLG'] = DF_INCIDENTDEVICELOCATION_.CLUE_DESC.apply(
    lambda x: 1 if (x.find('TRANSFORMER') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['OPEN_DEVICE_CLUE_FLG'] = DF_INCIDENTDEVICELOCATION_.CLUE_DESC.apply(
    lambda x: 1 if (x.find('OPEN DEVICE') != -1) else 0)


# segration of cause desc
DF_INCIDENTDEVICELOCATION_['CAUSE_DESC1'] = DF_INCIDENTDEVICELOCATION_[['CAUSE_DESC']].fillna('0')
DF_INCIDENTDEVICELOCATION_['OH_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if((x.find('OH') != -1) | (x.find('O.H.') != -1)) else 0)
DF_INCIDENTDEVICELOCATION_['UG_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if((x.find('UG') != -1) | (x.find('U.G.') != -1)) else 0)
DF_INCIDENTDEVICELOCATION_['ANIMAL_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('ANIMAL') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['WEATHER_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('WEATHER') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['WEATHER_COLD_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('COLD') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['WEATHER_LIGHTNING_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('LIGHTNING') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['WEATHER__SNOW_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('SNOW') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['WEATHER__WIND_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('WIND') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['WEATHER__HEAT_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('HEAT') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['WEATHER__FLOOD_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('FLOOD') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['PUBLIC_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('PUBLIC') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['STREET_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('ST ') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['SUBSTATION_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('SUBSTATION') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['TREE_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('TREE') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['MISCELLANEOUS_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('MISCELLANEOUS') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['CUST_REQUEST_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('CUSTOMER REQUEST') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['NO_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('NO CAUSE') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['PLANNED_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('PLANNED WORK') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['NO_OUTAGE_CAUSE_FLG'] = DF_INCIDENTDEVICELOCATION_.CAUSE_DESC1.apply(
    lambda x: 1 if(x.find('NO OUTAGE') != -1) else 0)


# segration of OCCURN desc
DF_INCIDENTDEVICELOCATION_['FUSE_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if((x.find('FUSE') != -1) & (x.find('FUSE NOT') == -1)) else 0)
DF_INCIDENTDEVICELOCATION_['CUST_EQUIP_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('CUSTOMER EQUIP') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['POLE_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('POLE') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['TRANSFORMER_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('TRANSFORMER') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['METER_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('METER') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['SERVICE_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('SERVICE') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['CABLE_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('CABLE') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['ST_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('ST') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['FIRE_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('FIRE') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['FOUND_OPEN_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if((x.find('FOUND OPEN') != -1) & (x.find('NOT FOUND OPEN') == -1)) else 0)
DF_INCIDENTDEVICELOCATION_['PUBLIC_SAFETY_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('SAFETY') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['WIRE_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('WIRE') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['SWITCH_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('SWITCH') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['CUTOUT_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('CUTOUT') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['REGULATOR_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('REGULATOR') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['CAP_BANK_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('CAP BANK') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['OH_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('OH') != -1) else 0)
DF_INCIDENTDEVICELOCATION_['RECLOSER_OCCURN_FLG'] = DF_INCIDENTDEVICELOCATION_.OCCURN_DESC.apply(
    lambda x: 1 if(x.find('RECLOSER') != -1) else 0)
DF_INCIDENTDEVICELOCATION_.drop(columns=['CAUSE_DESC1'], inplace=True)

# preprocessing to get flags at INCIDENT_ID level (LEVEL SPECIFIED)
PRIORITY_LIST = list(DF_INCIDENTDEVICELOCATION_.filter(regex=("PRIORITY_VAL"), axis=1).columns)

CAT_LIST = ['POLE_CLUE_FLG', 'PART_LIGHT_CLUE_FLG', 'EMERGENCY_CLUE_FLG', 'POWER_OUT_CLUE_FLG', 'TREE_CLUE_FLG', 'WIRE_DOWN_CLUE_FLG',
            'IVR_CLUE_FLG', 'EQUIPMENT_CLUE_FLG', 'TRANSFORMER_CLUE_FLG', 'OPEN_DEVICE_CLUE_FLG', 'OH_CAUSE_FLG', 'UG_CAUSE_FLG',
            'ANIMAL_CAUSE_FLG', 'WEATHER_CAUSE_FLG', 'WEATHER_COLD_CAUSE_FLG', 'WEATHER_LIGHTNING_CAUSE_FLG', 'WEATHER__SNOW_CAUSE_FLG',
            'WEATHER__WIND_CAUSE_FLG', 'WEATHER__HEAT_CAUSE_FLG', 'WEATHER__FLOOD_CAUSE_FLG', 'PUBLIC_CAUSE_FLG', 'STREET_CAUSE_FLG',
            'SUBSTATION_CAUSE_FLG', 'TREE_CAUSE_FLG', 'MISCELLANEOUS_CAUSE_FLG', 'CUST_REQUEST_CAUSE_FLG', 'NO_CAUSE_FLG', 'PLANNED_CAUSE_FLG',
            'NO_OUTAGE_CAUSE_FLG', 'FUSE_OCCURN_FLG', 'CUST_EQUIP_OCCURN_FLG', 'POLE_OCCURN_FLG', 'TRANSFORMER_OCCURN_FLG', 'METER_OCCURN_FLG',
            'SERVICE_OCCURN_FLG', 'CABLE_OCCURN_FLG', 'ST_OCCURN_FLG', 'FIRE_OCCURN_FLG', 'FOUND_OPEN_OCCURN_FLG', 'PUBLIC_SAFETY_OCCURN_FLG',
            'WIRE_OCCURN_FLG', 'SWITCH_OCCURN_FLG', 'CUTOUT_OCCURN_FLG', 'REGULATOR_OCCURN_FLG', 'CAP_BANK_OCCURN_FLG', 'OH_OCCURN_FLG', 'RECLOSER_OCCURN_FLG']

CAT_LIST = CAT_LIST+PRIORITY_LIST

DF_INCIDENTDEVICELOCATION_CAT = DF_INCIDENTDEVICELOCATION_.groupby(['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE'], as_index=False)[CAT_LIST].agg('sum')

DUMMY_COL = list(DF_INCIDENTDEVICELOCATION_CAT.columns)
DUMMY_COL.remove('INCIDENT_ID')
DUMMY_COL.remove('STRCTUR_NO')
DUMMY_COL.remove('CIRCT_ID')
DUMMY_COL.remove('DNI_EQUIP_TYPE')

for i in DUMMY_COL:
    DF_INCIDENTDEVICELOCATION_CAT[i] = DF_INCIDENTDEVICELOCATION_CAT[i].apply(lambda x: 1 if x >= 1 else 0)


# merge numercial and categorical columns to get ADS at INCIDENT_ID level (LEVEL SPECIFIED)

DF_ADS = pd.merge(DF_NUMERICAL, DF_INCIDENTDEVICELOCATION_CAT, on=['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE'], how='left')
DF_ADS = pd.merge(DF_ADS, DF_TREATED, on=['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE'], how='left')


logging.info("****QC Check****")
logging.info("Shape of Numerical columns at INCIDENT_ID, STRCTUR_NO, CIRCT_ID, DNI_EQUIP_TYPE Level")
logging.info(DF_ADS.shape)
logging.info('\n')

SHAPE = DF_ADS.shape[0]
if SHAPE == 0:
    raise Exception('ADS contains 0 rows after OCCURN_CD filter')

# ## **Add cyclicity according to hour**
DF_ADS['Hour'] = DF_ADS['CREATION_DATETIME'].dt.hour
DF_ADS['Hour_Sin'] = np.sin(DF_ADS.Hour*(2.*np.pi/24))
DF_ADS['Hour_Cos'] = np.cos(DF_ADS.Hour*(2.*np.pi/24))
DF_ADS.drop(['Hour'], axis=1, inplace=True)


# ## **X Y Co-ordinate Conversion**
# function to convert geo_x, geo_y coordinate to lat, long

def change_to_loc(df):
    '''
	Input - GEO_X_COORD, GEO_Y_COORD
	Output - LAT, LONG coordinates of the goe_x and geo_y values
	'''
    demnorthing = df.GEO_Y_COORD
    demeasting = df.GEO_X_COORD
    northing = float(demnorthing) * 0.3048
    easting = float(demeasting) * 0.3048
    om = (northing - 250000 + 4151863.7425) / 6367236.89768
    fo = om + (math.sin(om) * math.cos(om)) * (0.005022893948 + 0.000029370625 * math.pow(math.cos(om), 2) +
    	0.000000235059 * math.pow(math.cos(om), 4) + 0.000000002181 * math.pow(math.cos(om), 6))
    tf = math.sin(fo) / math.cos(fo)
    nf2 = 0.00673949677548 * math.pow(math.cos(fo), 2)
    rn = 0.9999666667 * 6378137 / math.pow((1 - 0.0066943800229034 * math.pow(math.sin(fo), 2)), 0.5)
    q = (easting - 100000) / rn
    b2 = -0.5 * tf * (1 + nf2)
    b4 = -(1 / 12) * (5 + (3 * math.pow(tf, 2)) + (nf2 * (1 - 9 * math.pow(tf, 2)) - 4 * math.pow(nf2, 2)))
    b6 = (1 / 360) * (61 + (90 * math.pow(tf, 2)) + (45 * math.pow(tf, 4)) +
                  (nf2 * (46 - (252 * math.pow(tf, 2)) - (90 * math.pow(tf, 4)))))
    lat = fo + b2 * math.pow(q, 2) * (1 + math.pow(q, 2) * (b4 + b6 * math.pow(q, 2)))
    b3 = -(1 / 6) * (1 + 2 * math.pow(tf, 2) + nf2)
    b5 = (1 / 120) * (5 + 28 * math.pow(tf, 2) + 24 * math.pow(tf, 4) + nf2 * (6 + 8 * math.pow(tf, 2)))
    b7 = -(1 / 5040) * (61 + 662 * math.pow(tf, 2) + 1320 * math.pow(tf, 4) + 720 * math.pow(tf, 6))
    l = q * (1 + math.pow(q, 2) * (b3 + math.pow(q, 2) * (b5 + b7 * math.pow(q, 2))))
    lon = 1.4951653925 - l / math.cos(fo)
    coord = [(lat * 57.2957795131), (-1 * lon * 57.2957795131)]
    return coord[0], coord[1]

DF_LOCATION_['LAT'], DF_LOCATION_['LONG'] = zip(*DF_LOCATION_.apply(change_to_loc, axis=1))

# subset from geo coordinates from location table
DF_GEO_LOCATION = DF_LOCATION_[['LOCATION_ID', 'INCIDENT_ID', 'LAT', 'LONG']]

# merge with ADS
DF_ADS = pd.merge(DF_ADS, DF_GEO_LOCATION, on=['LOCATION_ID', 'INCIDENT_ID'], how='left')


# ## **Add Zones Feature**
# function to add zone feature to the ads according to geo coordinates
def add_zone_feature(df):
    '''
	Input - dataframe with LAT, LONG columns
	Output - ZONES which the LAT, LONG belong to
    '''
    center_lat = 39.7684
    center_long = -86.1581
    zone = ''

    if float(df['LAT']) < center_lat:
        if float(df['LONG']) < center_long:
            zone = 'ZONE1'
        else:
            zone = 'ZONE2'
    else:
        if float(df['LONG']) < center_long:
            zone = 'ZONE4'
        else:
            zone = 'ZONE3'

    return zone


DF_ADS['ZONE'] = DF_ADS.apply(add_zone_feature, axis=1)
logging.info(DF_ADS['ZONE'].unique())


# ## **Create User defined Insertion Time Column**
# create insertion time flag

DF_ADS['INSERTION_TIME'] = datetime.today().strftime('%Y%m%d%H%M')
DF_ADS['INSERTION_TIME'] = DF_ADS['INSERTION_TIME'].astype(np.int64)


# ## **Rank Subsequent Outages**
DF_ADS['Date'] = DF_ADS.CREATION_DATETIME.dt.date
DF_ADS['RANK_SUBSEQUENT_OUTAGES'] = DF_ADS.groupby(['Date'], as_index=False)['CREATION_DATETIME'].rank(method='dense', ascending=True)
DF_ADS.drop(['Date'], axis=1, inplace=True)


# ## **Prepare weather mapping columns**
LIST_COLUMNS = ['LAT', 'LONG']
DF_ADS[LIST_COLUMNS] = DF_ADS[LIST_COLUMNS].apply(pd.to_numeric, errors='coerce')
DF_ADS['LAT'] = DF_ADS['LAT'].ffill()
DF_ADS['LONG'] = DF_ADS['LONG'].ffill()

DF_ADS['Marker1_LAT'] = 39.9613
DF_ADS['Marker2_LAT'] = 39.8971
DF_ADS['Marker3_LAT'] = 39.9060
DF_ADS['Marker4_LAT'] = 39.9024
DF_ADS['Marker5_LAT'] = 39.8960
DF_ADS['Marker6_LAT'] = 39.8339
DF_ADS['Marker7_LAT'] = 39.8412
DF_ADS['Marker8_LAT'] = 39.8381
DF_ADS['Marker9_LAT'] = 39.8386
DF_ADS['Marker10_LAT'] = 39.7579
DF_ADS['Marker11_LAT'] = 39.7621
DF_ADS['Marker12_LAT'] = 39.7621
DF_ADS['Marker13_LAT'] = 39.7695
DF_ADS['Marker14_LAT'] = 39.6617
DF_ADS['Marker15_LAT'] = 39.6639
DF_ADS['Marker16_LAT'] = 39.6702
DF_ADS['Marker17_LAT'] = 39.6744
DF_ADS['Marker18_LAT'] = 39.5909
DF_ADS['Marker19_LAT'] = 39.5295
DF_ADS['Marker20_LAT'] = 39.5475

DF_ADS['Marker1_LONG'] = -86.4034
DF_ADS['Marker2_LONG'] = -86.3045
DF_ADS['Marker3_LONG'] = -86.2001
DF_ADS['Marker4_LONG'] = -86.0738
DF_ADS['Marker5_LONG'] = -85.9783
DF_ADS['Marker6_LONG'] = -86.3155
DF_ADS['Marker7_LONG'] = -86.2056
DF_ADS['Marker8_LONG'] = -86.0985
DF_ADS['Marker9_LONG'] = -85.9811
DF_ADS['Marker10_LONG'] = -86.3155
DF_ADS['Marker11_LONG'] = -86.2042
DF_ADS['Marker12_LONG'] = -86.0923
DF_ADS['Marker13_LONG'] = -85.9708
DF_ADS['Marker14_LONG'] = -86.2935
DF_ADS['Marker15_LONG'] = -86.1823
DF_ADS['Marker16_LONG'] = -86.0669
DF_ADS['Marker17_LONG'] = -85.9557
DF_ADS['Marker18_LONG'] = -86.4212
DF_ADS['Marker19_LONG'] = -86.5874
DF_ADS['Marker20_LONG'] = -86.2743

# calculate distance from 2 lat long
def haversine(p1, p2):
    '''
	Input - point1 and point2 in LAT, LONG
	Output - Minimum diatance from marker
    '''
    R = 6371     # earth radius in km
    p1 = [math.radians(v) for v in p1]
    p2 = [math.radians(v) for v in p2]

    d_lat = p2[0] - p1[0]
    d_lng = p2[1] - p1[1]
    a = math.pow(math.sin(d_lat / 2), 2) + math.cos(p1[0]) * math.cos(p2[0]) * math.pow(math.sin(d_lng / 2), 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c   # returns distance between p1 and p2 in km

# calculate minimum distance
def minimum_distance(lat, long, marker1_lat, marker2_lat, marker3_lat, marker4_lat, marker5_lat, marker6_lat, marker7_lat, marker8_lat, marker9_lat, marker10_lat, marker11_lat,
                     marker12_lat, marker13_lat, marker14_lat, marker15_lat, marker16_lat, marker17_lat, marker18_lat, marker19_lat, marker20_lat, marker1_long, marker2_long,
                     marker3_long, marker4_long, marker5_long, marker6_long, marker7_long, marker8_long, marker9_long, marker10_long, marker11_long, marker12_long, marker13_long,
                     marker14_long, marker15_long, marker16_long, marker17_long, marker18_long, marker19_long, marker20_long):
    '''
	Input - latitude, longitude of outages and different marker locations
	Output - minimum distance and index of marker location
    '''
    dist1 = haversine((lat, long), (marker1_lat, marker1_long))
    dist2 = haversine((lat, long), (marker2_lat, marker2_long))
    dist3 = haversine((lat, long), (marker3_lat, marker3_long))
    dist4 = haversine((lat, long), (marker4_lat, marker4_long))
    dist5 = haversine((lat, long), (marker5_lat, marker5_long))
    dist6 = haversine((lat, long), (marker6_lat, marker6_long))
    dist7 = haversine((lat, long), (marker7_lat, marker7_long))
    dist8 = haversine((lat, long), (marker8_lat, marker8_long))
    dist9 = haversine((lat, long), (marker9_lat, marker9_long))
    dist10 = haversine((lat, long), (marker10_lat, marker10_long))
    dist11 = haversine((lat, long), (marker11_lat, marker11_long))
    dist12 = haversine((lat, long), (marker12_lat, marker12_long))
    dist13 = haversine((lat, long), (marker13_lat, marker13_long))
    dist14 = haversine((lat, long), (marker14_lat, marker14_long))
    dist15 = haversine((lat, long), (marker15_lat, marker15_long))
    dist16 = haversine((lat, long), (marker16_lat, marker16_long))
    dist17 = haversine((lat, long), (marker17_lat, marker17_long))
    dist18 = haversine((lat, long), (marker18_lat, marker18_long))
    dist19 = haversine((lat, long), (marker19_lat, marker19_long))
    dist20 = haversine((lat, long), (marker20_lat, marker20_long))

    dist_list = [dist1, dist2, dist3, dist4, dist5, dist6, dist7, dist8, dist9, dist10, dist11, dist12, dist13, dist14, dist15, dist16, dist17, dist18, dist19, dist20]

    min_index, min_value = min(enumerate(dist_list), key=operator.itemgetter(1))

    if(math.isnan(lat)) | (math.isnan(long)):
        return None, None
    else:
        return min_value, min_index+1

DF_ADS['Min_Distance'], DF_ADS['Marker_Location'] = zip(*DF_ADS.apply(lambda row: minimum_distance(row['LAT'], row['LONG'], row['Marker1_LAT'], row['Marker2_LAT'],
    row['Marker3_LAT'], row['Marker4_LAT'], row['Marker5_LAT'], row['Marker6_LAT'], row['Marker7_LAT'], row['Marker8_LAT'], row['Marker9_LAT'], row['Marker10_LAT'],
    row['Marker11_LAT'], row['Marker12_LAT'], row['Marker13_LAT'], row['Marker14_LAT'], row['Marker15_LAT'], row['Marker16_LAT'], row['Marker17_LAT'],
    row['Marker18_LAT'], row['Marker19_LAT'], row['Marker20_LAT'], row['Marker1_LONG'], row['Marker2_LONG'], row['Marker3_LONG'], row['Marker4_LONG'],
    row['Marker5_LONG'], row['Marker6_LONG'], row['Marker7_LONG'], row['Marker8_LONG'], row['Marker9_LONG'], row['Marker10_LONG'], row['Marker11_LONG'],
    row['Marker12_LONG'], row['Marker13_LONG'], row['Marker14_LONG'], row['Marker15_LONG'], row['Marker16_LONG'], row['Marker17_LONG'], row['Marker18_LONG'],
    row['Marker19_LONG'], row['Marker20_LONG']), axis=1))



DF_ADS.drop(['Marker1_LAT', 'Marker2_LAT', 'Marker3_LAT', 'Marker4_LAT', 'Marker5_LAT', 'Marker6_LAT', 'Marker7_LAT', 'Marker8_LAT', 'Marker9_LAT', 'Marker10_LAT',
            'Marker11_LAT', 'Marker12_LAT', 'Marker13_LAT', 'Marker14_LAT', 'Marker15_LAT', 'Marker16_LAT', 'Marker17_LAT', 'Marker18_LAT', 'Marker19_LAT', 'Marker20_LAT',
            'Marker1_LONG', 'Marker2_LONG', 'Marker3_LONG', 'Marker4_LONG', 'Marker5_LONG', 'Marker6_LONG', 'Marker7_LONG', 'Marker8_LONG', 'Marker9_LONG', 'Marker10_LONG',
            'Marker11_LONG', 'Marker12_LONG', 'Marker13_LONG', 'Marker14_LONG', 'Marker15_LONG', 'Marker16_LONG', 'Marker17_LONG', 'Marker18_LONG', 'Marker19_LONG',
            'Marker20_LONG'], axis=1, inplace=True)

DF_ADS['Marker_Location'] = 'Marker '+DF_ADS['Marker_Location'].astype(str)

logging.info('Check shape of dataframe ads')
logging.info(DF_ADS.shape)


# ## **Add dispatch Area Location**
def cal_distance_from_dipatch_area(lat, long):
    '''
	Input - Latitude, Logitude of an outages
	Output - Minimum distance from a dispatch location and its index
    '''
    if(math.isnan(lat)) | (math.isnan(long)):
        return None, None
    else:
        coords1 = [lat, long]
        dist_34 = geopy.distance.distance(coords1, [39.8802, -86.2324]).miles
        dist_arl = geopy.distance.distance(coords1, [39.8802, -86.0854]).miles
        dist_mill = geopy.distance.distance(coords1, [39.7880, -86.2296]).miles
        dist_english = geopy.distance.distance(coords1, [39.7880, -86.0868]).miles
        dist_wii = geopy.distance.distance(coords1, [39.7003, -86.2303]).miles
        dist_south = geopy.distance.distance(coords1, [39.7003, -86.0834]).miles
    
        dist_list = [dist_34, dist_arl, dist_mill, dist_english, dist_wii, dist_south]

        min_index, min_value = min(enumerate(dist_list), key=operator.itemgetter(1))
    
        return min_value, min_index+1

		
DF_ADS['Min_Distance'], DF_ADS['Grid'] = zip(*DF_ADS.apply(lambda row: cal_distance_from_dipatch_area(row['LAT'], row['LONG']), axis=1))


def map_grid_to_location(row):
    '''
    Add dispatch area location for all outages which are present
    '''
    value = ''
    if row == 1:
        value = '34th'
    elif row == 2:
        value = 'ARL.'
    elif row == 3:
        value = 'MILL'
    elif row == 4:
        value = 'ENGLISH'
    elif row == 5:
        value = 'W.I.'
    elif row == 6:
        value = 'SOUTH'
    else:
        value = 'NO_LOCATION'
	
    return value


DF_ADS['Dispatch_Location'] = DF_ADS.apply(lambda row: map_grid_to_location(row['Grid']), axis=1)
DF_ADS.drop(['Min_Distance', 'Grid'], axis=1, inplace=True)


logging.info(DF_ADS.head())


# ## **QC Check**
def check_level(group):
    '''
	checks level of the table created
	'''
    print(len(group))

logging.info(DF_ADS.groupby(['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE']).apply(check_level))
logging.info('\n')
logging.info("No of NA's if any")
logging.info('\n')
logging.info(DF_ADS.isnull().values.any())

DF_ADS.fillna(method='ffill', inplace=True)

# ## **Write table to OMS Live Mapped Dataset to Curated OMS**
logging.disable(logging.CRITICAL)
DF_ADS.to_csv('gs://aes-analytics-0002-curated/Outage_Restoration/Live_Data_Curation/OMS/OMS_Live_Data.csv')