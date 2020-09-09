#!/usr/bin/env python
# coding: utf-8

# # **Import Required Packages**

# In[1]:


import os
import math
import warnings
import operator
import pandas as pd
import numpy as np
import datetime as dt
import logging

from pandas.io import gbq
from datetime import date, timedelta
from datetime import datetime
from google.cloud import storage
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


# In[2]:


# import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
# logging.debug('This message should appear on the console')
# logging.info('So should this')
# logging.warning('And this, too')
# logging.basicConfig(format='%(asctime)s %(message)s')
# logging.warning('is when this event was logged.')

# # **Check all Live Files present in Bucket**

# In[3]:


current_date = datetime.today().strftime('%Y-%m-%d')
print(current_date)
client = storage.Client()
BUCKET_NAME = 'aes-datahub-0002-raw'
bucket = client.get_bucket(BUCKET_NAME)

blobs = bucket.list_blobs(prefix='OMS/'+current_date)
dirlist = []

for blob in blobs:
#     print(blob.name)
    dirlist.append(str(blob.name))


# In[4]:


# string matching to read tables 

_matching_incident = [s for s in dirlist if "INCIDENT_IPL" in s]
_matching_live_incident = [s for s in _matching_incident if "HIS" not in s]
# print(_matching_live_incident)
# print('\n')

_matching_incident_device = [s for s in dirlist if "INCIDENT_DEVICE_IPL" in s]
_matching_live_incident_device = [s for s in _matching_incident_device if "HIS" not in s]
# print(_matching_live_incident_device)
# print('\n')

_matching_location = [s for s in dirlist if "LOCATION_IPL" in s]
_matching_live_location = [s for s in _matching_location if "HIS" not in s]
# print(_matching_live_location)


# # **Read Live Files in Buckets**

# In[5]:


bucket_name = 'gs://aes-datahub-0002-raw/'

print(current_date)
print('\n')
logging.info('%s', current_date)
logging.info('\n')

live_incident_device = spark.read.format('CSV').option("header","true").option("inferSchema","true").option("delimiter",",").load(
    bucket_name + _matching_live_incident_device[-1]).toPandas()
print(bucket_name + _matching_live_incident_device[-1])
logging.info('Live Incident Device Table name')
logging.info('%s', bucket_name + _matching_live_incident_device[-1])

live_incident = spark.read.format('CSV').option("header","true").option("inferSchema","true").option("delimiter",",").load(
    bucket_name + _matching_live_incident[-1]).toPandas()
print(bucket_name + _matching_live_incident[-1])
logging.info('Live Incident Table name')
logging.info('%s', bucket_name + _matching_live_incident[-1])

live_location = spark.read.format('CSV').option("header","true").option("inferSchema","true").option("delimiter",",").load(
    bucket_name + _matching_live_location[-1]).toPandas()
print(bucket_name + _matching_live_location[-1])
logging.info('Live Location Table name')
logging.info('%s', bucket_name + _matching_live_location[-1])
print('\n')
logging.info('\n')


# In[6]:


## QC checks
print("****QC Check****")
logging.info("****QC Check****")
print("Shape of Live Incident Device Table")
logging.info("Shape of Live Incident Device Table")
print(live_incident_device.shape)
logging.info("%s", live_incident_device.shape)
print("\n")

shape = live_incident_device.shape[0]
if (shape==0):
    raise Exception('Live Incident device table contains 0 rows')

print("****QC Check****")
print("Shape of Live Location Table")
print(live_location.shape)
print("\n")

shape = live_location.shape[0]
if (shape==0):
    raise Exception('Live location table contains 0 rows')

print("****QC Check****")
print("Shape of Live Incident Table")
print(live_incident.shape)
print("\n")
shape = live_incident.shape[0]
if (shape==0):
    raise Exception('Live incident contains 0 rows')

print("****QC Check****")
print("No of Distinct INCIDENT_ID in INCIDENT_DEVICE Table")
print(live_incident_device.INCIDENT_ID.nunique())
print("\n")
print("****QC Check****")
print("No of Distinct INCIDENT_ID in LOCATION Table")
print(live_location.INCIDENT_ID.nunique())
print("\n")
print("****QC Check****")
print("No of Distinct INCIDENT_ID in INCIDENT Table")
print(live_incident.INCIDENT_ID.nunique())
print("\n")


# # **Merge Files and Perform Data QC checks**

# In[7]:


# merge INCIDENT_DEVICE_ID and LOCATION table

df_incident_device_=live_incident_device.copy(deep=True)
df_location_=live_location.copy(deep=True)
df_incident_=live_incident.copy(deep=True)


# subset location tables to get required columns for analysis 
df_location_subset = df_location_[['INCIDENT_ID','LOCATION_ID', 'MAJ_OTG_ID', 'CITY_NAM', 'OCCURN_CD', 'CAUSE_CD']]

# data quality qc
# if (len(df_incident_device_[['INCIDENT_ID', 'LOCATION_ID']]) != len(df_incident_device_[['INCIDENT_ID', 'LOCATION_ID']].drop_duplicates()):
print("****QC Check****")
print("INCIDENT_DEVICE table before and after dropping duplicates at INCIDENT_ID, LOCATION_ID")
print(len(df_incident_device_[['INCIDENT_ID', 'LOCATION_ID']]), len(df_incident_device_[['INCIDENT_ID', 'LOCATION_ID']].drop_duplicates()))
print("\n")
print("****QC Check****")
print("LOCATION table before and after dropping duplicates at INCIDENT_ID, LOCATION_ID")
print(len(df_location_subset[['INCIDENT_ID', 'LOCATION_ID']]), len(df_location_subset[['INCIDENT_ID', 'LOCATION_ID']].drop_duplicates()))
print("\n")

df_incidentdevicelocation_ = pd.merge(df_incident_device_, df_location_subset, on=['INCIDENT_ID', 'LOCATION_ID'], how='left')

print("****QC Check****")
print("INICDENT_DEVICE, LOCATION table merged before and after dropping duplicates at INCIDENT_ID, LOCATION_ID")
print(len(df_incidentdevicelocation_[['INCIDENT_ID', 'LOCATION_ID']]), len(df_incidentdevicelocation_[['INCIDENT_ID', 'LOCATION_ID']].drop_duplicates()))
print("\n")

shape = df_incidentdevicelocation_.shape[0]
if (shape==0):
    raise Exception('Incident and device location merge contains 0 rows')

# # **Apply Required Filters**

# In[8]:


######################################################################################################################################################################################################
######################################################################### APPLYING FILTERS FOR CORRECT DATA INPUTS####################################################################################
######################################################################################################################################################################################################

# customer quantity greater than 0
print('Filter for customer quantity greater than 0')
# print("****QC Check****")
print("Rows left after checking for INCIDENTS whose CUSTOMER QUANTITY IS > 0")
df_incidentdevicelocation_ = df_incidentdevicelocation_[(df_incidentdevicelocation_.DOWNSTREAM_CUST_QTY > 0)]
print(df_incidentdevicelocation_.shape)
print("\n")

shape = df_incidentdevicelocation_.shape[0]
if (shape==0):
    raise Exception('Incident and device location merge contains 0 rows after CUST_QTY filter')

# equip_stn_no is not NCC and not null
print('Filter for equp_stn_no is not NCC or not null')
# print("****QC Check****")
print("Rows left after checking that EQUIP_STN_NO is not from <<NON CONNECTED CUSTOMERS>>")
df_incidentdevicelocation_ = df_incidentdevicelocation_[(df_incidentdevicelocation_.EQUIP_STN_NO != '<NCC>') & (df_incidentdevicelocation_.EQUIP_STN_NO.notnull())]
print(df_incidentdevicelocation_.shape)
print("\n")

shape = df_incidentdevicelocation_.shape[0]
if (shape==0):
    raise Exception('Incident and device location merge contains 0 rows after EQUIP_STN_NO filter')

# removing NAN from DNI_EQUIP_TYPE, CIRCT_ID, STRCTUR_NO
print('Removing NAN from DNI_EQIP_TYPE, CICRT_ID, STRCTUR_NO')
# print("****QC Check****")
print("Rows left after checking CIRCT_ID is not 0 and not null, STRCTUR_NO is not null and DNI_EQIP_TYPE is not null")
df_incidentdevicelocation_ = df_incidentdevicelocation_[(df_incidentdevicelocation_.CIRCT_ID != 0)]
df_incidentdevicelocation_ = df_incidentdevicelocation_[~df_incidentdevicelocation_.CIRCT_ID.isnull()]
df_incidentdevicelocation_ = df_incidentdevicelocation_[~df_incidentdevicelocation_.STRCTUR_NO.isnull()]
df_incidentdevicelocation_ = df_incidentdevicelocation_[~df_incidentdevicelocation_.DNI_EQUIP_TYPE.isnull()]
print(df_incidentdevicelocation_.shape)
print("\n")

shape = df_incidentdevicelocation_.shape[0]
if (shape==0):
    raise Exception('Incident and device location merge contains 0 rows after ID filter')

# removing CLUE_CD which start with 0 but does not start with 00
print('Removing CLUE_CD which start with 0 but do not start with 00')
# print("****QC Check****")
print("Rows left after filtering for CLUE CODES which start with 0 but do not start with 00")
df_incidentdevicelocation_ = df_incidentdevicelocation_[(df_incidentdevicelocation_.CLUE_CD.str[:1] == '0') & (df_incidentdevicelocation_.CLUE_CD.str[:2] != '00')]
df_incidentdevicelocation_ = df_incidentdevicelocation_[df_incidentdevicelocation_.CLUE_CD != '01']
print(df_incidentdevicelocation_.shape)
print("\n")

shape = df_incidentdevicelocation_.shape[0]
if (shape==0):
    raise Exception('Incident and device location merge contains 0 rows after Clue filter')

# removing occurence codes starting with cancel, found ok and duplicate
print('Removing CLUE_CD which start with 0 but do not start with 00')
# print("****QC Check****")
print("Rows left after removing OCCURN_CD which have descriptions starting with CANCEL, FOUND OK or DUPLICATE")
occur_remov = [30003001, 33003301, 33003302, 34003400, 34003401, 34003402, 34003403, 34003404, 34003405, 34003406, 34003407, 34003408, 34003409, 35003500,
                35003501, 35003502, 35003503, 35003504, 35003505, 35003506, 35003507, 35003508, 36003600, 36003601, 36003602, 36003603, 36003604, 36003605,
                36003606, 36003607, 36003608, 37003703, 38003802, 38003803, 38003804, 38003807, 39003910, 41004100, 41004101, 41004102, 48004800, 48004802,
                48004803, 49004900, 49004901, 49004902, 50005000, 50005001, 50005002, 52005200, 52005201, 52005202, 52005203, 52005204, 52005205, 52005206,
                52005207, 53005300, 53005301, 53005302, 53005303, 53005304, 53005305, 53005306, 53005307, 53005308, 53005309, 53005310, 54005400, 54005401,
                54005402, 54005403, 54005404, 54005405, 34003410, 30003000, 36503650, 36503651, 36503652, 36503653, 36503654, 36503655, 36503656, 36503657,
                36503658]
df_incidentdevicelocation_ = df_incidentdevicelocation_[~(df_incidentdevicelocation_.OCCURN_CD.isin(occur_remov))]
print(df_incidentdevicelocation_.shape)
print("\n")

shape = df_incidentdevicelocation_.shape[0]
if (shape==0):        
    raise Exception('ADS contains 0 rows after OCCURN_CD filter')

# # **Aggregate Numerical Columns**

# In[9]:


'''
Create outage id columns 
'''

## START ADS CREATION FOR NUMERICAL COLUMNS AT INCIDENT LEVEL

# 1.1 Aggregate numerical columns at INCIDENT_ID level to keep all unique INCIDNET_ID's
df_numerical = df_incidentdevicelocation_.groupby(['INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE'],as_index=False).agg({'CALL_QTY':'sum','DOWNSTREAM_CUST_QTY':'sum','KVA_VAL':'max', 
                                                                                                           'DOWNSTREAM_KVA_VAL':'max', 'INCIDENT_DEVICE_ID': 'max',
                                                                                                           'CREATION_DATETIME' : 'min','SUBST_ID': 'min', 'LOCATION_ID' : 'max',
                                                                                                            'INCIDENT_DEVICE_ID' : 'max', 'MAJ_OTG_ID' : 'max'})
df_numerical.rename(columns={'DOWNSTREAM_CUST_QTY' : 'CUST_QTY'}, inplace=True)

df_numerical['INCIDENT_ID']=df_numerical['INCIDENT_ID'].astype(np.int64)
df_numerical['CIRCT_ID']=df_numerical['CIRCT_ID'].astype(np.int64)

df_numerical['OUTAGE_ID'] = df_numerical.apply(lambda x:'%s%s%s%s' % (x['INCIDENT_ID'], x['STRCTUR_NO'], x['CIRCT_ID'], x['DNI_EQUIP_TYPE']),axis=1)

print("****QC Check****")
print("Shape of Numerical columns at 'INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE' Level")
print(df_numerical.shape)
print('\n')

shape = df_numerical.shape[0]
if (shape==0):        
    raise Exception('ADS contains 0 rows after OCCURN_CD filter')

# # **Create Day and Night Flags**

# In[10]:


# 1.2 Create Day and Night Flags from CREATION_DATETIME columns
df_numerical['CREATION_DATETIME'] = pd.to_datetime(df_numerical['CREATION_DATETIME'],errors='coerce')
df_numerical['DAY_FLAG'] = df_numerical.CREATION_DATETIME.dt.hour.apply(lambda x: 1 if ((x >= 6) & (x<18)) else 0)


# # **City, Priority Treatment**

# In[11]:


df_incidentdevicelocation_['PRIORITY_VAL_1'] = df_incidentdevicelocation_['PRIORITY_VAL'].apply(lambda x: 1 if x == 1 else 0)
df_incidentdevicelocation_['PRIORITY_VAL_2'] = df_incidentdevicelocation_['PRIORITY_VAL'].apply(lambda x: 1 if x == 2 else 0)
df_incidentdevicelocation_['PRIORITY_VAL_3'] = df_incidentdevicelocation_['PRIORITY_VAL'].apply(lambda x: 1 if x == 3 else 0)
df_incidentdevicelocation_['PRIORITY_VAL_5'] = df_incidentdevicelocation_['PRIORITY_VAL'].apply(lambda x: 1 if x == 5 else 0)

df_incidentdevicelocation_.drop(['PRIORITY_VAL'],axis=1,inplace=True)

df_incidentdevicelocation_.CITY_NAM = df_incidentdevicelocation_.CITY_NAM.apply(lambda x: 'INDIANAPOLIS' if(str(x).find('INDIAN') != -1) else x)
df_incidentdevicelocation_.CITY_NAM = df_incidentdevicelocation_.CITY_NAM.apply(lambda x: 'NO_CITY' if(x != x) else x)


# In[12]:


# city treatment
def cat_city_treat(group):
    if(group.CITY_NAM.nunique() > 1):
        x = group[group.CITY_NAM != 'NO_CITY'].CITY_NAM.unique()
        group.CITY_NAM = x[0]
        return group
    else:
        return group

df_treated = df_incidentdevicelocation_[['INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE','CITY_NAM']]
df_treated = df_treated.groupby(['INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE'], as_index = False).apply(cat_city_treat)
df_treated.drop_duplicates(subset=['INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE'],ignore_index=True,inplace=True)

# # **Cause, Clue, Occurn Mapping**

# In[13]:


# cause, occurn, clue mapping files
cluemapping_ = spark.read.format('CSV').option("header","true").option("inferSchema","true").option("delimiter",",").load(
    'gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/Mapping_Tables/cluemapping.csv').toPandas()
occurnmapping_ = spark.read.format('CSV').option("header","true").option("inferSchema","true").option("delimiter",",").load(
    'gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/Mapping_Tables/occurnmapping.csv').toPandas()
causemapping_ = spark.read.format('CSV').option("header","true").option("inferSchema","true").option("delimiter",",").load(
    'gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/Mapping_Tables/causemapping.csv').toPandas()

df_incidentdevicelocation_ = pd.merge(df_incidentdevicelocation_, cluemapping_, on=['CLUE_CD'], how='left')
df_incidentdevicelocation_ = pd.merge(df_incidentdevicelocation_, occurnmapping_, on=['OCCURN_CD'], how='left')
df_incidentdevicelocation_ = pd.merge(df_incidentdevicelocation_, causemapping_, on=['CAUSE_CD'], how='left')

df_incidentdevicelocation_["CLUE_DESC"]= df_incidentdevicelocation_["CLUE_DESC"].astype(str)
df_incidentdevicelocation_["CAUSE_DESC"]= df_incidentdevicelocation_["CAUSE_DESC"].astype(str)
df_incidentdevicelocation_["OCCURN_DESC"]= df_incidentdevicelocation_["OCCURN_DESC"].astype(str)


# In[14]:


# segregation of clue code desc

df_incidentdevicelocation_['POLE_CLUE_FLG'] = df_incidentdevicelocation_.CLUE_DESC.apply(lambda x: 1 if (x.lower().find('pole') != -1) else 0)
df_incidentdevicelocation_['PART_LIGHT_CLUE_FLG'] = df_incidentdevicelocation_.CLUE_DESC.apply(lambda x: 1 if (x.lower().find('part lights') != -1) else 0)
df_incidentdevicelocation_['EMERGENCY_CLUE_FLG'] = df_incidentdevicelocation_.CLUE_DESC.apply(lambda x: 1 if (x.lower().find('emergency') != -1) else 0)
df_incidentdevicelocation_['POWER_OUT_CLUE_FLG'] = df_incidentdevicelocation_.CLUE_DESC.apply(lambda x: 1 if (x.lower().find('power out') != -1) else 0)
df_incidentdevicelocation_['TREE_CLUE_FLG'] = df_incidentdevicelocation_.CLUE_DESC.apply(lambda x: 1 if (x.lower().find('tree') != -1) else 0)
df_incidentdevicelocation_['WIRE_DOWN_CLUE_FLG'] = df_incidentdevicelocation_.CLUE_DESC.apply(lambda x: 1 if (x.lower().find('wire down') != -1) else 0)
df_incidentdevicelocation_['IVR_CLUE_FLG'] = df_incidentdevicelocation_.CLUE_DESC.apply(lambda x: 1 if (x.lower().find('ivr') != -1) else 0)
df_incidentdevicelocation_['EQUIPMENT_CLUE_FLG'] = df_incidentdevicelocation_.CLUE_DESC.apply(lambda x: 1 if (x.find('EQUIPMENT') != -1) else 0)
df_incidentdevicelocation_['TRANSFORMER_CLUE_FLG'] = df_incidentdevicelocation_.CLUE_DESC.apply(lambda x: 1 if (x.find('TRANSFORMER') != -1) else 0)
df_incidentdevicelocation_['OPEN_DEVICE_CLUE_FLG'] = df_incidentdevicelocation_.CLUE_DESC.apply(lambda x: 1 if (x.find('OPEN DEVICE') != -1) else 0)


# segration of cause desc

df_incidentdevicelocation_['CAUSE_DESC1'] = df_incidentdevicelocation_[['CAUSE_DESC']].fillna('0')
df_incidentdevicelocation_['OH_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if((x.find('OH') != -1) | (x.find('O.H.') != -1)) else 0)
df_incidentdevicelocation_['UG_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if((x.find('UG') != -1) | (x.find('U.G.') != -1)) else 0)
df_incidentdevicelocation_['ANIMAL_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('ANIMAL') != -1) else 0)
df_incidentdevicelocation_['WEATHER_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('WEATHER') != -1) else 0)
df_incidentdevicelocation_['WEATHER_COLD_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('COLD') != -1) else 0)
df_incidentdevicelocation_['WEATHER_LIGHTNING_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('LIGHTNING') != -1) else 0)
df_incidentdevicelocation_['WEATHER__SNOW_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('SNOW') != -1) else 0)
df_incidentdevicelocation_['WEATHER__WIND_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('WIND') != -1) else 0)
df_incidentdevicelocation_['WEATHER__HEAT_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('HEAT') != -1) else 0)
df_incidentdevicelocation_['WEATHER__FLOOD_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('FLOOD') != -1) else 0)
df_incidentdevicelocation_['PUBLIC_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('PUBLIC') != -1) else 0)
df_incidentdevicelocation_['STREET_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('ST ') != -1) else 0)
df_incidentdevicelocation_['SUBSTATION_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('SUBSTATION') != -1) else 0)
df_incidentdevicelocation_['TREE_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('TREE') != -1) else 0)
df_incidentdevicelocation_['MISCELLANEOUS_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('MISCELLANEOUS') != -1) else 0)
df_incidentdevicelocation_['CUST_REQUEST_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('CUSTOMER REQUEST') != -1) else 0)
df_incidentdevicelocation_['NO_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('NO CAUSE') != -1) else 0)
df_incidentdevicelocation_['PLANNED_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('PLANNED WORK') != -1) else 0)
df_incidentdevicelocation_['NO_OUTAGE_CAUSE_FLG'] = df_incidentdevicelocation_.CAUSE_DESC1.apply(lambda x: 1 if(x.find('NO OUTAGE') != -1) else 0)


# segration of OCCURN desc
df_incidentdevicelocation_['FUSE_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if((x.find('FUSE') != -1) & (x.find('FUSE NOT') == -1)) else 0)
df_incidentdevicelocation_['CUST_EQUIP_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('CUSTOMER EQUIP') != -1) else 0)
df_incidentdevicelocation_['POLE_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('POLE') != -1) else 0)
df_incidentdevicelocation_['TRANSFORMER_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('TRANSFORMER') != -1) else 0)
df_incidentdevicelocation_['METER_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('METER') != -1) else 0)
df_incidentdevicelocation_['SERVICE_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('SERVICE') != -1) else 0)
df_incidentdevicelocation_['CABLE_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('CABLE') != -1) else 0)
df_incidentdevicelocation_['ST_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('ST') != -1) else 0)
df_incidentdevicelocation_['FIRE_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('FIRE') != -1) else 0)
df_incidentdevicelocation_['FOUND_OPEN_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if((x.find('FOUND OPEN') != -1) & (x.find('NOT FOUND OPEN') == -1)) else 0)
df_incidentdevicelocation_['PUBLIC_SAFETY_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('SAFETY') != -1) else 0)
df_incidentdevicelocation_['WIRE_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('WIRE') != -1) else 0)
df_incidentdevicelocation_['SWITCH_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('SWITCH') != -1) else 0)
df_incidentdevicelocation_['CUTOUT_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('CUTOUT') != -1) else 0)
df_incidentdevicelocation_['REGULATOR_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('REGULATOR') != -1) else 0)
df_incidentdevicelocation_['CAP_BANK_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('CAP BANK') != -1) else 0)
df_incidentdevicelocation_['OH_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('OH') != -1) else 0)
df_incidentdevicelocation_['RECLOSER_OCCURN_FLG'] = df_incidentdevicelocation_.OCCURN_DESC.apply(lambda x: 1 if(x.find('RECLOSER') != -1) else 0)

df_incidentdevicelocation_.drop(columns = ['CAUSE_DESC1'],inplace=True)


# In[15]:


# preprocessing to get flags at INCIDENT_ID level (LEVEL SPECIFIED)
priority_list = list(df_incidentdevicelocation_.filter(regex=("PRIORITY_VAL"), axis=1).columns)

cat_list = ['POLE_CLUE_FLG','PART_LIGHT_CLUE_FLG','EMERGENCY_CLUE_FLG','POWER_OUT_CLUE_FLG','TREE_CLUE_FLG','WIRE_DOWN_CLUE_FLG',
            'IVR_CLUE_FLG','EQUIPMENT_CLUE_FLG','TRANSFORMER_CLUE_FLG','OPEN_DEVICE_CLUE_FLG','OH_CAUSE_FLG','UG_CAUSE_FLG',
            'ANIMAL_CAUSE_FLG','WEATHER_CAUSE_FLG','WEATHER_COLD_CAUSE_FLG','WEATHER_LIGHTNING_CAUSE_FLG','WEATHER__SNOW_CAUSE_FLG',
            'WEATHER__WIND_CAUSE_FLG','WEATHER__HEAT_CAUSE_FLG','WEATHER__FLOOD_CAUSE_FLG','PUBLIC_CAUSE_FLG','STREET_CAUSE_FLG',
            'SUBSTATION_CAUSE_FLG','TREE_CAUSE_FLG','MISCELLANEOUS_CAUSE_FLG','CUST_REQUEST_CAUSE_FLG','NO_CAUSE_FLG','PLANNED_CAUSE_FLG',
            'NO_OUTAGE_CAUSE_FLG','FUSE_OCCURN_FLG','CUST_EQUIP_OCCURN_FLG','POLE_OCCURN_FLG','TRANSFORMER_OCCURN_FLG','METER_OCCURN_FLG',
            'SERVICE_OCCURN_FLG','CABLE_OCCURN_FLG','ST_OCCURN_FLG','FIRE_OCCURN_FLG','FOUND_OPEN_OCCURN_FLG','PUBLIC_SAFETY_OCCURN_FLG',
            'WIRE_OCCURN_FLG','SWITCH_OCCURN_FLG','CUTOUT_OCCURN_FLG','REGULATOR_OCCURN_FLG','CAP_BANK_OCCURN_FLG','OH_OCCURN_FLG','RECLOSER_OCCURN_FLG']

cat_list = cat_list + priority_list

df_incidentdevicelocation_cat = df_incidentdevicelocation_.groupby(['INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE'], as_index = False)[cat_list].agg('sum')
    
dummy_col = list(df_incidentdevicelocation_cat.columns)
dummy_col.remove('INCIDENT_ID')
dummy_col.remove('STRCTUR_NO')
dummy_col.remove('CIRCT_ID')
dummy_col.remove('DNI_EQUIP_TYPE')

for i in dummy_col:
    df_incidentdevicelocation_cat[i] =  df_incidentdevicelocation_cat[i].apply(lambda x: 1 if x>=1 else 0)


# In[16]:


# merge numercial and categorical columns to get ADS at INCIDENT_ID level (LEVEL SPECIFIED)
df_ads = pd.merge(df_numerical, df_incidentdevicelocation_cat, on = ['INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE'], how='left')
df_ads = pd.merge(df_ads, df_treated, on=['INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE'], how='left')

print("****QC Check****")
print("Shape of Numerical columns at INCIDENT_ID Level")
print(df_ads.shape)
print('\n')

shape = df_ads.shape[0]
if (shape==0):        
    raise Exception('ADS contains 0 rows after OCCURN_CD filter')

# display(df_ads.head())


# # **Add cyclicity according to hour**

# In[17]:


df_ads['Hour'] = df_ads['CREATION_DATETIME'].dt.hour
df_ads['Hour_Sin'] = np.sin(df_ads.Hour*(2.*np.pi/24))
df_ads['Hour_Cos'] = np.cos(df_ads.Hour*(2.*np.pi/24))
df_ads.drop(['Hour'],axis=1,inplace=True)


# # **X Y Co-ordinate Conversion**

# In[18]:


# function to convert geo_x, geo_y coordinate to lat, long
def change_to_loc(df):
    demnorthing = df.GEO_Y_COORD
    demeasting = df.GEO_X_COORD
    northing = float(demnorthing) * 0.3048
    easting = float(demeasting) * 0.3048
    om = (northing - 250000 + 4151863.7425) / 6367236.89768
    fo = om + (math.sin(om) * math.cos(om)) * (0.005022893948 + 0.000029370625 * math.pow(math.cos(om), 2) + 0.000000235059 * math.pow(math.cos(om), 4) + 0.000000002181 * math.pow(math.cos(om), 6))
    tf = math.sin(fo) / math.cos(fo)
    nf2 = 0.00673949677548 * math.pow(math.cos(fo), 2)
    rn = 0.9999666667 * 6378137 / math.pow((1 - 0.0066943800229034 * math.pow(math.sin(fo), 2)), 0.5)
    q = (easting - 100000) / rn
    b2 = -0.5 * tf * (1 + nf2)
    b4 = -(1 / 12) * (5 + (3 * math.pow(tf, 2)) + (nf2 * (1 - 9 * math.pow(tf, 2)) - 4 * math.pow(nf2, 2)))
    b6 = (1 / 360) * (61 + (90 * math.pow(tf, 2)) + (45 * math.pow(tf, 4)) + (nf2 * (46 - (252 * math.pow(tf, 2)) - (90 * math.pow(tf, 4)))))
    lat = fo + b2 * math.pow(q, 2) * (1 + math.pow(q, 2) * (b4 + b6 * math.pow(q, 2)))
    b3 = -(1 / 6) * (1 + 2 * math.pow(tf, 2) + nf2)
    b5 = (1 / 120) * (5 + 28 * math.pow(tf, 2) + 24 * math.pow(tf, 4) + nf2 * (6 + 8 * math.pow(tf, 2)))
    b7 = -(1 / 5040) * (61 + 662 * math.pow(tf, 2) + 1320 * math.pow(tf, 4) + 720 * math.pow(tf, 6))
    l = q * (1 + math.pow(q, 2) * (b3 + math.pow(q, 2) * (b5 + b7 * math.pow(q, 2))))
    lon = 1.4951653925 - l / math.cos(fo)
    coord = [(lat * 57.2957795131), (-1 * lon * 57.2957795131)]
    return coord[0],coord[1]

df_location_['LAT'],df_location_['LONG'] = zip(*df_location_.apply(change_to_loc, axis=1))


# In[19]:


# subset from geo coordinates from location table
df_geo_location_ = df_location_[['LOCATION_ID','INCIDENT_ID', 'LAT', 'LONG']]
# merge with ADS
df_ads = pd.merge(df_ads, df_geo_location_, on=['LOCATION_ID', 'INCIDENT_ID'], how='left')


# # **Add Zones Feature**

# In[20]:


# function to add zone feature to the ads according to geo coordinates
def add_zone_feature(df):
    center_lat = 39.7684
    center_long = -86.1581
    zone = ''

    if(float(df['LAT']) < center_lat):
        if(float(df['LONG']) < center_long):
            zone = 'ZONE1'
        else:
            zone = 'ZONE2'
    else:
        if(float(df['LONG']) < center_long):
            zone = 'ZONE4'
        else:
            zone = 'ZONE3'
    
    return zone


# In[21]:


df_ads['ZONE'] = df_ads.apply(add_zone_feature, axis=1)
print(df_ads['ZONE'].unique())


# # **Add Subsequent Outages Feature**

# In[25]:


#### create trigger id 
df_ads['MAJ_OTG_ID'] = datetime.today().strftime('%Y%m%d%H%M')


# In[26]:


df_ads['RANK_SUBSEQUENT_MAJ_OTG_ID'] = df_ads.groupby(['MAJ_OTG_ID'])['CREATION_DATETIME'].rank(method='dense', ascending=True)


# # **Add Live Outages Feature**

# In[ ]:


# live outages is the no of active incident in the ads 
# group by 4 coulumns and find no of outages 
df_ads['LIVE_OUTAGE'] = (df_ads['OUTAGE_ID'].nunique())


# # **Prepare weather mapping columns**

# In[27]:


list_columns = ['LAT','LONG']
df_ads[list_columns] = df_ads[list_columns].apply(pd.to_numeric, errors='coerce')


# In[28]:


df_ads['Marker1_LAT'] =  39.9613 
df_ads['Marker2_LAT'] = 39.8971
df_ads['Marker3_LAT'] = 39.9060
df_ads['Marker4_LAT'] = 39.9024
df_ads['Marker5_LAT'] = 39.8960
df_ads['Marker6_LAT'] = 39.8339
df_ads['Marker7_LAT'] = 39.8412
df_ads['Marker8_LAT'] = 39.8381
df_ads['Marker9_LAT'] = 39.8386
df_ads['Marker10_LAT'] = 39.7579
df_ads['Marker11_LAT'] = 39.7621
df_ads['Marker12_LAT'] = 39.7621
df_ads['Marker13_LAT'] = 39.7695
df_ads['Marker14_LAT'] = 39.6617
df_ads['Marker15_LAT'] = 39.6639
df_ads['Marker16_LAT'] = 39.6702
df_ads['Marker17_LAT'] = 39.6744
df_ads['Marker18_LAT'] = 39.5909
df_ads['Marker19_LAT'] = 39.5295
df_ads['Marker20_LAT'] = 39.5475

df_ads['Marker1_LONG'] = -86.4034 
df_ads['Marker2_LONG'] = -86.3045
df_ads['Marker3_LONG'] = -86.2001
df_ads['Marker4_LONG'] = -86.0738
df_ads['Marker5_LONG'] = -85.9783
df_ads['Marker6_LONG'] = -86.3155
df_ads['Marker7_LONG'] = -86.2056
df_ads['Marker8_LONG'] = -86.0985
df_ads['Marker9_LONG'] = -85.9811
df_ads['Marker10_LONG'] = -86.3155
df_ads['Marker11_LONG'] = -86.2042
df_ads['Marker12_LONG'] = -86.0923
df_ads['Marker13_LONG'] = -85.9708
df_ads['Marker14_LONG'] = -86.2935
df_ads['Marker15_LONG'] = -86.1823
df_ads['Marker16_LONG'] = -86.0669
df_ads['Marker17_LONG'] = -85.9557
df_ads['Marker18_LONG'] = -86.4212
df_ads['Marker19_LONG'] = -86.5874
df_ads['Marker20_LONG'] = -86.2743


# In[29]:


# calculate distance from 2 lat long 
def haversine(p1, p2):
    R = 6371     # earth radius in km
    p1 = [math.radians(v) for v in p1]
    p2 = [math.radians(v) for v in p2]

    d_lat = p2[0] - p1[0]
    d_lng = p2[1] - p1[1]
    a = math.pow(math.sin(d_lat / 2), 2) + math.cos(p1[0]) * math.cos(p2[0]) * math.pow(math.sin(d_lng / 2), 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c   # returns distance between p1 and p2 in km


# In[30]:


# calculate minimum distance

def minimum_distance(lat, long, marker1_lat, marker2_lat, marker3_lat, marker4_lat, marker5_lat, marker6_lat, marker7_lat, marker8_lat, marker9_lat, marker10_lat, marker11_lat,
                     marker12_lat, marker13_lat, marker14_lat, marker15_lat, marker16_lat, marker17_lat, marker18_lat, marker19_lat, marker20_lat, marker1_long, marker2_long,
                     marker3_long, marker4_long, marker5_long, marker6_long, marker7_long, marker8_long, marker9_long, marker10_long, marker11_long, marker12_long, marker13_long,
                     marker14_long, marker15_long, marker16_long, marker17_long, marker18_long, marker19_long, marker20_long):
    
    dist1 = haversine((lat,long), (marker1_lat, marker1_long))
    dist2 = haversine((lat,long), (marker2_lat, marker2_long))
    dist3 = haversine((lat,long), (marker3_lat, marker3_long))
    dist4 = haversine((lat,long), (marker4_lat, marker4_long))
    dist5 = haversine((lat,long), (marker5_lat, marker5_long))
    dist6 = haversine((lat,long), (marker6_lat, marker6_long))
    dist7 = haversine((lat,long), (marker7_lat, marker7_long))
    dist8 = haversine((lat,long), (marker8_lat, marker8_long))
    dist9 = haversine((lat,long), (marker9_lat, marker9_long))
    dist10 = haversine((lat,long), (marker10_lat, marker10_long))
    dist11 = haversine((lat,long), (marker11_lat, marker11_long))
    dist12 = haversine((lat,long), (marker12_lat, marker12_long))
    dist13 = haversine((lat,long), (marker13_lat, marker13_long))
    dist14 = haversine((lat,long), (marker14_lat, marker14_long))
    dist15 = haversine((lat,long), (marker15_lat, marker15_long))
    dist16 = haversine((lat,long), (marker16_lat, marker16_long))
    dist17 = haversine((lat,long), (marker17_lat, marker17_long))
    dist18 = haversine((lat,long), (marker18_lat, marker18_long))
    dist19 = haversine((lat,long), (marker19_lat, marker19_long))
    dist20 = haversine((lat,long), (marker20_lat, marker20_long))
    
    dist_list = [dist1, dist2, dist3, dist4, dist5, dist6, dist7, dist8, dist9, dist10, dist11, dist12, dist13, dist14, dist15, dist16, dist17, dist18, dist19, dist20]

    min_index, min_value = min(enumerate(dist_list), key=operator.itemgetter(1))
    
    if ( (math.isnan(lat)) | (math.isnan(long)) ):
        return None, None
    else :
        return min_value, min_index+1


# In[31]:


df_ads['Min_Distance'], df_ads['Marker_Location'] = zip(*df_ads.apply(lambda row: minimum_distance(row['LAT'], row['LONG'], row['Marker1_LAT'], row['Marker2_LAT'],
                                                            row['Marker3_LAT'], row['Marker4_LAT'], row['Marker5_LAT'], row['Marker6_LAT'],
                                                            row['Marker7_LAT'], row['Marker8_LAT'], row['Marker9_LAT'], row['Marker10_LAT'], 
                                                            row['Marker11_LAT'], row['Marker12_LAT'], row['Marker13_LAT'], row['Marker14_LAT'],
                                                            row['Marker15_LAT'], row['Marker16_LAT'], row['Marker17_LAT'], row['Marker18_LAT'],
                                                            row['Marker19_LAT'], row['Marker20_LAT'], row['Marker1_LONG'], row['Marker2_LONG'],
                                                            row['Marker3_LONG'], row['Marker4_LONG'], row['Marker5_LONG'], row['Marker6_LONG'], 
                                                            row['Marker7_LONG'], row['Marker8_LONG'], row['Marker9_LONG'], row['Marker10_LONG'],
                                                            row['Marker11_LONG'], row['Marker12_LONG'], row['Marker13_LONG'], row['Marker14_LONG'],
                                                            row['Marker15_LONG'], row['Marker16_LONG'], row['Marker17_LONG'], row['Marker18_LONG'], 
                                                            row['Marker19_LONG'], row['Marker20_LONG']),axis=1))


# In[32]:


df_ads.drop(['Marker1_LAT', 'Marker2_LAT', 'Marker3_LAT', 'Marker4_LAT', 'Marker5_LAT', 'Marker6_LAT', 'Marker7_LAT', 'Marker8_LAT', 'Marker9_LAT', 'Marker10_LAT',
            'Marker11_LAT', 'Marker12_LAT', 'Marker13_LAT', 'Marker14_LAT', 'Marker15_LAT', 'Marker16_LAT', 'Marker17_LAT', 'Marker18_LAT', 'Marker19_LAT', 'Marker20_LAT',
            'Marker1_LONG', 'Marker2_LONG', 'Marker3_LONG', 'Marker4_LONG', 'Marker5_LONG', 'Marker6_LONG', 'Marker7_LONG', 'Marker8_LONG', 'Marker9_LONG', 'Marker10_LONG',
            'Marker11_LONG', 'Marker12_LONG', 'Marker13_LONG', 'Marker14_LONG', 'Marker15_LONG', 'Marker16_LONG', 'Marker17_LONG', 'Marker18_LONG', 'Marker19_LONG',
            'Marker20_LONG'], axis=1, inplace=True)
df_ads['Marker_Location'] = 'Marker' + df_ads['Marker_Location'].astype(str)
print('Check shape of dataframe ads')
print(df_ads.shape)


# # **QC Check**

# In[33]:


def check_level(group):
    print(len(group))
df_ads.groupby(['INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE']).apply(check_level)

df_ads.fillna(method='ffill',inplace=True)

# QC checks every value should be one

# # **Write table to OMS Live Mapped Dataset to Curated OMS**

# In[34]:


df_ads.to_csv('gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/OMS/OMS_Live_Data.csv')


# In[ ]:


#os.system("gsutil cp root/OMS_Live_Data.csv gs://aes-datahub-0002-curated/Outage_Restoration/Live_Data_Curation/OMS_Live_Data.csv"

