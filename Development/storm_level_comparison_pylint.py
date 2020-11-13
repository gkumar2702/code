#!/usr/bin/env python
# coding: utf-8

"""
DAG for diagnostic view dashboard
"""

import warnings
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from pandas.io import gbq
from google.cloud import storage
import pandas_gbq as gbq


warnings.filterwarnings('ignore')
pd.options.mode.chained_assignment = None  # default='warn'

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)
pd.options.display.float_format = '{:.2f}'.format

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

CURRENT_DATE = datetime.today().strftime('%Y-%m-%d')
print(CURRENT_DATE)
CLIENT = storage.Client()
BUCKET_NAME = 'aes-datahub-0001-raw'
BUCKET = CLIENT.get_bucket(BUCKET_NAME)

BLOBS = BUCKET.list_blobs(prefix='OMS/'+CURRENT_DATE)
DIRLIST = []


for blob in BLOBS:
#     print(blob.name)
    DIRLIST.append(str(blob.name))

_MATCHING_FACILITY = [s for s in DIRLIST if "FACILITY_IPL_Daily" in s]
_MATCHING_LIVE_FACILITY = [s for s in _MATCHING_FACILITY if "HIS" in s]
print(_MATCHING_LIVE_FACILITY)
print('\n')

BUCKET_NAME = 'gs://aes-datahub-0001-raw/'

print(CURRENT_DATE)
print('\n')
logging.info('%s', CURRENT_DATE)
logging.info('\n')

FACILITY = pd.read_csv(BUCKET_NAME + _MATCHING_LIVE_FACILITY[0], encoding="ISO-8859-1", sep=",")

print(FACILITY.shape)

## QC checks
print("****QC Check****")
print("Shape of HIS Incident Device Table")
print(FACILITY.shape)
print("\n")
print("****QC Check****")
print("No of Distinct INCIDENT_ID in INCIDENT_DEVICE Table")
print(FACILITY.FAC_JOB_ID.nunique())
print("\n")


# # **Apply Required Filters**

####### APPLYING FILTERS FOR CORRECT DATA INPUTS ###########

# customer quantity greater than 0
print('Filter for customer quantity greater than 0')
# print("****QC Check****")
print("Rows left after checking for INCIDENTS whose CUSTOMER QUANTITY IS > 0")
FACILITY = FACILITY[(FACILITY.DOWNSTREAM_CUST_QTY > 0)]
print(FACILITY.shape)
print("\n")

# equip_stn_no is not NCC and not null
print('Filter for equp_stn_no is not NCC or not null')
# print("****QC Check****")
print("Rows left after checking that EQUIP_STN_NO is not from <<NON CONNECTED CUSTOMERS>>")
FACILITY = FACILITY[(FACILITY.EQUIP_STN_NO != '<NCC>') & (FACILITY.EQUIP_STN_NO.notnull())]
print(FACILITY.shape)
print("\n")

# removing NAN from DNI_EQUIP_TYPE, CIRCT_ID, STRCTUR_NO
print('Removing NAN from DNI_EQIP_TYPE, CICRT_ID, STRCTUR_NO')
# print("****QC Check****")
print("Rows left after checking CIRCT_ID is not\
    0 and not null STRCTUR_NO is not null and DNI_EQIP_TYPE is not null")
FACILITY = FACILITY[(FACILITY.CIRCT_ID != 0)]
FACILITY = FACILITY[~FACILITY.CIRCT_ID.isnull()]
FACILITY = FACILITY[~FACILITY.STRCTUR_NO.isnull()]
FACILITY = FACILITY[~FACILITY.DNI_EQUIP_TYPE.isnull()]
print(FACILITY.shape)
print("\n")

# removing CLUE_CD which start with 0 but does not start with 00
print('Removing CLUE_CD which start with 0 but do not start with 00')
# print("****QC Check****")
print("Rows left after filtering for CLUE CODES which start with 0 but do not start with 00")
FACILITY = FACILITY[(FACILITY.CLUE_CD.str[:1] == '0') & (FACILITY.CLUE_CD.str[:2] != '00')]
FACILITY = FACILITY[FACILITY.CLUE_CD != '01']
print(FACILITY.shape)
print("\n")

# removing occurence codes starting with cancel, found ok and duplicate
print('Removing CLUE_CD which start with 0 but do not start with 00')
# print("****QC Check****")
print("Rows left after removing OCCURN_CD which have\
    descriptions starting with CANCEL, FOUND OK or DUPLICATE")
OCCUR_REMOV = [30003001, 33003301, 33003302, 34003400, 34003401, 34003402, 34003403,
               34003404, 34003405, 34003406, 34003407, 34003408, 34003409, 35003500,
               35003501, 35003502, 35003503, 35003504, 35003505, 35003506, 35003507,
               35003508, 36003600, 36003601, 36003602, 36003603, 36003604, 36003605,
               36003606, 36003607, 36003608, 37003703, 38003802, 38003803, 38003804,
               38003807, 39003910, 41004100, 41004101, 41004102, 48004800, 48004802,
               48004803, 49004900, 49004901, 49004902, 50005000, 50005001, 50005002,
               52005200, 52005201, 52005202, 52005203, 52005204, 52005205, 52005206,
               52005207, 53005300, 53005301, 53005302, 53005303, 53005304, 53005305,
               53005306, 53005307, 53005308, 53005309, 53005310, 54005400, 54005401,
               54005402, 54005403, 54005404, 54005405, 34003410, 30003000, 36503650,
               36503651, 36503652, 36503653, 36503654, 36503655, 36503656, 36503657,
               36503658]

FACILITY = FACILITY[~(FACILITY.OCCURN_CD.isin(OCCUR_REMOV))]
print(FACILITY.shape)
print("\n")

DF_NUMERICAL = FACILITY


# # **Aggregate Numerical Columns**

'''
Create outage id columns
remove and filter outages only after the trigger
'''

## START ADS CREATION FOR NUMERICAL COLUMNS AT INCIDENT LEVEL

# 1.1 Aggregate numerical columns at INCIDENT_ID level to keep all unique INCIDNET_ID's
DF_NUMERICAL = FACILITY.groupby(['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE'],
                                as_index=False).agg({'CALL_QTY':'sum',
                                                     'DOWNSTREAM_CUST_QTY':'sum',
                                                     'KVA_VAL':'max',
                                                     'DOWNSTREAM_KVA_VAL':'max',
                                                     'CREATION_DATETIME' : 'min',
                                                     'ENERGIZED_DATETIME':'max',
                                                     'SUBST_ID': 'min',
                                                     'MAJ_OTG_ID' : 'max'
                                                    })

DF_NUMERICAL.rename(columns={'DOWNSTREAM_CUST_QTY' : 'CUST_QTY'}, inplace=True)

DF_NUMERICAL['INCIDENT_ID'] = DF_NUMERICAL['INCIDENT_ID'].astype(np.int64)
DF_NUMERICAL['CIRCT_ID'] = DF_NUMERICAL['CIRCT_ID'].astype(np.int64)

DF_NUMERICAL['OUTAGE_ID'] = DF_NUMERICAL.apply(lambda x: '%s%s%s%s' % (x['INCIDENT_ID'],
                                                                       x['STRCTUR_NO'],
                                                                       x['CIRCT_ID'],
                                                                       x['DNI_EQUIP_TYPE']), axis=1)

print("****QC Check****")
print("Shape of Numerical columns at 'INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE' Level")
print(DF_NUMERICAL.shape)
print('\n')

DF_NUMERICAL['CREATION_DATETIME'] = pd.to_datetime(DF_NUMERICAL['CREATION_DATETIME'])
DF_NUMERICAL['ENERGIZED_DATETIME'] = pd.to_datetime(DF_NUMERICAL['ENERGIZED_DATETIME'])
DF_NUMERICAL['TTR'] = (DF_NUMERICAL['ENERGIZED_DATETIME'] - DF_NUMERICAL['CREATION_DATETIME']).astype('timedelta64[s]')
DF_NUMERICAL['TTR'] = DF_NUMERICAL['TTR']/60

DF_NUMERICAL['TTR'] = DF_NUMERICAL['TTR'].round(decimals=0)

DF_NUMERICAL['TTR'] = DF_NUMERICAL[DF_NUMERICAL['TTR']>5]

DF_NUMERICAL['Date'] = DF_NUMERICAL.CREATION_DATETIME.dt.date

DF_FINAL = DF_NUMERICAL.groupby(['Date'], as_index=False).agg({'CUST_QTY':'sum',
                                                               'OUTAGE_ID':'count'})

DF_FINAL = DF_FINAL.rename(columns={'OUTAGE_ID':'Outages_recorded'})

DF_PRED = 'SELECT * FROM aes-analytics-0001.mds_outage_restoration.IPL_Storm_Preparations'

DF_PRED = gbq.read_gbq(DF_PRED, project_id="aes-analytics-0001")
DF_PRED.drop_duplicates(inplace=True)

DF_PRED['Date'] = pd.to_datetime(DF_PRED.Date).dt.date

DF_MERGED = DF_FINAL.merge(DF_PRED, how='inner', left_on=['Date'], right_on='Date')

DF_MERGED['Date'] = DF_MERGED['Date'].astype(str)

DF_STORM = 'SELECT * FROM aes-analytics-0001.mds_outage_restoration.IPL_Storm_Diagnostics'

DF_STORM = gbq.read_gbq(DF_STORM, project_id="aes-analytics-0001")
DF_STORM.drop_duplicates(inplace=True)

DF_FINAL = DF_STORM.append(DF_MERGED)

DF_FINAL.drop_duplicates(subset=['Date'],keep='last',inplace=True)

DF_FINAL.to_gbq('mds_outage_restoration.IPL_Storm_Diagnostics',
                 project_id='aes-analytics-0001',
                 chunksize=None, reauth=False, if_exists='replace',
                 auth_local_webserver=False, table_schema=None,
                 location=None, progress_bar=True, credentials=None
                )
