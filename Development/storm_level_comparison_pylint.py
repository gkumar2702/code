"""
Authored: Mu Sigma
Updated: 26 Nov 2020
Version: 1.5
Tasks: Compute actuals no of outages and customers affected
and aggregate to day level to compare with predictions
"""

# standard library imports 
import warnings
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from pandas.io import gbq
import pandas_gbq as gbq

# third party improts
from google.cloud import storage

# setup logs
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

CURRENT_DATE = datetime.today().strftime('%Y-%m-%d')
CLIENT = storage.Client()
BUCKET_NAME = 'aes-datahub-0001-raw'
BUCKET = CLIENT.get_bucket(BUCKET_NAME)

BLOBS = BUCKET.list_blobs(prefix='OMS/'+CURRENT_DATE)
DIRLIST = []

for blob in BLOBS:
    DIRLIST.append(str(blob.name))

_MATCHING_FACILITY = [s for s in DIRLIST if "FACILITY_IPL_Daily" in s]
_MATCHING_LIVE_FACILITY = [s for s in _MATCHING_FACILITY if "HIS" in s]

BUCKET_NAME = 'gs://aes-datahub-0001-raw/'

logging.info('Todays Date %s \n', CURRENT_DATE)

FACILITY = pd.read_csv(BUCKET_NAME + _MATCHING_LIVE_FACILITY[0], encoding="ISO-8859-1", sep=",")

## QC checks
logging.info('****QC Check****')
logging.info('Shape of HIS Incident Device Table %s \n', FACILITY.shape)
logging.info('****QC Check****')
logging.info('No of Distinct INCIDENT_ID in INCIDENT_DEVICE Table %s \n', FACILITY.FAC_JOB_ID.nunique())

# **Apply Required Filters**
# APPLYING FILTERS FOR CORRECT DATA INPUTS ###########
# customer quantity greater than 0
logging.info('Filter for customer quantity greater than 0')
FACILITY = FACILITY[(FACILITY.DOWNSTREAM_CUST_QTY > 0)]
logging.info("Rows left after checking for INCIDENTS whose CUSTOMER QUANTITY IS > 0 %s \n", FACILITY.shape)

# equip_stn_no is not NCC and not null
logging.info('Filter for equp_stn_no is not NCC or not null')
# logging.info("****QC Check****")
logging.info("Rows left after checking that EQUIP_STN_NO is not from <<NON CONNECTED CUSTOMERS>> %s \n", FACILITY.shape)
FACILITY = FACILITY[(FACILITY.EQUIP_STN_NO != '<NCC>') & (FACILITY.EQUIP_STN_NO.notnull())]

# removing NAN from DNI_EQUIP_TYPE, CIRCT_ID, STRCTUR_NO
logging.info('Removing NAN from DNI_EQIP_TYPE, CICRT_ID, STRCTUR_NO')
FACILITY = FACILITY[(FACILITY.CIRCT_ID != 0)]
FACILITY = FACILITY[~FACILITY.CIRCT_ID.isnull()]
FACILITY = FACILITY[~FACILITY.STRCTUR_NO.isnull()]
FACILITY = FACILITY[~FACILITY.DNI_EQUIP_TYPE.isnull()]
logging.info("Rows left after checking CIRCT_ID is not\
    0 and not null STRCTUR_NO is not null and DNI_EQIP_TYPE is not null %s \n", FACILITY.shape)

# removing CLUE_CD which start with 0 but does not start with 00
logging.info('Removing CLUE_CD which start with 0 but do not start with 00')
FACILITY = FACILITY[(FACILITY.CLUE_CD.str[:1] == '0') & (FACILITY.CLUE_CD.str[:2] != '00')]
FACILITY = FACILITY[FACILITY.CLUE_CD != '01']
FACILITY = FACILITY[FACILITY.CLUE_CD != '09OD']
logging.info("Rows left after filtering for CLUE CODES which start with 0 but do not start with 00 %s \n", FACILITY.shape)

# removing occurence codes starting with cancel, found ok and duplicate
logging.info('Removing CLUE_CD which start with 0 but do not start with 00')
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
logging.info("Rows left after removing OCCURN_CD which have\
    descriptions starting with CANCEL, FOUND OK or DUPLICATE %s \n", FACILITY.shape)

DF_NUMERICAL = FACILITY.copy(deep=True)

# **Aggregate Numerical Columns**
'''
Create outage id columns
remove and filter outages only after the trigger
'''

## START ADS CREATION FOR NUMERICAL COLUMNS AT INCIDENT LEVEL
# 1.1 Aggregate numerical columns at INCIDENT_ID level to keep all unique 'INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE'
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

logging.info("****QC Check****")
logging.info("Shape of Numerical columns at 'INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE' Level %s \n", DF_NUMERICAL.shape)

DF_NUMERICAL['CREATION_DATETIME'] = pd.to_datetime(DF_NUMERICAL['CREATION_DATETIME'])
DF_NUMERICAL['ENERGIZED_DATETIME'] = pd.to_datetime(DF_NUMERICAL['ENERGIZED_DATETIME'])
DF_NUMERICAL['TTR'] = (DF_NUMERICAL.ENERGIZED_DATETIME - DF_NUMERICAL.CREATION_DATETIME).dt.total_seconds().div(60).round(4)
DF_NUMERICAL['TTR'] = DF_NUMERICAL['TTR'].round(decimals=0)

DF_NUMERICAL = DF_NUMERICAL[DF_NUMERICAL['TTR']>30]

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


DF_MERGED.to_gbq('mds_outage_restoration.IPL_Storm_Diagnostics',
                 project_id='aes-analytics-0001',
                 chunksize=None, reauth=False, if_exists='replace',
                 auth_local_webserver=False, table_schema=None,
                 location=None, progress_bar=True, credentials=None
                )
