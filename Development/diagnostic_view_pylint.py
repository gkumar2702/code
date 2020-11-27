"""
Authored: Mu Sigma
Updated: 26 Nov 2020
Version: 1.5
Description: Backend script to merge actuals with predicted values of the outages
and write as a BQ table
Output table: mds_outage_restoration.IPL_Diagnostic_View
"""

# standard library imports 
import warnings
import logging
import datetime as dt
from datetime import datetime
import pandas as pd
import numpy as np
from google.cloud import storage
import pandas_gbq as gbq

# import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

# **Check all Live Files present in Bucket**
CURRENT_DATE = datetime.today().strftime('%Y-%m-%d')
logging.info(CURRENT_DATE)
CLIENT = storage.Client()
BUCKET_NAME = 'aes-datahub-0001-raw'
BUCKET = CLIENT.get_bucket(BUCKET_NAME)

BLOBS = BUCKET.list_blobs(prefix='OMS/' + CURRENT_DATE)
DIRLIST = []

for blob in BLOBS:
    DIRLIST.append(str(blob.name))

# string matching to read tables
_MATCHING_FACILITY = [s for s in DIRLIST if "FACILITY_IPL_Daily" in s]
_MATCHING_LIVE_FACILITY = [s for s in _MATCHING_FACILITY if "HIS" in s]
logging.info('HIS FACILITY_IPL_Daily TABLE %s \n', _MATCHING_LIVE_FACILITY)

# **Read Live Files in Buckets**
BUCKET_NAME = 'gs://aes-datahub-0001-raw/'
logging.info('Todays Date %s \n', CURRENT_DATE)

FACILITY = pd.read_csv(BUCKET_NAME + _MATCHING_LIVE_FACILITY[0], encoding="ISO-8859-1", sep=",")
logging.info(BUCKET_NAME + _MATCHING_LIVE_FACILITY[0])
logging.info('HIS Table name %s \n', BUCKET_NAME + _MATCHING_LIVE_FACILITY[0])

## QC checks
logging.info('****QC Check****')
logging.info('shape of HIS Incident Device Table %s \n', FACILITY.shape)
logging.info('****QC Check****')
logging.info('No of Distinct INCIDENT_ID in INCIDENT_DEVICE Table %s \n', FACILITY.FAC_JOB_ID.nunique())

# APPLYING FILTERS FOR CORRECT DATA INPUTS
# customer quantity greater than 0
logging.info('Filter for customer quantity greater than 0')
logging.info("Rows left after checking for INCIDENTS whose CUSTOMER QUANTITY IS > 0")
FACILITY = FACILITY[(FACILITY.DOWNSTREAM_CUST_QTY > 0)]
logging.info('Shape of Facility job table %s \n', FACILITY.shape)

# equip_stn_no is not NCC and not null
logging.info('Filter for equp_stn_no is not NCC or not null')
logging.info("Rows left after checking that EQUIP_STN_NO is not from <<NON CONNECTED CUSTOMERS>>")
FACILITY = FACILITY[(FACILITY.EQUIP_STN_NO != '<NCC>') & (FACILITY.EQUIP_STN_NO.notnull())]
logging.info('Shape of Facility job table %s \n', FACILITY.shape)

# removing NAN from DNI_EQUIP_TYPE, CIRCT_ID, STRCTUR_NO
logging.info('Removing NAN from DNI_EQIP_TYPE, CICRT_ID, STRCTUR_NO')
logging.info("Rows left after checking CIRCT_ID,STRCTUR_NO,DNI_EQIP_TYPE is not null")
FACILITY = FACILITY[(FACILITY.CIRCT_ID != 0)]
FACILITY = FACILITY[~FACILITY.CIRCT_ID.isnull()]
FACILITY = FACILITY[~FACILITY.STRCTUR_NO.isnull()]
FACILITY = FACILITY[~FACILITY.DNI_EQUIP_TYPE.isnull()]
logging.info('Shape of Facility job table %s \n', FACILITY.shape)

# removing CLUE_CD which start with 0 but does not start with 00
logging.info('Removing CLUE_CD which start with 0 but do not start with 00')
logging.info("Rows left after filtering for CLUE CODES which start with 0 but do not start with 00")
FACILITY = FACILITY[(FACILITY.CLUE_CD.str[:1] == '0') & (FACILITY.CLUE_CD.str[:2] != '00')]
FACILITY = FACILITY[FACILITY.CLUE_CD != '01']
FACILITY = FACILITY[FACILITY.CLUE_CD != '09OD']
logging.info('Shape of Facility job table %s \n', FACILITY.shape)

# removing occurence codes starting with cancel, found ok and duplicate
logging.info('Removing CLUE_CD which start with 0 but do not start with 00 \n')

logging.info('Rows left after removing OCCURN_CD which have CANCEL, FOUND OK or DUPLICATE \n')
OCCUR_REMOV = [30003001, 33003301, 33003302, 34003400, 34003401, 34003402, 34003403, 34003404,
               34003405, 34003406, 34003407, 34003408, 34003409, 35003500,
               35003501, 35003502, 35003503, 35003504, 35003505, 35003506, 35003507, 35003508,
               36003600, 36003601, 36003602, 36003603, 36003604, 36003605,
               36003606, 36003607, 36003608, 37003703, 38003802, 38003803, 38003804, 38003807,
               39003910, 41004100, 41004101, 41004102, 48004800, 48004802,
               48004803, 49004900, 49004901, 49004902, 50005000, 50005001, 50005002, 52005200,
               52005201, 52005202, 52005203, 52005204, 52005205, 52005206,
               52005207, 53005300, 53005301, 53005302, 53005303, 53005304, 53005305, 53005306,
               53005307, 53005308, 53005309, 53005310, 54005400, 54005401,
               54005402, 54005403, 54005404, 54005405, 34003410, 30003000, 36503650, 36503651,
               36503652, 36503653, 36503654, 36503655, 36503656, 36503657,
               36503658]
FACILITY = FACILITY[~(FACILITY.OCCURN_CD.isin(OCCUR_REMOV))]
logging.info('Shape of Facility job table %s \n', FACILITY.shape)

# **Aggregate Numerical Columns**
# start ads creation at numerical level 'INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID', 'DNI_EQUIP_TYPE'
# 1.1 Aggregate numerical columns at INCIDENT_ID level to keep all unique INCIDNET_ID's
DF_NUMERICAL = FACILITY.groupby(['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID',
                            'DNI_EQUIP_TYPE'], as_index=False).agg({'CALL_QTY':'sum',
                                                                    'DOWNSTREAM_CUST_QTY':'sum',
                                                                    'KVA_VAL':'max',
                                                                    'DOWNSTREAM_KVA_VAL':'max',
                                                                    'CREATION_DATETIME' : 'min',
                                                                    'ENERGIZED_DATETIME':'max',
                                                                    'SUBST_ID': 'min',
                                                                    'MAJ_OTG_ID' : 'max',
                                                                    'ETR_DATETIME' : 'max'})

DF_NUMERICAL.rename(columns={'DOWNSTREAM_CUST_QTY' : 'CUST_QTY'}, inplace=True)

DF_NUMERICAL['INCIDENT_ID'] = DF_NUMERICAL['INCIDENT_ID'].astype(np.int64)
DF_NUMERICAL['CIRCT_ID'] = DF_NUMERICAL['CIRCT_ID'].astype(np.int64)

DF_NUMERICAL['OUTAGE_ID'] = DF_NUMERICAL.apply(lambda x: '%s%s%s%s' %
                                               (x['INCIDENT_ID'], x['STRCTUR_NO'],
                                                x['CIRCT_ID'], x['DNI_EQUIP_TYPE']), axis=1)

logging.info("****QC Check****")
logging.info("Shape of Numerical columns at 'INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE' Level %s \n", DF_NUMERICAL.shape)

DF_NUMERICAL['CREATION_DATETIME'] = pd.to_datetime(DF_NUMERICAL['CREATION_DATETIME'])
DF_NUMERICAL['ENERGIZED_DATETIME'] = pd.to_datetime(DF_NUMERICAL['ENERGIZED_DATETIME'])
DF_NUMERICAL['ETR_DATETIME'] = pd.to_datetime(DF_NUMERICAL['ETR_DATETIME'])
DF_NUMERICAL['TTR'] = (DF_NUMERICAL.ENERGIZED_DATETIME - DF_NUMERICAL.CREATION_DATETIME).dt.total_seconds().div(60).round(4)

DF_NUMERICAL['OMS_ETR'] = (DF_NUMERICAL.ETR_DATETIME - DF_NUMERICAL.CREATION_DATETIME).dt.total_seconds().div(60).round(4)
DF_NUMERICAL['OMS_ETR'] = DF_NUMERICAL['OMS_ETR']/60


# QC checks every value should be one
DF_NUMERICAL['TTR'] = DF_NUMERICAL['TTR'].round(decimals=0)
DF_NUMERICAL['OMS_ETR'] = DF_NUMERICAL['OMS_ETR'].round(decimals=0)

def check_level(group):
    '''check the level'''
    logging.info(len(group))

logging.info(DF_NUMERICAL.shape)

DF_FINAL = DF_NUMERICAL[['OUTAGE_ID',
                         'INCIDENT_ID',
                         'STRCTUR_NO',
                         'CIRCT_ID',
                         'DNI_EQUIP_TYPE',
                         'CREATION_DATETIME',
                         'ENERGIZED_DATETIME',
                         'TTR']]

DF_FINAL.drop_duplicates(subset='OUTAGE_ID',keep='last', inplace=True)

DF_ETR = DF_NUMERICAL[['OUTAGE_ID',
                         'INCIDENT_ID',
                         'STRCTUR_NO',
                         'CIRCT_ID',
                         'DNI_EQUIP_TYPE',
                         'CREATION_DATETIME',
                         'ENERGIZED_DATETIME',
                         'TTR',
                         'ETR_DATETIME',
                         'OMS_ETR']]

DF_ETR.drop_duplicates(subset='OUTAGE_ID',keep='last', inplace=True)
logging.info(DF_FINAL.shape)

DF_PRED = 'SELECT * FROM aes-analytics-0001.mds_outage_restoration.IPL_Predictions'
DF_PRED = gbq.read_gbq(DF_PRED, project_id="aes-analytics-0001")
PREDICTIONS = list(DF_PRED['OUTAGE_ID'].unique())
DF_PRED.drop_duplicates(subset='OUTAGE_ID', inplace=True)

DF_PRED['Creation_Time'] = pd.to_datetime(DF_PRED['Creation_Time'])
DF_PRED['Creation_Time'] = DF_PRED['Creation_Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
DF_PRED['Creation_Time'] = pd.to_datetime(DF_PRED['Creation_Time'])


DF_MERGED = DF_FINAL.merge(DF_PRED, how='left', left_on=['OUTAGE_ID'],
                           right_on=['OUTAGE_ID'], suffixes=('', '_y'))


# accounting for missing outages in the live pipeline
DF_MERGED = DF_MERGED[DF_MERGED.TTR>30]
DF_DIAGNOSTIC = DF_MERGED.dropna()

DF_DIAGNOSTIC.drop(DF_DIAGNOSTIC.filter(regex='_y').columns, axis=1, inplace=True)
DF_DIAGNOSTIC['TTR'] = DF_DIAGNOSTIC['TTR'].astype(np.int64)
DF_DIAGNOSTIC['ETR'] = DF_DIAGNOSTIC['ETR'].astype(np.int64)

## adding OMS ETRs to the diagnostic
DF_ETR_BS = DF_ETR[DF_ETR.OMS_ETR>=0]
DF_ETR_STORM = DF_ETR[DF_ETR.OMS_ETR<0]
DF_ETR_STORM.OMS_ETR = np.nan
DF_ETR_FINAL = DF_ETR_BS.append(DF_ETR_STORM)
DF_DIAGNOSTIC = DF_DIAGNOSTIC.merge(DF_ETR_FINAL[['OUTAGE_ID', 'ETR_DATETIME', 'OMS_ETR']],
                          how='left', left_on=['OUTAGE_ID'],
                          right_on=['OUTAGE_ID'])

# reading the diagnostic table
DF_DIAG = 'SELECT * FROM aes-analytics-0001.mds_outage_restoration.IPL_Diagnostic_View'
DF_DIAG = gbq.read_gbq(DF_DIAG, project_id="aes-analytics-0001")

DF_DIAG.drop_duplicates(inplace=True)
logging.info("Previous PREDICTIONS read successfully")

shape = DF_DIAGNOSTIC.shape[0]
if shape != 0:
    pass
else:
    raise Exception('No new Additions, All outages are already fed in the previos run')

# appending the two tables
DF_FINAL = DF_DIAG.append(DF_DIAGNOSTIC)

# dropping duplicates and storing the recent entries
DF_FINAL.drop_duplicates(subset=['OUTAGE_ID'], keep='last', inplace=True)
DF_FINAL.reset_index(drop=True, inplace=True)

# keeping the data type consistent
DF_FINAL['TTR'] = DF_FINAL['TTR'].astype(np.float64)
DF_FINAL['ETR'] = DF_FINAL['ETR'].astype(np.float64)
DF_FINAL['OMS_ETR'] = DF_FINAL['OMS_ETR'].astype(np.float64)

# **Write table to Big Query**
DF_FINAL.to_gbq('mds_outage_restoration.IPL_Diagnostic_View', project_id='aes-analytics-0001',
                chunksize=None, reauth=False,
                if_exists='replace', auth_local_webserver=False, table_schema=None,
                location=None, progress_bar=True, credentials=None)
