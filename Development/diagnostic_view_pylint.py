"""
Authored: Mu Sigma
Updated: 15 Dec 2020
Version: 2
Description: Backend script to merge actuals with predicted values of the outages
from the OMS system and model predictions and write as a BQ table
Output table: mds_outage_restoration.IPL_Diagnostic_View
"""

# standard library imports
import ast
import logging
import datetime as dt
from datetime import datetime
import pandas as pd
import numpy as np
import pandas_gbq as gbq
from configparser import ConfigParser, ExtendedInterpolation

# third party imports
from google.cloud import storage

# Setup logs
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# read config file
CONFIGPARSER = ConfigParser(interpolation=ExtendedInterpolation())
CONFIGPARSER.read('config_ETR.ini')
logging.info('Config File Loaded')
logging.info('Config File Sections %s', CONFIGPARSER.sections())

# reading settings from config file
try:
    BUCKET_NAME_RAW = CONFIGPARSER['SETTINGS_IPL_DIAG']['BUCKET_NAME_RAW']
    BUCKET_NAME = CONFIGPARSER['SETTINGS_IPL_DIAG']['BUCKET_NAME']
    prefix=CONFIGPARSER['SETTINGS_IPL_DIAG']['prefix']
    PROJECT_ID = CONFIGPARSER['SETTINGS_IPL_DIAG']['PROJECT_ID']
    OP_BQ_SCHEMA = CONFIGPARSER['SETTINGS_IPL_DIAG']['OP_BQ_SCHEMA']
    OP_BQ_TAB = CONFIGPARSER['SETTINGS_IPL_DIAG']['OP_BQ_TAB']
    logging.info("Settings loaded from configuration file")
except:
    raise Exception('Config Files failed to load')


# reading required lists from config files
OCCUR_REMOV = ast.literal_eval(CONFIGPARSER.get("DIAG_VIEW", "OCCURN_REMOV"))
OCCUR_REMOV = list(OCCUR_REMOV)
NUMERICAL_COLS = ast.literal_eval(CONFIGPARSER.get("DIAG_VIEW", "NUMERICAL_COLS"))
NUMERICAL_COLS = list(NUMERICAL_COLS)
NUMERICAL_COLS_ETR = ast.literal_eval(CONFIGPARSER.get("DIAG_VIEW", "NUMERICAL_COLS_ETR"))
NUMERICAL_COLS_ETR = list(NUMERICAL_COLS_ETR)
DF_PRED = CONFIGPARSER['DIAG_VIEW']['DF_PRED']
DF_DIAG = CONFIGPARSER['DIAG_VIEW']['DF_DIAG']
logging.info("lists and other variables loaded from configuration file")

# check all Live Files present in Bucket
CURRENT_DATE = datetime.today().strftime('%Y-%m-%d')
logging.info(CURRENT_DATE)
CLIENT = storage.Client()
BUCKET = CLIENT.get_bucket(BUCKET_NAME_RAW)

BLOBS = BUCKET.list_blobs(prefix=prefix + CURRENT_DATE)
DIRLIST = []

for blob in BLOBS:
    DIRLIST.append(str(blob.name))

# string matching to read tables
_MATCHING_FACILITY = [s for s in DIRLIST if "FACILITY_IPL_Daily" in s]
_MATCHING_LIVE_FACILITY = [s for s in _MATCHING_FACILITY if "HIS" in s]
logging.info('HIS FACILITY_IPL_Daily TABLE %s \n', _MATCHING_LIVE_FACILITY)

# read live files present in buckets
logging.info('Todays Date %s \n', CURRENT_DATE)
try:
    FACILITY = pd.read_csv(BUCKET_NAME + _MATCHING_LIVE_FACILITY[0], encoding="ISO-8859-1", sep=",")
except:
    raise Exception("Failed to load live Facility data")
logging.info(BUCKET_NAME + _MATCHING_LIVE_FACILITY[0])
logging.info('HIS Table name %s \n', BUCKET_NAME + _MATCHING_LIVE_FACILITY[0])

SHAPE = FACILITY.shape[0]
if SHAPE != 0:
    pass
else:
    raise Exception('Facility table contains 0 rows')

# QC checks
logging.info('****QC Check****')
logging.info('Shape of HIS Incident Device Table %s \n', FACILITY.shape)
logging.info('****QC Check****')
logging.info('No of Distinct INCIDENT_ID in INCIDENT_DEVICE Table %s \n', FACILITY.FAC_JOB_ID.nunique())

# applying filters for correct data input
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

# rename downstream customer quantity to customer quantity
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
    '''
    Input - Dataframe with all data operations
    Output - 1 1 1 1 if the level of the table is as desired
    '''
    logging.info(len(group))

logging.info('Checking shape of the numerical columns \n', DF_NUMERICAL.shape)


DF_FINAL = DF_NUMERICAL[NUMERICAL_COLS]
DF_FINAL.drop_duplicates(subset='OUTAGE_ID',keep='last', inplace=True)
DF_ETR = DF_NUMERICAL[NUMERICAL_COLS_ETR]
DF_ETR.drop_duplicates(subset='OUTAGE_ID',keep='last', inplace=True)
logging.info('Checking shape of the DF_FINAL dataframe ', DF_FINAL.shape)

try:
    DF_PRED = gbq.read_gbq(DF_PRED, project_id=PROJECT_ID)
except:
    raise Exception("Failed to load predictions from Bigquery table")
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

# adding OMS ETRs to the diagnostic
DF_ETR_BS = DF_ETR[DF_ETR.OMS_ETR>=0]
DF_ETR_STORM = DF_ETR[DF_ETR.OMS_ETR<0]
DF_ETR_STORM.OMS_ETR = np.nan
DF_ETR_FINAL = DF_ETR_BS.append(DF_ETR_STORM)
DF_DIAGNOSTIC = DF_DIAGNOSTIC.merge(DF_ETR_FINAL[['OUTAGE_ID', 'ETR_DATETIME', 'OMS_ETR']],
                          how='left', left_on=['OUTAGE_ID'],
                          right_on=['OUTAGE_ID'])

# reading the diagnostic table
try:
    DF_DIAG = gbq.read_gbq(DF_DIAG, project_id=PROJECT_ID)
except:
    raise Exception("Failed to load diagnostic table from Bigquery table")
DF_DIAG.drop_duplicates(inplace=True)
logging.info("Previous PREDICTIONS read successfully")

shape = DF_DIAGNOSTIC.shape[0]
if shape != 0:
    pass
else:
    raise Exception('No new Additions, All outages are already fed in the previous run')

# appending the two tables
DF_FINAL = DF_DIAG.append(DF_DIAGNOSTIC)

# dropping duplicates and storing the recent entries
DF_FINAL.drop_duplicates(subset=['OUTAGE_ID'], keep='last', inplace=True)
DF_FINAL.reset_index(drop=True, inplace=True)

# keeping the data type consistent
DF_FINAL['TTR'] = DF_FINAL['TTR'].astype(np.float64)
DF_FINAL['ETR'] = DF_FINAL['ETR'].astype(np.float64)
DF_FINAL['OMS_ETR'] = DF_FINAL['OMS_ETR'].astype(np.float64)

# Write table to Big Query
try:
    OP_BQ_TAB_PATH = """{schema}.{output_table}""".format(schema=OP_BQ_SCHEMA,
                                                          output_table=OP_BQ_TAB)
    DF_FINAL.to_gbq(OP_BQ_TAB_PATH, project_id=PROJECT_ID,
                    chunksize=None, reauth=False,
                    if_exists='replace', auth_local_webserver=False, table_schema=None,
                    location=None, progress_bar=True, credentials=None)
except:
    raise Exception("Failed to write to Bigquery table")
