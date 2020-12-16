"""
Authored: Mu Sigma
Updated: 15 Dec 2020
Version: 2
Tasks: Compute actuals no of outages and customers affected
and aggregate to day level to compare with predictions
Output table: mds_outage_restoration.IPL_Storm_Diagnostics
"""

# standard library imports 
import ast
import warnings
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from pandas.io import gbq
import pandas_gbq as gbq
from configparser import ConfigParser, ExtendedInterpolation

# third party imports
from google.cloud import storage

# setup logs
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

# reading settings from configuration file
try:
    BUCKET_NAME_RAW = CONFIGPARSER['SETTINGS_IPL_DIAG']['BUCKET_NAME_RAW']
    BUCKET_NAME = CONFIGPARSER['SETTINGS_IPL_DIAG']['BUCKET_NAME']
    prefix=CONFIGPARSER['SETTINGS_IPL_DIAG']['prefix']
    PROJECT_ID = CONFIGPARSER['SETTINGS_IPL_DIAG']['PROJECT_ID']
    OP_BQ_SCHEMA = CONFIGPARSER['STORM_LVL_COMP']['OP_BQ_SCHEMA']
    OP_BQ_TAB = CONFIGPARSER['STORM_LVL_COMP']['OP_BQ_TAB']
    logging.info("Settings loaded from configuration file")
except:
    raise Exception(" Config file failed to load")

# reading list and other variables from configuration files
OCCUR_REMOV = ast.literal_eval(CONFIGPARSER.get("DIAG_VIEW", "OCCURN_REMOV"))
OCCUR_REMOV = list(OCCUR_REMOV)
DF_PRED = CONFIGPARSER['STORM_LVL_COMP']['DF_PRED']
logging.info("Settings loaded from configuration file")

# fetching live data for current date
CURRENT_DATE = datetime.today().strftime('%Y-%m-%d')
CLIENT = storage.Client()
BUCKET = CLIENT.get_bucket(BUCKET_NAME_RAW)

BLOBS = BUCKET.list_blobs(prefix=prefix+CURRENT_DATE)
DIRLIST = []

for blob in BLOBS:
    DIRLIST.append(str(blob.name))
_MATCHING_FACILITY = [s for s in DIRLIST if "FACILITY_IPL_Daily" in s]
_MATCHING_LIVE_FACILITY = [s for s in _MATCHING_FACILITY if "HIS" in s]
logging.info('Todays Date %s \n', CURRENT_DATE)

try:
    FACILITY = pd.read_csv(BUCKET_NAME + _MATCHING_LIVE_FACILITY[0], encoding="ISO-8859-1", sep=",")
except:
    raise Exception("Failed to load live Facility table")
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

# Apply Required Filters
# APPLYING FILTERS FOR CORRECT DATA INPUTS
# customer quantity greater than 0
logging.info('Filter for customer quantity greater than 0')
FACILITY = FACILITY[(FACILITY.DOWNSTREAM_CUST_QTY > 0)]
logging.info("Rows left after checking for INCIDENTS whose CUSTOMER QUANTITY IS > 0 %s \n", FACILITY.shape)

# equip_stn_no is not NCC and not null
logging.info('Filter for equp_stn_no is not NCC or not null \n')
logging.info("Rows left after checking that EQUIP_STN_NO is not from <<NON CONNECTED CUSTOMERS>> %s \n", FACILITY.shape)
FACILITY = FACILITY[(FACILITY.EQUIP_STN_NO != '<NCC>') & (FACILITY.EQUIP_STN_NO.notnull())]

# removing NAN from DNI_EQUIP_TYPE, CIRCT_ID, STRCTUR_NO
logging.info('Removing NAN from DNI_EQIP_TYPE, CICRT_ID, STRCTUR_NO')
FACILITY = FACILITY[(FACILITY.CIRCT_ID != 0)]
FACILITY = FACILITY[~FACILITY.CIRCT_ID.isnull()]
FACILITY = FACILITY[~FACILITY.STRCTUR_NO.isnull()]
FACILITY = FACILITY[~FACILITY.DNI_EQUIP_TYPE.isnull()]
logging.info("Rows left after checking CIRCT_ID is not 0 and not null STRCTUR_NO is not null and DNI_EQIP_TYPE is not null %s \n", FACILITY.shape)

# removing CLUE_CD which start with 0 but does not start with 00
logging.info('Removing CLUE_CD which start with 0 but do not start with 00')
FACILITY = FACILITY[(FACILITY.CLUE_CD.str[:1] == '0') & (FACILITY.CLUE_CD.str[:2] != '00')]
FACILITY = FACILITY[FACILITY.CLUE_CD != '01']
FACILITY = FACILITY[FACILITY.CLUE_CD != '09OD']
logging.info("Rows left after filtering for CLUE CODES which start with 0 but do not start with 00 %s \n", FACILITY.shape)

# removing occurence codes starting with cancel, found ok and duplicate
logging.info('Removing CLUE_CD which start with 0 but do not start with 00')
FACILITY = FACILITY[~(FACILITY.OCCURN_CD.isin(OCCUR_REMOV))]
logging.info("Rows left after removing OCCURN_CD which have    descriptions starting with CANCEL, FOUND OK or DUPLICATE %s \n", FACILITY.shape)

DF_NUMERICAL = FACILITY.copy(deep=True)


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

# renaming columns 
DF_NUMERICAL.rename(columns={'DOWNSTREAM_CUST_QTY' : 'CUST_QTY'}, inplace=True)

# converting required columns to int
DF_NUMERICAL['INCIDENT_ID'] = DF_NUMERICAL['INCIDENT_ID'].astype(np.int64)
DF_NUMERICAL['CIRCT_ID'] = DF_NUMERICAL['CIRCT_ID'].astype(np.int64)
DF_NUMERICAL['OUTAGE_ID'] = DF_NUMERICAL.apply(lambda x: '%s%s%s%s' % (x['INCIDENT_ID'],
                                                                       x['STRCTUR_NO'],
                                                                       x['CIRCT_ID'],
                                                                       x['DNI_EQUIP_TYPE']), axis=1)
# QC Checks
logging.info("****QC Check****")
logging.info("Shape of Numerical columns at 'INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE' Level %s \n", DF_NUMERICAL.shape)
DF_NUMERICAL['CREATION_DATETIME'] = pd.to_datetime(DF_NUMERICAL['CREATION_DATETIME'])
DF_NUMERICAL['ENERGIZED_DATETIME'] = pd.to_datetime(DF_NUMERICAL['ENERGIZED_DATETIME'])

# calculating the TTR column
DF_NUMERICAL['TTR'] = (DF_NUMERICAL.ENERGIZED_DATETIME - DF_NUMERICAL.CREATION_DATETIME).dt.total_seconds().div(60).round(4)
DF_NUMERICAL['TTR'] = DF_NUMERICAL['TTR'].round(decimals=0)
DF_NUMERICAL = DF_NUMERICAL[DF_NUMERICAL['TTR']>30]

# creating the date column
DF_NUMERICAL['Date'] = DF_NUMERICAL.CREATION_DATETIME.dt.date

# grouping the outages by date to create actual customer quantity and no of outages columns
DF_FINAL = DF_NUMERICAL.groupby(['Date'], as_index=False).agg({'CUST_QTY':'sum',
                                                               'OUTAGE_ID':'count'})
DF_FINAL = DF_FINAL.rename(columns={'OUTAGE_ID':'Outages_recorded'})

# reading historical predictions from bigQuery table
try:
    DF_PRED = gbq.read_gbq(DF_PRED, project_id=PROJECT_ID)
except:
    raise Exception("Failed to load predictions from Bigquery table")
DF_PRED.drop_duplicates(inplace=True)
DF_PRED['Date'] = pd.to_datetime(DF_PRED.Date).dt.date

# merging Actuals with predictions
DF_MERGED = DF_FINAL.merge(DF_PRED, how='inner', left_on=['Date'], right_on='Date')
DF_MERGED['Date'] = DF_MERGED['Date'].astype(str)

# writing to bigQuery table
OP_BQ_TAB_PATH = """{schema}.{output_table}""".format(schema=OP_BQ_SCHEMA,
                                                      output_table=OP_BQ_TAB)
try:
    DF_MERGED.to_gbq(OP_BQ_TAB_PATH, project_id=PROJECT_ID,
                     chunksize=None, reauth=False, if_exists='replace',
                     auth_local_webserver=False, table_schema=None,
                     location=None, progress_bar=True, credentials=None)
except:
    raise Exception("Failed to write into Bigquery table")
