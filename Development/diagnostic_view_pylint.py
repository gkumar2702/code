"""
Authored: Sudheer

Updated: 2 Sep 2020

Description: Backend script to merge actuals with predicted values of the outages
and write as a BQ table

Output table: mds_outage_restoration.IPL_Diagnostic_View
"""
# # **Import Required Packages**

import warnings
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from google.cloud import storage
import pandas_gbq as gbq
warnings.filterwarnings('ignore')
pd.options.mode.chained_assignment = None  # default='warn'

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)
pd.options.display.float_format = '{:.2f}'.format


# import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


# # **Check all Live Files present in Bucket**

# In[99]:


CURRENT_DATE = datetime.today().strftime('%Y-%m-%d')
logging.info(CURRENT_DATE)
CLIENT = storage.Client()
BUCKET_NAME = 'aes-datahub-0001-raw'
BUCKET = CLIENT.get_bucket(BUCKET_NAME)

BLOBS = BUCKET.list_blobs(prefix='OMS/' + CURRENT_DATE)
DIRLIST = []

for blob in BLOBS:
#     logging.info(blob.name)
    DIRLIST.append(str(blob.name))


# In[100]:


# string matching to read tables

_MATCHING_FACILITY = [s for s in DIRLIST if "FACILITY_IPL_Daily" in s]
_MATCHING_LIVE_FACILITY = [s for s in _MATCHING_FACILITY if "HIS" in s]
logging.info(_MATCHING_LIVE_FACILITY)
logging.info('\n')


# In[101]:


logging.info(_MATCHING_LIVE_FACILITY)


# # **Read Live Files in Buckets**

# In[102]:


BUCKET_NAME = 'gs://aes-datahub-0001-raw/'

logging.info(CURRENT_DATE)
logging.info('\n')
logging.info('%s', CURRENT_DATE)
logging.info('\n')

FACILITY = pd.read_csv(BUCKET_NAME + _MATCHING_LIVE_FACILITY[0], encoding="ISO-8859-1", sep=",")
logging.info(BUCKET_NAME + _MATCHING_LIVE_FACILITY[0])
logging.info('HIS Table name')
logging.info('%s', BUCKET_NAME + _MATCHING_LIVE_FACILITY[0])


## QC checks
logging.info("****QC Check****")
logging.info("shape of HIS Incident Device Table")
logging.info(FACILITY.shape)
logging.info("\n")
logging.info("****QC Check****")
logging.info("No of Distinct INCIDENT_ID in INCIDENT_DEVICE Table")
logging.info(FACILITY.FAC_JOB_ID.nunique())
logging.info("\n")



# APPLYING FILTERS FOR CORRECT DATA INPUTS


# customer quantity greater than 0
logging.info('Filter for customer quantity greater than 0')
# logging.info("****QC Check****")
logging.info("Rows left after checking for INCIDENTS whose CUSTOMER QUANTITY IS > 0")
FACILITY = FACILITY[(FACILITY.DOWNSTREAM_CUST_QTY > 0)]
logging.info(FACILITY.shape)
logging.info("\n")

# equip_stn_no is not NCC and not null
logging.info('Filter for equp_stn_no is not NCC or not null')
# logging.info("****QC Check****")
logging.info("Rows left after checking that EQUIP_STN_NO is not from <<NON CONNECTED CUSTOMERS>>")
FACILITY = FACILITY[(FACILITY.EQUIP_STN_NO != '<NCC>') & (FACILITY.EQUIP_STN_NO.notnull())]
logging.info(FACILITY.shape)
logging.info("\n")


# removing NAN from DNI_EQUIP_TYPE, CIRCT_ID, STRCTUR_NO
logging.info('Removing NAN from DNI_EQIP_TYPE, CICRT_ID, STRCTUR_NO')
# logging.info("****QC Check****")
logging.info("Rows left after checking CIRCT_ID,STRCTUR_NO,DNI_EQIP_TYPE is not null")
FACILITY = FACILITY[(FACILITY.CIRCT_ID != 0)]
FACILITY = FACILITY[~FACILITY.CIRCT_ID.isnull()]
FACILITY = FACILITY[~FACILITY.STRCTUR_NO.isnull()]
FACILITY = FACILITY[~FACILITY.DNI_EQUIP_TYPE.isnull()]
logging.info(FACILITY.shape)
logging.info("\n")

# removing CLUE_CD which start with 0 but does not start with 00
logging.info('Removing CLUE_CD which start with 0 but do not start with 00')
# logging.info("****QC Check****")
logging.info("Rows left after filtering for CLUE CODES which start with 0 but do not start with 00")
FACILITY = FACILITY[(FACILITY.CLUE_CD.str[:1] == '0') & (FACILITY.CLUE_CD.str[:2] != '00')]
FACILITY = FACILITY[FACILITY.CLUE_CD != '01']
logging.info(FACILITY.shape)
logging.info("\n")

# removing occurence codes starting with cancel, found ok and duplicate
logging.info('Removing CLUE_CD which start with 0 but do not start with 00')
# logging.info("****QC Check****")
logging.info("Rows left after removing OCCURN_CD which have CANCEL, FOUND OK or DUPLICATE")
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
logging.info(FACILITY.shape)
logging.info("\n")

# # **Aggregate Numerical Columns**

# In[108]:


## START ADS CREATION FOR NUMERICAL COLUMNS AT INCIDENT LEVEL

# 1.1 Aggregate numerical columns at INCIDENT_ID level to keep all unique INCIDNET_ID's
DF_NUMERICAL = FACILITY.groupby(['INCIDENT_ID', 'STRCTUR_NO', 'CIRCT_ID',
                            'DNI_EQUIP_TYPE'], as_index=False).agg({'CALL_QTY':'sum',
                                                                    'DOWNSTREAM_CUST_QTY':'sum',
                                                                    'KVA_VAL':'max',
                                                                    'DOWNSTREAM_KVA_VAL':'max',
                                                                    'CREATION_DATETIME' : 'min',
                                                                    'ENERGIZED_DATETIME':'max',
                                                                    'SUBST_ID': 'min',
                                                                    'MAJ_OTG_ID' : 'max'})

DF_NUMERICAL.rename(columns={'DOWNSTREAM_CUST_QTY' : 'CUST_QTY'}, inplace=True)

DF_NUMERICAL['INCIDENT_ID'] = DF_NUMERICAL['INCIDENT_ID'].astype(np.int64)
DF_NUMERICAL['CIRCT_ID'] = DF_NUMERICAL['CIRCT_ID'].astype(np.int64)

DF_NUMERICAL['OUTAGE_ID'] = DF_NUMERICAL.apply(lambda x: '%s%s%s%s' %
                                               (x['INCIDENT_ID'], x['STRCTUR_NO'],
                                                x['CIRCT_ID'], x['DNI_EQUIP_TYPE']), axis=1)

logging.info("****QC Check****")
logging.info("shape of Numerical columns at 'INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE' Level")
logging.info(DF_NUMERICAL.shape)
logging.info('\n')


# In[109]:


DF_NUMERICAL['CREATION_DATETIME'] = pd.to_datetime(DF_NUMERICAL['CREATION_DATETIME'])
DF_NUMERICAL['ENERGIZED_DATETIME'] = pd.to_datetime(DF_NUMERICAL['ENERGIZED_DATETIME'])
DF_NUMERICAL['TTR'] = DF_NUMERICAL['ENERGIZED_DATETIME'] - DF_NUMERICAL['CREATION_DATETIME']
DF_NUMERICAL['TTR'] = DF_NUMERICAL['TTR'].astype('timedelta64[s]')
DF_NUMERICAL['TTR'] = DF_NUMERICAL['TTR']/60


# QC checks every value should be one

# In[110]:


DF_NUMERICAL['TTR'] = DF_NUMERICAL['TTR'].round(decimals=0)


# In[111]:


def check_level(group):
    '''check the level'''
    logging.info(len(group))


#DF_NUMERICAL.groupby(['INCIDENT_ID','STRCTUR_NO','CIRCT_ID','DNI_EQUIP_TYPE']).apply(check_level)


# In[112]:


logging.info(DF_NUMERICAL.shape)


# In[113]:


DF_FINAL = DF_NUMERICAL[['OUTAGE_ID',
                         'INCIDENT_ID',
                         'STRCTUR_NO',
                         'CIRCT_ID',
                         'DNI_EQUIP_TYPE',
                         'CREATION_DATETIME',
                         'ENERGIZED_DATETIME',
                         'TTR']]
DF_FINAL.drop_duplicates(subset='OUTAGE_ID', inplace=True)

# In[114]:


logging.info(DF_FINAL.shape)


# In[115]:

DF_PRED = 'SELECT * FROM aes-analytics-0001.mds_outage_restoration.IPL_Predictions'
DF_PRED = gbq.read_gbq(DF_PRED, project_id="aes-analytics-0001")
PREDICTIONS = list(DF_PRED['OUTAGE_ID'].unique())
DF_PRED.drop_duplicates(subset='OUTAGE_ID', inplace=True)

# In[117]:


DF_PRED['Creation_Time'] = pd.to_datetime(DF_PRED['Creation_Time'])
DF_PRED['Creation_Time'] = DF_PRED['Creation_Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
DF_PRED['Creation_Time'] = pd.to_datetime(DF_PRED['Creation_Time'])


# In[118]:


DF_MERGED = DF_FINAL.merge(DF_PRED, how='left', left_on=['OUTAGE_ID'],
                           right_on=['OUTAGE_ID'], suffixes=('', '_y'))


# In[119]:


DF_DIAGNOSTIC = DF_MERGED.dropna()


# In[120]:


DF_DIAGNOSTIC.drop(DF_DIAGNOSTIC.filter(regex='_y').columns, axis=1, inplace=True)



# In[122]:


DF_DIAGNOSTIC['TTR'] = DF_DIAGNOSTIC['TTR'].astype(np.int64)
DF_DIAGNOSTIC['ETR'] = DF_DIAGNOSTIC['ETR'].astype(np.int64)

#reading the diagnostic table
DF_DIAG = 'SELECT * FROM aes-analytics-0001.mds_outage_restoration.IPL_Diagnostic_View'
DF_DIAG = gbq.read_gbq(DF_DIAG, project_id="aes-analytics-0001")
#prev_predictions=list(DF_DIAG['OUTAGE_ID'].unique())
DF_DIAG.drop_duplicates(inplace=True)
logging.info("Previous PREDICTIONS read successfully")



shape = DF_DIAGNOSTIC.shape[0]
if shape == 0:
    raise Exception('No new Additions, All outages are already fed in the previos run')

##Appending the two tables

DF_FINAL = DF_DIAG.append(DF_DIAGNOSTIC)

## Dropping duplicates and storing the recent entries

DF_FINAL.drop_duplicates(subset=['OUTAGE_ID'], keep='first', inplace=True)
DF_FINAL.reset_index(drop=True, inplace=True)

DF_FINAL['TTR'] = DF_FINAL['TTR'].astype(np.float64)
DF_FINAL['ETR'] = DF_FINAL['ETR'].astype(np.float64)
# # **Write table to Big Query**

DF_FINAL.to_gbq('mds_outage_restoration.IPL_Diagnostic_View', project_id='aes-analytics-0001',
                chunksize=None, reauth=False,
                if_exists='replace', auth_local_webserver=False, table_schema=None,
                location=None, progress_bar=True, credentials=None)
