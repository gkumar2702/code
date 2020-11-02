'''Script to check the status of ingestion'''
import logging
import time
from datetime import datetime
import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from google.cloud import storage
logging.basicConfig(level=logging.INFO)
SC = SparkContext.getOrCreate()
SPARK = SparkSession(SC)

time.sleep(120)
CURRENT_DATE = datetime.today().strftime('%Y-%m-%d')
logging.info(CURRENT_DATE)
CLIENT = storage.Client()
BUCKET_NAME = 'aes-datahub-0001-raw'
BUCKET = CLIENT.get_bucket(BUCKET_NAME)

BLOBS = BUCKET.list_blobs(prefix='OMS/'+CURRENT_DATE)
DIRLIST = []

for blob in BLOBS:
    DIRLIST.append(str(blob.name))


# In[3]:


# string matching to read tables

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

# In[4]:


BUCKET_NAME = 'gs://aes-datahub-0001-raw/'

logging.info("Current Date %s", CURRENT_DATE)
logging.info('\n')
try:
    LIVE_INCIDENT_DEVICE = SPARK.read.format('CSV').option("header", "true").option(
        "inferSchema", "true").option("delimiter", ",").load(
            BUCKET_NAME +
            _MATCHING_LIVE_INCIDENT_DEVICE[-1]
            ).toPandas()
    logging.info("Path Name LIVE INCIDENT DEVICE %s",
                 BUCKET_NAME + _MATCHING_LIVE_INCIDENT_DEVICE[-1])

    LIVE_INCIDENT = SPARK.read.format('CSV').option("header", "true").option(
        "inferSchema", "true").option("delimiter", ",").load(
            BUCKET_NAME +
            _MATCHING_LIVE_INCIDENT[-1]
            ).toPandas()
    logging.info("Path Name LIVE INCIDENT %s", BUCKET_NAME + _MATCHING_LIVE_INCIDENT[-1])

    LIVE_LOCATION = SPARK.read.format('CSV').option("header", "true").option(
        "inferSchema", "true").option("delimiter", ",").load(
            BUCKET_NAME +
            _MATCHING_LIVE_LOCATION[-1]).toPandas()
    logging.info("Path Name LIVE LOCATION %s", BUCKET_NAME + _MATCHING_LIVE_LOCATION[-1])

    logging.info('\n')

    FILE_READ_LIST = [BUCKET_NAME + _MATCHING_LIVE_INCIDENT_DEVICE[-1], BUCKET_NAME +
                      _MATCHING_LIVE_INCIDENT[-1], BUCKET_NAME + _MATCHING_LIVE_LOCATION[-1]]
    CURRENT_FILE_READ = pd.DataFrame({'Filepath' : file_read_list})

    CURRENT_FILE_READ.head()
except:
    raise Exception("Data ingestion failed at the start of the day")

# In[5]:


try:
    LAST_FILE_READ = pd.read_csv(
        'gs://aes-analytics-0001-curated/Outage_Restoration/Staging/Last_OMS_File_Checker.csv')
except:
    LAST_FILE_READ = pd.DataFrame()
    CURRENT_FILE_READ.to_csv(
        'gs://aes-analytics-0001-curated/Outage_Restoration/Staging/Last_OMS_File_Checker.csv',
        index=False)

if LAST_FILE_READ.empty:
    print("New Files Path's have been stored")
else:
    if ((CURRENT_FILE_READ.Filepath[0] == LAST_FILE_READ.Filepath[0]) or (
            CURRENT_FILE_READ.Filepath[1] == LAST_FILE_READ.Filepath[1]) or (
                CURRENT_FILE_READ.Filepath[2] == LAST_FILE_READ.Filepath[2])):
        raise Exception('No new input data files from OMS')

CURRENT_FILE_READ.to_csv(
    'gs://aes-analytics-0001-curated/Outage_Restoration/Staging/Last_OMS_File_Checker.csv',
    index=False)
