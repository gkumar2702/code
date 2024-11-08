"""
Authored: Mu Sigma
Updated: 15 Dec 2020
Version: 2
Description: Script to check the status of OMS (Outage Management System)
data ingestion into gcp every two hours 
"""

# standard library imports
import ast
import logging
import time
from configparser import ConfigParser, ExtendedInterpolation
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pandas as pd

# third party imports
from google.cloud import storage

# setup logs
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# config setup
CONFIGPARSER = ConfigParser(interpolation=ExtendedInterpolation())
CONFIGPARSER.read('config_ETR.ini')
logging.info('Config File Loaded')
logging.info('Config File Sections %s', CONFIGPARSER.sections())

# sleep for 2 mins 
time.sleep(120)

# reading settings from config file
try:
    BUCKET_NAME_RAW = CONFIGPARSER['SETTINGS_IPL_FILE_CHK']['BUCKET_NAME_RAW']
    prefix = CONFIGPARSER['SETTINGS_IPL_FILE_CHK']['prefix']
    BUCKET_NAME = CONFIGPARSER['SETTINGS_IPL_FILE_CHK']['BUCKET_NAME']
except:
    raise Exception("Config file failed to load")

# reading file location from config file
LAST_FILE_READ_PATH = CONFIGPARSER['FILE_CHK_PATH']['LAST_FILE_READ_PATH']
CURRENT_FILE_READ_PATH = CONFIGPARSER['FILE_CHK_PATH']['CURRENT_FILE_READ_PATH']

# setting up Blob
CURRENT_DATE = datetime.today().strftime('%Y-%m-%d')
logging.info(CURRENT_DATE)
CLIENT = storage.Client()
BUCKET = CLIENT.get_bucket(BUCKET_NAME_RAW)
BLOBS = BUCKET.list_blobs(prefix=prefix+CURRENT_DATE)
DIRLIST = []
for blob in BLOBS:
    DIRLIST.append(str(blob.name))

# string matching to read INCIDENT_IPL table
_MATCHING_INCIDENT = [s for s in DIRLIST if "INCIDENT_IPL" in s]
_MATCHING_LIVE_INCIDENT = [s for s in _MATCHING_INCIDENT if "HIS" not in s]
logging.info('Names of all the matching live incident tables %s \n', _MATCHING_LIVE_INCIDENT)

# string matching to read INCIDENT_DEVICE_IPL table
_MATCHING_INCIDENT_DEVICE = [s for s in DIRLIST if "INCIDENT_DEVICE_IPL" in s]
_MATCHING_LIVE_INCIDENT_DEVICE = [s for s in _MATCHING_INCIDENT_DEVICE if "HIS" not in s]
logging.info('Names of all the matching live incident tables %s \n', _MATCHING_LIVE_INCIDENT_DEVICE)

# string matching to read LOCATION_IPL table
_MATCHING_LOCATION = [s for s in DIRLIST if "LOCATION_IPL" in s]
_MATCHING_LIVE_LOCATION = [s for s in _MATCHING_LOCATION if "HIS" not in s]
logging.info('Names of all the matching live location tables %s \n',_MATCHING_LIVE_LOCATION)

# Read Live Files in Buckets
logging.info("Current Date %s \n", CURRENT_DATE)

try:
    LIVE_INCIDENT_DEVICE = pd.read_csv(
        BUCKET_NAME+_MATCHING_LIVE_INCIDENT_DEVICE[-1])
    logging.info("Path Name LIVE INCIDENT DEVICE %s \n",
                 BUCKET_NAME+_MATCHING_LIVE_INCIDENT_DEVICE[-1])

    LIVE_INCIDENT = pd.read_csv(
        BUCKET_NAME+_MATCHING_LIVE_INCIDENT[-1])
    logging.info("Path Name LIVE INCIDENT %s \n",
                 BUCKET_NAME + _MATCHING_LIVE_INCIDENT[-1])

    LIVE_LOCATION = pd.read_csv(
        BUCKET_NAME+_MATCHING_LIVE_LOCATION[-1])
    logging.info("Path Name LIVE LOCATION %s \n",
                 BUCKET_NAME + _MATCHING_LIVE_LOCATION[-1])

    FILE_READ_LIST = [BUCKET_NAME + _MATCHING_LIVE_INCIDENT_DEVICE[-1], BUCKET_NAME +
                      _MATCHING_LIVE_INCIDENT[-1], BUCKET_NAME + _MATCHING_LIVE_LOCATION[-1]]
    CURRENT_FILE_READ = pd.DataFrame({'Filepath' : FILE_READ_LIST})
except:
    raise Exception("Data ingestion failed at the start of the day")

# reading the last file from the bucket
try:
    LAST_FILE_READ = pd.read_csv(LAST_FILE_READ_PATH)
except:
    LAST_FILE_READ = pd.DataFrame()
    CURRENT_FILE_READ.to_csv(CURRENT_FILE_READ_PATH,
        index=False)

# logic to check if last file read is same as the current file
if LAST_FILE_READ.empty:
    logging.info("New Files Path's have been stored")
else:
    if ((CURRENT_FILE_READ.Filepath[0] == LAST_FILE_READ.Filepath[0]) or (
            CURRENT_FILE_READ.Filepath[1] == LAST_FILE_READ.Filepath[1]) or (
                CURRENT_FILE_READ.Filepath[2] == LAST_FILE_READ.Filepath[2])):
        raise Exception('No new input data files from OMS \n')

# writing the lastest file
CURRENT_FILE_READ.to_csv(CURRENT_FILE_READ_PATH,
    index=False)
logging.info("File check run successful")
