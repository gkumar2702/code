'''
Author - Mu Sigma
Updated: 15 Dec 2020
Version: 2
Tasks: Check for the last file in the OMS data bucket
Schedule: Every 2 hours
Environment: Composer-0001
Run-time environments: Pyspark,SparkR and python 3.7 callable
'''

# standard library imports 
import datetime

# third party imports 
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import (DataProcPySparkOperator)
from airflow.models import DAG

# ===================Variables=================================
ENV = Variable.get("env")
# name of the job performed in airflow
JOB_NAME = 'outage_IPL_ingestion_status'
# name of the composer to be used
COMPOSER_NAME = 'composer-'+ENV
# name of the python clsuter being used
CLUSTER_NAME = 'dp-outage-python-0001'
# location of the scripts
SCRIPT_LOC = 'gs://us-east4-composer-0001-40ca8a74-bucket/data/'\
             'Outage_restoration/IPL/Python_scripts/'
# specify location of the config file
CONFIG_FILE = 'gs://us-east4-composer-0001-40ca8a74-bucket/data/'\
              'Outage_restoration/IPL/Config_Files/config_ETR.ini'

OUTPUT_DATE = datetime.datetime.now().strftime("%Y%m%d")

# setting up start time 
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
START_TIME = datetime.datetime(2020, 12, 15, 12, 10, 00)

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': START_TIME,
    'email_on_failure': True,
    'email': ['ms.gkumar.c@aes.com', 'musigma.bkumar.c@aes.com', 'musigma.aaggarwal.c@aes.'\
              'com', 'musigma.dchauhan.c@aes.com', 'musigma.skumar.c@aes.com'],
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    # Consistent network configs for all tasks
    'gcp_conn_id': 'google_cloud_default',
    'subnetwork_uri': COMPOSER_NAME,
    'internal_ip_only': True,
    'region': 'us-east4',
    'zone': 'us-east4-c'}

# =================== DAG Definition =================================
with DAG(
        dag_id=JOB_NAME,
        default_args=DEFAULT_ARGS,
        schedule_interval='0 */2 * * *'
) as dag:
    FILE_CHECK = DataProcPySparkOperator(task_id='OMS_Ingestion_Status_Check',
                                         main=SCRIPT_LOC+'oms_filechecker.py',
                                         cluster_name=CLUSTER_NAME,
                                         gcp_conn_id='google_cloud_default',
                                         region='us-east4',
                                         job_error_states=['ERROR'],
                                         dag=dag,
                                         files=CONFIG_FILE,
                                         dataproc_pyspark_properties=None,
                                         dataproc_pyspark_jars=None,
                                         delegate_to=None,
                                         arguments=None,
                                         archives=None,
                                         pyfiles=None)

# Create pipeline
FILE_CHECK
