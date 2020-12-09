'''
Author - Mu Sigma
Updated: 9 Dec 2020
Version: 2
Tasks: Live data processing, weather data addition,
post processing and profiles addition, Regression run
Schedule: At the end of every 30 minutes
Description: Scheduled DAG to process OMS data arriving at 30 min interval,
adding the weather data and predicting using RDS model objects
Environment: Composer-0002
Run-time environments: Pyspark,SparkR and python 3.7 callable
'''

# standard library imports
import datetime as dt

# third party imports
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import (DataProcPySparkOperator)
from airflow.models import DAG

# ===================Variables=================================
ENV = Variable.get("env")

# name of the composer to be used
COMPOSER_NAME = 'composer-'+ENV
# name of the job performed in airflow
JOB_NAME = 'outage_end_end'
# location of the scripts
SCRIPT_LOC = 'gs://us-east4-composer-0001-40ca8a74-bucket/data'\
    '/Outage_restoration/IPL/Python_scripts/'
# specify location of the config file
CONFIG_FILE = 'gs://aes-analytics-0001-curated/Outage_Restoration'\
    '/Live_Data_Curation/Config/confignew0001.ini'
# name of the python clsuter being used
CLUSTER = 'dp-outage-python-0001'

START_TIME = dt.datetime(2020, 12, 8, 19, 50, 00)

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': START_TIME,
    'email_on_failure': True,
    'EMAIL': ['musigma.bkumar.c@aes.com', 'ms.gkumar.c@aes.com', 'eric.nussbaumer@aes.com', 'musigma.skumar.c@aes.com'],
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=1),
    # Consistent network configs for all tasks
    'gcp_conn_id': 'google_cloud_default',
    'subnetwork_uri': COMPOSER_NAME,
    'internal_ip_only': True,
    'region': 'us-east4',
    'zone': 'us-east4-c',
    'labels': {'resource-owner': 'datascience',
               'financial-identifier': 'digital'}}



# =================== DAG Definition =================================
with DAG(
        dag_id=JOB_NAME,
        default_args=DEFAULT_ARGS,
        schedule_interval='*/32 * * * *'
) as dag:
    OMS_LIVE_DATASET_PREPROCESSING = DataProcPySparkOperator(task_id=
                                                             'OMS_LIVE_DATASET_PREPROCESSING',
                                                             main=
                                                             SCRIPT_LOC+
                                                             'live_oms_preprocessing_pylint.py',
                                                             files=CONFIG_FILE,
                                                             cluster_name=CLUSTER,
                                                             gcp_conn_id='google_cloud_default',
                                                             region='us-east4',
                                                             job_error_states=['ERROR'],
                                                             dag=dag,
                                                             arguments=None,
                                                             archives=None,
                                                             pyfiles=None,
                                                             dataproc_pyspark_properties=None,
                                                             dataproc_pyspark_jars=None,
                                                             delegate_to=None,
                                                             email_on_failure=None)

    WEATHER_DATA_COLLATION = DataProcPySparkOperator(task_id=
                                                     'WEATHER_DATA_COLLATION',
                                                     main=SCRIPT_LOC+
                                                     'weather_source_data_collation_pylint.py',
                                                     files=CONFIG_FILE,
                                                     cluster_name=CLUSTER,
                                                     gcp_conn_id='google_cloud_default',
                                                     region='us-east4',
                                                     job_error_states=['ERROR'],
                                                     dag=dag,
                                                     arguments=None,
                                                     archives=None,
                                                     pyfiles=None,
                                                     dataproc_pyspark_properties=None,
                                                     dataproc_pyspark_jars=None,
                                                     delegate_to=None)

    CURATED_DATASET_CREATION = DataProcPySparkOperator(task_id=
                                                       'CURATED_DATASET_CREATION',
                                                       main=SCRIPT_LOC+
                                                       'curated_dataset_creation_pylint.py',
                                                       files=CONFIG_FILE,
                                                       cluster_name=CLUSTER,
                                                       gcp_conn_id='google_cloud_default',
                                                       region='us-east4',
                                                       job_error_states=['ERROR'],
                                                       dag=dag,
                                                       arguments=None,
                                                       archives=None,
                                                       pyfiles=None,
                                                       dataproc_pyspark_properties=None,
                                                       dataproc_pyspark_jars=None,
                                                       delegate_to=None,
                                                       email_on_failure=None)

    REGRESSION_CODE_RUN = DataProcPySparkOperator(task_id='REGRESSION_CODE_RUN',
                                                  main=SCRIPT_LOC+
                                                  'load_predict_pylint.py',
                                                  files=CONFIG_FILE,
                                                  cluster_name=CLUSTER,
                                                  gcp_conn_id='google_cloud_default',
                                                  region='us-east4',
                                                  job_error_states=['ERROR'],
                                                  dag=dag,
                                                  arguments=None,
                                                  archives=None,
                                                  pyfiles=None,
                                                  dataproc_pyspark_properties=None,
                                                  dataproc_pyspark_jars=None,
                                                  delegate_to=None)
# Create pipeline for tasks in order
OMS_LIVE_DATASET_PREPROCESSING >> WEATHER_DATA_COLLATION >> CURATED_DATASET_CREATION >> REGRESSION_CODE_RUN
