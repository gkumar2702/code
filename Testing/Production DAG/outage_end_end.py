"""
Authored by Sudheer

Updated: 14 Oct 2020

Tasks: Live data processing, weather data addition,
post processing and profiles addition, Regression run

Schedule: At the end of every 30 minutes

Description: Scheduled DAG to process OMS data arriving at 30 min interval,
adding the weather data and predicting using RDS model objects

Environment: Composer-0002

Run-time environments: Pyspark,SparkR and python callable


"""
import datetime
#from tempfile import NamedTemporaryFile
#from typing import Optional, Union
from airflow.models import Variable
#from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
#from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.dataproc_operator import (DataProcPySparkOperator)
from airflow.models import DAG
#from airflow.utils.trigger_rule import TriggerRule
#from airflow import models
#from airflow import AirflowException
#from airflow.models import BaseOperator
#from airflow.operators.bash_operator  import BashOperator
#from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
#from airflow.operators.dummy_operator import DummyOperator
#from airflow.utils.decorators import apply_defaults
#from airflow.contrib.hooks.sftp_hook import SFTPHook
#from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
#from airflow.contrib.operators.bigquery_operator import BigQueryOperator
#from airflow.operators.dagrun_operator  import TriggerDagRunOperator
#from airflow.contrib.operators import bigquery_to_gcs

# ===================Variables=================================
ENV = Variable.get("env")
print(ENV)
JOB_NAME = 'outage_END_END'
PROJECT = 'aes-datahub-'+ENV
COMPOSER_NAME = 'composer-'+ENV
BUCKET = 'aes-analytics-0002-curated'
COMPOSER_BUCKET = 'us-east4-composer-0002-8d07c42c-bucket'
DATAPROC_BUCKET = 'aes-datahub-0002-temp'
#LANDING_BUCKET = '{}-landing'.format(PROJECT)
RAW_BUCKET = 'aes-datahub-'+ENV+'-raw'
CLUSTER_NAME = 'outage-python-cluster-0002'
#subnetwork_uri= 'composer-'+ENV,
BQ_PROJECT = "aes-analytics-0002"
BQ_DATASET = "mds_outage_restoration"
BQ_TABLE_CHVG = "IPL_Live_Input_Master_Dataset"
BQ_TABLE_FINAL = "IPL_LIVE_PREDICTIONS"
BQ_TABLE_REPO = "IPL_PREDICTIONS"
CLUSTER_NAME_R = 'outage-r-cluster-0002'
EMAIL = ['musigma.bkumar.c@aes.com']
BQ_DATASET_NAME = "mds_outage_restoration"
# intermediate_table = BQ_PROJECT+'.'+BQ_DATASET_NAME +'.IPL_Live_Master_Dataset'
# output_file='gs://aes-analytics-0002-curated/Outage_Restoration/Staging/\
# IPL_Live_Master_Dataset.csv'

OUTPUT_DATE = datetime.datetime.now().strftime("%Y%m%d")


YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
START_TIME = datetime.datetime(2020, 10, 27, 14, 00, 00)

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': START_TIME,
    'email_on_failure': True,
    'EMAIL': ['musigma.bkumar.c@aes.com', 'ms.gkumar.c@aes.com', 'eric.nussbaumer@aes.com'],
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
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
    OMS_LIVE_DATASET_COLLATION = DataProcPySparkOperator(task_id='OMS_LIVE_DATA_COLLATION',
                                                         main='gs://us-east4-composer-0002-8d07c4'\
                                                               '2c-bucket/data/'\
                                                               'Outage_restoration/IPL/'\
															   'Python_scripts/live_oms_'\
															   'preprocessing_pylint.py',
                                                         arguments=None,
                                                         archives=None,
                                                         pyfiles=None,
                                                         files=None,
                                                         cluster_name='dp-outage-python-0002',
                                                         dataproc_pyspark_properties=None,
                                                         dataproc_pyspark_jars=None,
                                                         gcp_conn_id='google_cloud_default',
                                                         delegate_to=None,
                                                         region='us-east4',
                                                         job_error_states=['ERROR'],
                                                         dag=dag,
                                                         email_on_failure=None)

    WEATHER_DATA_COLLATION = DataProcPySparkOperator(task_id='WEATHER_DATA_COLLATION',
                                                     main='gs://us-east4-composer-0002-'\
                        							       '8d07c42c-bucket/data/Outage_'\
                        								   'restoration/IPL/Python_scripts/'\
                        								   'weather_source_data_collation'\
                        	    						   '_pylint.py',
                                                     arguments=None,
                                                     archives=None,
                                                     pyfiles=None,
                                                     files=None,
                                                     cluster_name='dp-outage-python-0002',
                                                     dataproc_pyspark_properties=None,
                                                     dataproc_pyspark_jars=None,
                                                     gcp_conn_id='google_cloud_default',
                                                     delegate_to=None,
                                                     region='us-east4',
                                                     job_error_states=['ERROR'],
                                                     dag=dag)

    CURATED_DATASET_CREATION = DataProcPySparkOperator(task_id='CURATED_DATASET_CREATION',
                                                       main='gs://us-east4-composer-0002-8d07c42c'\
                                                             '-bucket/data/Outage_restoration/IPL'\
                                                             '/Python_scripts/curated_dataset_'\
													         'creation_pylint.py',
                                                       arguments=None,
                                                       archives=None,
                                                       pyfiles=None,
                                                       files=None,
                                                       cluster_name='dp-outage-python-0002',
                                                       dataproc_pyspark_properties=None,
                                                       dataproc_pyspark_jars=None,
                                                       gcp_conn_id='google_cloud_default',
                                                       delegate_to=None,
                                                       region='us-east4',
                                                       job_error_states=['ERROR'],
                                                       dag=dag,
                                                       email_on_failure=None)

    REGRESSION_CODE_RUN = DataProcPySparkOperator(task_id='Regression_Run',
                                                  main='gs://us-east4-composer-0002-8d07c42c-'\
                                                        'bucket/data/Outage_restoration/IPL/'\
                                                        'Python_scripts/load_predict_pylint.py',
                                                  arguments=None,
                                                  archives=None,
                                                  pyfiles=None,
                                                  files=None,
                                                  cluster_name='dp-outage-python-0002',
                                                  dataproc_pyspark_properties=None,
                                                  dataproc_pyspark_jars=None,
                                                  gcp_conn_id='google_cloud_default',
                                                  delegate_to=None,
                                                  region='us-east4',
                                                  job_error_states=['ERROR'],
                                                  dag=dag)
# Create pipeline
OMS_LIVE_DATASET_COLLATION >> WEATHER_DATA_COLLATION >> CURATED_DATASET_CREATION >> REGRESSION_CODE_RUN
