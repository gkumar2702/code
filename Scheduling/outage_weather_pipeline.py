'''
Authored by Musigma
Updated: 15 Dec 2020
Tasks: Weather data for next n hours for big query tables
Schedule: At the end of every hour or 60 minutes
Description: Scheduled DAG to process the weather data for
next 6 hrs, 12 hrs and 24 hrs store final csv as big query tables
which will be used for weather profile dashboard
Environment: Composer-0001
Run-time environments: Pyspark,SparkR and python callable
'''

# standard library import
import datetime

# third party import
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import (DataProcPySparkOperator)
from airflow.models import DAG

# specify location of the config file
CONFIG_FILE = 'gs://us-east4-composer-0001-40ca8a74-bucket/data'\
              '/Outage_restoration/IPL/Config_Files/config_storm.ini'

# ===================Variables=================================
ENV = Variable.get("env")

JOB_NAME = 'outage_weather_for_next_n_hours'
COMPOSER_NAME = 'composer-'+ENV
SCRIPT_LOC = 'gs://us-east4-composer-0001-40ca8a74-bucket/'\
             'data/Outage_restoration/IPL/Python_scripts/'

OUTPUT_DATE = datetime.datetime.now().strftime("%Y%m%d")

START_TIME = datetime.datetime(2020, 11, 24, 15, 00, 00)

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': START_TIME,
    'email_on_failure': True,
    'EMAIL': ['musigma.aaggarwal.c@aes.com', 'musigma.bkumar.c@aes.com',
              'eric.nussbaumer@aes.com', 'ms.gkumar.c@aes.com',
              'musigma.skumar.c@aes.com'],
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
        schedule_interval='*/60 * * * *'
) as dag:
    WEATHER_DATA_NEXT_N_HOUR = DataProcPySparkOperator(task_id='WEATHER_DATA_NEXT_N_HOUR',
                                                       main=SCRIPT_LOC+
                                                            'weather_pipeline_script'\
                                    	    				'_pylint.py',
                                                       files=CONFIG_FILE,
                                                       cluster_name='dp-outage-python-0001',
                                                       gcp_conn_id='google_cloud_default',
                                                       region='us-east4',
                                                       job_error_states=['ERROR'],
                                                       dag=dag,
                                                       dataproc_pyspark_properties=None,
                                                       dataproc_pyspark_jars=None,
                                                       arguments=None,
                                                       archives=None,
                                                       pyfiles=None,
                                                       delegate_to=None)

# Create pipeline
WEATHER_DATA_NEXT_N_HOUR
