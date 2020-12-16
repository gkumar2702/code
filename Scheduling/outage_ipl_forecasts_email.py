'''
Authored: Mu Sigma
Updated: 15 Dec 2020
Version: 2
Description: Python script to create email structure and send
email everyday with forecasts from IPL Storm Preparedness model
(# Outages and # Customers) and daily weather forecast from
weather source API
Tasks: Trigger daily email alert with forecasts from IPL preparedness model and weather
source API
Schedule: Everyday at 1200 UTC
'''

# standard library
import datetime

# third party imports 
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import (DataProcPySparkOperator)
from airflow.models import DAG

# ===================Variables=================================
CONFIG_FILE = 'gs://us-east4-composer-0001-40ca8a74-bucket/data/Outage_restoration/IPL/Config_Files/config_storm.ini'
ENV = Variable.get("env")
JOB_NAME = 'outage-ipl-forecasts-email-alert'
COMPOSER_NAME = 'composer-'+ENV
CLUSTER_NAME = 'dp-outage-python-0001'
SCRIPT_LOC = 'gs://us-east4-composer-0001-40ca8a74-bucket/data'\
             '/Outage_restoration/IPL/Python_scripts/'
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

YEAR_MONTH = datetime.datetime.today().strftime('%Y-%m')
TODAY = datetime.datetime.today().strftime('%Y-%m-%d')
TODAY_NAME = datetime.datetime.today().strftime('%Y%m%d')


# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': YESTERDAY,
    'email_on_failure': True,
    'email': ['musigma.bkumar.c@aes.com', 'musigma.dchauhan.c@aes.com',
              'musigma.skumar.c@aes.com', 'musigma.aaggarwal.c@aes.com',
              'ms.gkumar.c@aes.com'],
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    # Consistent network configs for all tasks
    'gcp_conn_id': 'google_cloud_default',
    'subnetwork_uri': COMPOSER_NAME,
    'internal_ip_only': True,
    'region': 'us-east4',
    'zone': 'us-east4-c'
    }

# =================== DAG Definition =================================
with DAG(
        dag_id=JOB_NAME,
        default_args=DEFAULT_ARGS,
        schedule_interval='0 12 * * *'
) as dag:
    TRIGGER_ALERT = DataProcPySparkOperator(task_id='trigger-daily-email',
                                            main=SCRIPT_LOC+
                                            'forecasts_email_alert_pylint.py',
                                            files=CONFIG_FILE,
                                            cluster_name=CLUSTER_NAME,
                                            gcp_conn_id='google_cloud_default',
                                            region='us-east4',
                                            job_error_states=['ERROR'],
                                            dataproc_pyspark_properties=None,
                                            dataproc_pyspark_jars=None,
                                            arguments=None,
                                            archives=None,
                                            pyfiles=None,
                                            delegate_to=None,
                                            dag=dag)

# Create pipeline
TRIGGER_ALERT
