"""
DAG
"""
import datetime
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import (DataProcPySparkOperator)
from airflow.models import DAG
from airflow import models
from airflow import AirflowException

# ===================Variables=================================
ENV = Variable.get("env")
print(ENV)
JOB_NAME = 'outage_IPL_ingestion_status'
PROJECT = 'aes-datahub-'+ENV
COMPOSER_NAME = 'composer-'+ENV
BUCKET = 'aes-analytics-0001-curated'
COMPOSER_BUCKET = 'us-east4-composer-0001-40ca8a74-bucket'
DATAPROC_BUCKET = 'aes-datahub-0001-temp'
RAW_BUCKET = 'aes-datahub-'+ENV+'-raw'
CLUSTER_NAME = 'dp-outage-python-0001'
CLUSTER_NAME_R = 'outage-r-cluster-0001'
EMAIL = ['musigma.bkumar.c@aes.com']

OUTPUT_DATE = datetime.datetime.now().strftime("%Y%m%d")


YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
START_TIME = datetime.datetime(2020, 10, 23, 9, 00, 00)

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': START_TIME,
    'email_on_failure': True,
    'email': ['ms.gkumar.c@aes.com', 'musigma.bkumar.c@aes.com', 'musigma.aaggarwal.c@aes.'\
              'com', 'musigma.dchauhan.c@aes.com'],
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
                                         main='gs://us-east4-composer-0001-40ca8a74-bucket/data/Ou'\
 										     'tage_restoration/IPL/Python_scripts/oms_filechecker.py',
                                         arguments=None,
                                         archives=None,
                                         pyfiles=None,
                                         files=None,
                                         cluster_name='dp-outage-python-0001',
                                         dataproc_pyspark_properties=None,
                                         dataproc_pyspark_jars=None,
                                         gcp_conn_id='google_cloud_default',
                                         delegate_to=None,
                                         region='us-east4',
                                         job_error_states=['ERROR'],
                                         dag=dag)

# Create pipeline
FILE_CHECK
