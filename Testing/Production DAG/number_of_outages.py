"""
Dag to predict the number of outages, number of customers, storm recovery and storm duration
"""

import datetime
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import (
    DataProcPySparkOperator)
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow import AirflowException
#from tempfile import NamedTemporaryFile
#from typing import Optional, Union
from airflow.operators.bash_operator  import BashOperator
#from airflow.utils.decorators import apply_defaults
#from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
#from airflow.contrib.operators.bigquery_operator import BigQueryOperator
#from airflow.operators.dagrun_operator  import TriggerDagRunOperator
#from airflow.contrib.operators import bigquery_to_gcs

# ===================Variables=================================
ENV = Variable.get("env")
print(ENV)
JOB_NAME = 'outage_storm_level'
PROJECT = 'aes-datahub-'+ENV
COMPOSER_NAME = 'composer-'+ENV
BUCKET = 'aes-datahub-0002-curated'
COMPOSER_BUCKET = 'us-east4-composer-0002-8d07c42c-bucket'
DATAPROC_BUCKET = 'aes-datahub-0002-temp'
RAW_BUCKET = 'aes-datahub-'+ENV+ '-raw'
CURATED_BUCKET = 'aes-datahub-'+ENV+'-curated'
CLUSTER_NAME = 'outage-python-cluster-0002'
BQ_PROJECT = "aes-analytics-0002"
BQ_DATASET = "mds_outage_restoration"
BQ_TABLE_CHVG = "IPL_Live_Input_Master_Dataset"
BQ_TABLE_FINAL = "IPL_LIVE_PREDICTIONS"
BQ_TABLE_REPO = "IPL_PREDICTIONS"
CLUSTER_NAME_R = 'outage-r-cluster-0002'

YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': YESTERDAY,
    'email_on_failure': True,
    'email': ['ms.gkumar.c@aes.com', 'musigma.bkumar.c@aes.com',
              'eric.nussbaumer@aes.com', 'musigma.dchauhan.c@aes.com'],
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'gcp_conn_id': 'google_cloud_default',
    'subnetwork_uri': COMPOSER_NAME,
    'internal_ip_only': True,
    'region': 'us-east4',
    'zone': 'us-east4-c',
    'labels': {'resource-owner': 'datascience', 'financial-identifier': 'digital'}}

# =================== DAG Definition =================================
with DAG(
        dag_id=JOB_NAME,
        default_args=DEFAULT_ARGS,
        schedule_interval='30 4 * * *'
        ) as dag_def:

    PCA = DataProcPySparkOperator(
        task_id='PCA_calcualtion',
        main='gs://us-east4-composer-0002-8d07c42c-bucket/data'\
            '/Outage_restoration/IPL/Python_scripts/OUTAGE_script.py',
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
        dag=dag_def)

    NUMBER_OF_OUTAGES = BashOperator(
        task_id='NUMBER_OF_OUTAGES',
        bash_command="gcloud beta dataproc jobs submit spark-r    "
                     "/home/airflow/gcs/data/Outage_restoration/IPL/R_scripts/"\
                     "Number_of_Outages_script.R  "\
                     "--cluster=dp-outage-r-0002 --region=us-east4", dag=dag_def)
    OUTAGE_DURATION = BashOperator(
        task_id='OUTAGE_DURATION',
        bash_command="gcloud beta dataproc jobs submit spark-r    "\
                     "/home/airflow/gcs/data/Outage_restoration/IPL/R_scripts/Duration_script.R  "\
                     "--cluster=dp-outage-r-0002 --region=us-east4", dag=dag_def)
    CUST_QTY = BashOperator(
        task_id='CUST_QTY',
        bash_command="gcloud beta dataproc jobs submit spark-r    "\
                   "/home/airflow/gcs/data/Outage_restoration/IPL/R_scripts/Cust_Qty.R  "\
                   "--cluster=dp-outage-r-0002 --region=us-east4", dag=dag_def)
    RECOVERY_DURATION = BashOperator(
        task_id='RECOVERY_DURATION',
        bash_command="gcloud beta dataproc jobs submit spark-r    "\
		           "/home/airflow/gcs/data/Outage_restoration/IPL/R_scripts/Recovery_Duration.R  "\
                   "--cluster=dp-outage-r-0002 --region=us-east4", dag=dag_def)

    OUTPUT_COLLATION = DataProcPySparkOperator(
        task_id='OUTPUT_COLLATION',
        main='gs://us-east4-composer-0002-8d07c42c-bucket'\
            '/data/Outage_restoration/IPL/Python_scripts/Output_collation_pylint.py',
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
        dag=dag_def)

# Create pipeline
[OUTAGE_DURATION, PCA, CUST_QTY, RECOVERY_DURATION]
PCA >> NUMBER_OF_OUTAGES
NUMBER_OF_OUTAGES >> OUTPUT_COLLATION
