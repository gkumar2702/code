import os
import datetime
import subprocess
from airflow import models
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import (DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator)
from airflow.contrib.operators.dataflow_operator import GoogleCloudBucketHelper
from airflow.models import BaseOperator
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from datetime import date,timedelta,timezone
from google.cloud import storage
from airflow.operators.email_operator import EmailOperator
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule

# ===================Variables=================================
env = Variable.get("env")
print(env)
JOB_NAME = 'outage-weathersource-data-pull-0002'
PROJECT = 'aes-datahub-'+env
COMPOSER_NAME = 'composer-'+env
COMPOSER_BUCKET = 'us-east4-composer-0002-8d07c42c-bucket'
DATAPROC_BUCKET = 'aes-datahub-0002-temp'
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# =================== DAG Arguments =================================
default_args = {
    'start_date': yesterday,
    'email_on_failure': True,
    'email': 'musigma.bkumar.c@aes.com',
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
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
        default_args=default_args,
        schedule_interval='0 4 * * *'
) as dag:
  weathersource_IPL= DataProcPySparkOperator(task_id='WeatherSource_datapull_IPL',
    main='/home/airflow/gcs/data/Outage_restoration/IPL/Python_scripts/weathersource_script.py',
    arguments=None, 
    archives=None, 
    pyfiles=None, 
    files=None, 
    cluster_name='outage-python-cluster-0002', 
    dataproc_pyspark_properties=None, 
    dataproc_pyspark_jars=None, 
    gcp_conn_id='google_cloud_default', 
    delegate_to=None, 
    region='us-east4', 
    job_error_states=['ERROR'], 
    dag=dag
    )
  weathersource_DPL = DataProcPySparkOperator(task_id='WeatherSource_datapull_DPL',
    main='/home/airflow/gcs/data/Outage_restoration/DPL/Python_scripts/Weathersource-DPL.py',
    arguments=None, 
    archives=None, 
    pyfiles=None, 
    files=None,  
    cluster_name='outage-python-cluster-0002', 
    dataproc_pyspark_properties=None, 
    dataproc_pyspark_jars=None, 
    gcp_conn_id='google_cloud_default', 
    delegate_to=None, 
    region='us-east4', 
    job_error_states=['ERROR'], 
    dag=dag
    ) 
   
  cluster_classification = BashOperator(
    task_id='Storm_Profiles_Classificatiion',
    bash_command = "gcloud beta dataproc jobs submit spark-r    /home/airflow/gcs/data/Outage_restoration/IPL/R_scripts/cluster_classification_R_ws.R  --cluster=outage-r-cluster-0002 --region=us-east4",
    dag=dag
    )

#Pipeline
[weathersource_IPL,weathersource_DPL]
weathersource_IPL >> cluster_classification