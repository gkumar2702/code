import os
import datetime
import datetime, re, time
import subprocess
from airflow import models
from airflow.models import Variable
from airflow.contrib.operators.dataflow_operator import GoogleCloudBucketHelper
from airflow.models import BaseOperator
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import os
from itertools import product
import itertools as itertools
import datetime as dt
import numpy as np
from datetime import date,timedelta,timezone
from google.cloud import storage
from google.cloud.storage.blob import Blob
import re
import ast
import json
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule

# ===================Variables=================================
env = Variable.get("env")
print(env)
JOB_NAME = 'outage_weatherdata_pull-0002'
PROJECT = 'aes-datahub-'+env
COMPOSER_NAME = 'composer-'+env
COMPOSER_BUCKET = 'us-east4-composer-0002-8d07c42c-bucket'
DATAPROC_BUCKET = 'aes-datahub-0002-temp'
cluster_name = 'outage-weatherdata-pull-0002'
#LANDING_BUCKET = '{}-landing'.format(PROJECT)
#RAW_BUCKET = '{}-raw'.format(PROJECT)
#CURATED_BUCKET = '{}-curated'.format(PROJECT)
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# =================== DAG Arguments =================================
default_args = {
    'start_date': yesterday,
    'email_on_failure': True,
    'email': 'musigma.bkumar.c@aes.com',
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
        default_args=default_args,
        schedule_interval='0 6 * * *'
) as dag:

  create_cluster = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    cluster_name=cluster_name,
	project_id=PROJECT,
	num_workers=3,
	num_masters=1,
	master_machine_type='n1-highmem-16',
	worker_machine_type='n1-highmem-16',
    storage_bucket=DATAPROC_BUCKET,
	image_version= "1.4",
	init_actions_uris=['gs://aes-datahub-0002-curated/Test/Outage/packages_test.sh'],
	init_action_timeout='150m',
	dag=dag
	)
  Darksky= DataProcPySparkOperator(task_id='DarkSky_datapull',
    main='/home/airflow/gcs/data/Outage_restoration/IPL/Python_scripts/scheduled_weatherdatapull_new.py',
    arguments=None, 
    archives=None, 
    pyfiles=None, 
    files=None, 
    #job_name='{{task.task_id}}_{{ds_nodash}}', 
    cluster_name='outage-weatherdata-pull-0002', 
    dataproc_pyspark_properties=None, 
    dataproc_pyspark_jars=None, 
    gcp_conn_id='google_cloud_default', 
    delegate_to=None, 
    region='us-east4', 
    job_error_states=['ERROR'], 
    dag=dag
    )
  delete_cluster = DataprocClusterDeleteOperator(
    task_id='outage_delete_cluster',
    project_id=PROJECT,
    cluster_name=cluster_name,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
    )

create_cluster >> Darksky >> delete_cluster