import os
import datetime
from airflow import models
from airflow.models import Variable
from airflow.contrib.operators.dataflow_operator import GoogleCloudBucketHelper
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from itertools import product
from google.cloud import storage
from google.cloud.storage.blob import Blob
from airflow.contrib.operators.dataproc_operator import (DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator)
from airflow.operators.email_operator import EmailOperator
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule

# ===================Variables=================================
env = Variable.get("env")
print(env)
JOB_NAME = 'outage-cluster-creation-dag'
PROJECT = 'aes-datahub-'+env
COMPOSER_NAME = 'composer-'+env
COMPOSER_BUCKET = 'us-east4-composer-0002-8d07c42c-bucket'
DATAPROC_BUCKET = 'aes-datahub-0002-temp'
cluster_name_r = 'outage-r-cluster-0002'
cluster_name_python = 'outage-python-cluster-0002'
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())


# =================== DAG Arguments =================================
default_args = {
    'start_date': yesterday,
    'email_on_failure': True,
    'email': 'musigma.bkumar.c@AES.COM',
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
        schedule_interval='@once'
) as dag:
	create_cluster_r = DataprocClusterCreateOperator(
		task_id='create_dataproc_r_cluster',
		cluster_name=cluster_name_r,
		project_id=PROJECT,
        service_account="composer-0002@aes-datahub-0002.iam.gserviceaccount.com",
		num_workers=3,
		num_masters=1,
		master_machine_type='n1-standard-8',
		worker_machine_type='n1-standard-8',
		storage_bucket=DATAPROC_BUCKET,
		image_version= "preview",
		init_actions_uris=['gs://aes-datahub-0002-curated/Outage_Restoration/packages/requiredpackages_r.sh'],
		init_action_timeout='150m',
		dag=dag
		)

	create_cluster_python = DataprocClusterCreateOperator(
		task_id='create_dataproc_py_cluster',
		cluster_name=cluster_name_python,
		project_id=PROJECT,
        service_account="composer-0002@aes-datahub-0002.iam.gserviceaccount.com",
		num_workers=3,
		num_masters=1,
		master_machine_type='n1-standard-8',
		worker_machine_type='n1-standard-8',
		storage_bucket=DATAPROC_BUCKET,
		image_version= "1.4",
		init_actions_uris=['gs://aes-datahub-0002-curated/Outage_Restoration/packages/packages_test.sh'],
		init_action_timeout='150m',
		dag=dag
		)

[create_cluster_r,create_cluster_python]