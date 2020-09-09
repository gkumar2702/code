"""
Created on Tue Apr 27 17:40:53 2020

@author: Gourav
"""

import os
import datetime
from airflow.models import Variable

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
JOB_NAME = 'outage_cluster'
PROJECT = 'aes-datahub-'+env
COMPOSER_NAME = 'composer-'+env
COMPOSER_BUCKET = 'us-east4-composer-0002-8d07c42c-bucket'
DATAPROC_BUCKET = 'aes-datahub-0002-temp'
RAW_BUCKET = 'aes-datahub-'+env+'-raw'
CURATED_BUCKET = 'aes-datahub-'+env+'-curated'
cluster_name = 'outage-cluster'



yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())


# =================== DAG Arguments =================================
default_args = {
    'start_date': yesterday,
    'email_on_failure': True,
    'email': 'gourav.kumar@mu-sigma.com',
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    # Consistent network configs for all tasks
    'gcp_conn_id': 'google_cloud_default',
    'subnetwork_uri': COMPOSER_NAME,
    'internal_ip_only': True,
    'region': 'us-east4',
    'zone': 'us-east4-b'
	}

# =================== DAG Definition =================================
with DAG(
        dag_id=JOB_NAME,
        default_args=default_args,
        schedule_interval='@once'
) as dag:

    

  class CustomDataprocClusterCreateOperator(DataprocClusterCreateOperator):
  
      def __init__(self, *args, **kwargs):
          super(CustomDataprocClusterCreateOperator, self).__init__(*args, **kwargs)
  
      def _build_cluster_data(self):
          cluster_data = super(CustomDataprocClusterCreateOperator, self)._build_cluster_data()
          cluster_data['config']['endpointConfig'] = {
              'enableHttpPortAccess': True
          }
          cluster_data['config']['softwareConfig']['optionalComponents'] = [ 'JUPYTER', 'ANACONDA' ]
          return cluster_data
  
  #Start DataProc Cluster
  dataproc_cluster1 = CustomDataprocClusterCreateOperator(
      task_id='create-' + cluster_name, 
      cluster_name=cluster_name,
      project_id=PROJECT,
  	service_account="composer-0002@aes-datahub-0002.iam.gserviceaccount.com",
      num_workers=3,
      num_masters=1,
      master_machine_type='n1-highmem-8',
      worker_machine_type='n1-highmem-8',
      storage_bucket=DATAPROC_BUCKET,
      gcp_conn_id='google_cloud_default',	
      image_version='1.5',
      init_actions_uris=['gs://aes-datahub-0002-curated/Outage_Restoration/packages/packages_outage_cluster.sh'],
  #    init_action_timeout='100m',
      #idle_delete_ttl=1800,
      dag=dag
  )

dataproc_cluster1