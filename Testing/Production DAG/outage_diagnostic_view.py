"""
DAG for diagnostic view dashboard
"""

import datetime
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.models import DAG

# ===================Variables=================================
ENV = Variable.get("env")
print(ENV)
JOB_NAME = 'outage-diagnostic-backend'
PROJECT = 'aes-datahub-'+ENV
COMPOSER_NAME = 'composer-'+ENV
COMPOSER_BUCKET = 'us-east4-composer-0002-8d07c42c-bucket'
DATAPROC_BUCKET = 'aes-datahub-0002-temp'
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': YESTERDAY,
    'email_on_failure': True,
    'email': ['musigma.bkumar.c@aes.com', 'ms.gkumar.c@aes.com', 'eric.nussbaumer@aes.com'],
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
        schedule_interval='0 6 * * *'
) as dag:
    DIAGNOSTIC = DataProcPySparkOperator(task_id='Diagnostic_Daily_Append',
                                         main='/home/airflow/gcs/data/Outage_restoration/'\
										       'IPL/Python_scripts/diagnostic_view.py',
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
                                         dag=dag
                                        )

    STORM_LEVEL = DataProcPySparkOperator(task_id='Storm_Diagnostic_Daily_Append',
                                          main='/home/airflow/gcs/data/Outage_restoration/IPL/'\
										        'Python_scripts/storm_level_comparison_pylint.py',
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
                                          dag=dag
                                          )

DIAGNOSTIC >> STORM_LEVEL
