"""
DAG for getting the profiles
"""
import datetime
from airflow.models import Variable, DAG
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator  import BashOperator

# ===================Variables=================================
ENV = Variable.get("env")
print(ENV)
JOB_NAME = 'outage-weathersource-his-profiles-0001'
PROJECT = 'aes-datahub-'+ENV
COMPOSER_NAME = 'composer-'+ENV
COMPOSER_BUCKET = 'us-east4-composer-0001-40ca8a74-bucket'
DATAPROC_BUCKET = 'aes-datahub-0001-temp'
BUCKET = 'aes-analytics-0001-curated'
BQ_PROJECT = "aes-analytics-0001"
BQ_DATASET = "mds_outage_restoration"
BQ_TABLE_REPO = "IPL_HIS_Storm_Profiles"

OUTPUT_DATE = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y%m%d")

YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': YESTERDAY,
    'email_on_failure': True,
    'email': ['musigma.bkumar.c@aes.com', 'eric.nussbaumer@aes.com', 'ms.gkumar.c@aes.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
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
        schedule_interval='0 8 * * *'
) as dag:
    WEATHERSOURCE_IPL = DataProcPySparkOperator(
        task_id='WeatherSource_datapull_IPL',
        main='/home/airflow/gcs/data/Outage_restoration/IPL/'\
        								      'Python_scripts/weathersource_script_pylint.py',
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

    CLUSTER_CLASSIFICATION = BashOperator(
        task_id='Storm_Profiles_Classification',
        bash_command="gcloud beta dataproc jobs submit spark-r    /home/airflow/gcs/data/Outage_"\
		              "restoration/IPL/R_scripts/cluster_classification_R_his.R  --cluster=dp"\
					  "-outage-r-0001 --region=us-east4",
        dag=dag)

    WRITE_CSV_TO_BQ = GoogleCloudStorageToBigQueryOperator(
        task_id='Append_historical_profiles',
        bucket=BUCKET,
        #autodetect=True,
        #schema_fields=None,
        schema_object="Outage_Restoration/Schema/ipl_his_personalities.json",
        skip_leading_rows=1,
        source_objects=["Outage_Restoration/Live_Data_Curation/Historical_Storm_Profiles/"\
		                 "storm_profiles_"+OUTPUT_DATE+".csv"],
        create_disposition='CREATE_IF_NEEDED',
        destination_project_dataset_table=BQ_PROJECT+"."+BQ_DATASET+"."+BQ_TABLE_REPO,
        write_disposition='WRITE_APPEND')
#Pipeline
WEATHERSOURCE_IPL >> CLUSTER_CLASSIFICATION >> WRITE_CSV_TO_BQ
