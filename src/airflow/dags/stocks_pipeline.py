from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
from docker.types import Mount

PROJECT_ROOT = Variable.get("PROJECT_ROOT", default_var=".")

default_args = {
    'owner': 'financial-data-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

SPARK_MASTER_URL = Variable.get("SPARK_MASTER_URL", default_var="spark://spark-master:7077")

SPARK_COMMAND = lambda script_path: [
    'spark-submit',
    '--master', SPARK_MASTER_URL,
    '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
    '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
    '--driver-memory', '2g',
    '--executor-memory', '2g',
    script_path
]

dag = DAG(
    'stocks_daily_pipeline',
    default_args=default_args,
    description='Daily ingestion of stock data from Cryptostock API',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['financial-data', 'cryptostock'],
)


# Common mounts used by all tasks
COMMON_MOUNTS = [
    Mount(source=f'{PROJECT_ROOT}/src', target='/opt/src', type='bind'),
    Mount(source=f'{PROJECT_ROOT}/datalake', target='/datalake', type='bind'),
    Mount(source=f'{PROJECT_ROOT}/src/raw_datasets', target='/opt/raw_datasets', type='bind'),
]

DOCKER_URL = Variable.get("DOCKER_URL", default_var="unix://var/run/docker.sock")

# Task 1: Fetch data from stocks API
fetch_data = DockerOperator(
    task_id='fetch_stocks_data',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command='python /opt/src/api_ingestion/fetch_last_day_data.py',
    docker_url=DOCKER_URL,
    network_mode='data-engineering-project_default',
    mounts=COMMON_MOUNTS,
    environment={
        'ALPACA_API_KEY': Variable.get("ALPACA_API_KEY"),
        'ALPACA_SECRET_KEY': Variable.get("ALPACA_SECRET_KEY")
    },
    dag=dag,
)

# Task 2: Process data with Spark
process_spark = DockerOperator(
    task_id='process_with_spark',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command=[
        'spark-submit',
        '--master', 'spark://spark-master:7077',
        '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
        '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
        '--driver-memory', '2g',
        '--executor-memory', '2g',
        '/opt/src/ingestion/stocks_spark_ingestion.py'
    ],
    docker_url=DOCKER_URL,
    network_mode='data-engineering-project_default',
    mounts=COMMON_MOUNTS,
    environment={
        'SPARK_MASTER_URL': 'spark://spark-master:7077'
    },
    dag=dag,
)

create_gold_layer = DockerOperator(
    task_id='create_unified_gold_layer',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command=SPARK_COMMAND('/opt/src/ingestion/unified_gold_layer.py'),
    docker_url=DOCKER_URL,
    network_mode='data-engineering-project_default',
    mounts=COMMON_MOUNTS,
    environment={
        'SPARK_MASTER_URL': SPARK_MASTER_URL
    },
    dag=dag,
)

validate_data = DockerOperator(
    task_id='validate_gold_layer_and_export_duckdb',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command='python /opt/src/validation/validate_gold_layer.py',
    docker_url=DOCKER_URL,
    network_mode='data-engineering-project_default',
    mounts=COMMON_MOUNTS,
    dag=dag,
)

cleanup = DockerOperator(
    task_id='clean_first_cryptostocks_raw_jsons',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command='python /opt/src/api_ingestion/clean_cryptostocks_folder.py',
    docker_url=DOCKER_URL,
    network_mode='data-engineering-project_default',
    mounts=COMMON_MOUNTS,
    dag=dag,
)

# Task dependencies
fetch_data >> process_spark >> create_gold_layer >> validate_data >> cleanup