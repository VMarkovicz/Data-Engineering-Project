from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
from docker.types import Mount
from airflow.models import DagModel

PROJECT_ROOT = Variable.get("PROJECT_ROOT", default_var=".")
default_args = {
    'owner': 'financial-data-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'full_data_ingestion_pipeline',
    default_args=default_args,
    description='Full one-time ingestion of historical financial and economic data',
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
    tags=['financial-data', 'cryptostock', 'economic-data'],
)

COMMON_MOUNTS = [
    Mount(source=f'{PROJECT_ROOT}/src', target='/opt/src', type='bind'),
    Mount(source=f'{PROJECT_ROOT}/datalake', target='/datalake', type='bind'),
    Mount(source=f'{PROJECT_ROOT}/src/raw_datasets', target='/opt/raw_datasets', type='bind'),
]

SPARK_MASTER_URL = Variable.get("SPARK_MASTER_URL", default_var="spark://spark-master:7077")
DOCKER_URL = Variable.get("DOCKER_URL", default_var="unix://var/run/docker.sock")

SPARK_COMMAND = lambda script_path: [
    'spark-submit',
    '--master', SPARK_MASTER_URL,
    '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
    '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
    '--driver-memory', '2g',
    '--executor-memory', '2g',
    script_path
]

def unpause_stocks_daily():
    dag = DagModel.get_dagmodel("stocks_daily_pipeline")
    dag.set_is_paused(is_paused=False)

fetch_data = DockerOperator(
    task_id='fetch_stocks_data',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command='python /opt/src/api_ingestion/fetch_crypto_stock_data.py',
    docker_url=DOCKER_URL,
    network_mode='data-engineering-project_app-network',
    mounts=COMMON_MOUNTS,
    environment={
        'ALPACA_API_KEY': Variable.get("ALPACA_API_KEY"),
        'ALPACA_SECRET_KEY': Variable.get("ALPACA_SECRET_KEY")
    },
    dag=dag,
)

process_spark_crypto = DockerOperator(
    task_id='process_crypto_qdl_data',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command=SPARK_COMMAND('/opt/src/ingestion/crypto_qdl_ingestion.py'),
    docker_url=DOCKER_URL,
    network_mode='data-engineering-project_app-network',
    mounts=COMMON_MOUNTS,
    environment={
        'SPARK_MASTER_URL': SPARK_MASTER_URL
    },
    dag=dag,
)

process_spark_stocks = DockerOperator(
    task_id='process_stocks_data',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command=SPARK_COMMAND('/opt/src/ingestion/stocks_spark_ingestion.py'),
    docker_url=DOCKER_URL,
    network_mode='data-engineering-project_app-network',
    mounts=COMMON_MOUNTS,
    environment={
        'SPARK_MASTER_URL': SPARK_MASTER_URL
    },
    dag=dag,
)

process_spark_wb = DockerOperator(
    task_id='process_world_bank_data',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command=SPARK_COMMAND('/opt/src/ingestion/wb_spark_ingestion.py'),
    docker_url=DOCKER_URL,
    network_mode='data-engineering-project_app-network',
    mounts=COMMON_MOUNTS,
    environment={
        'SPARK_MASTER_URL': SPARK_MASTER_URL
    },
    dag=dag,
)

process_spark_zillow = DockerOperator(
    task_id='process_zillow_real_estate_data',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command=SPARK_COMMAND('/opt/src/ingestion/zillow_spark_ingestion.py'),
    docker_url=DOCKER_URL,
    network_mode='data-engineering-project_app-network',
    mounts=COMMON_MOUNTS,
    environment={
        'SPARK_MASTER_URL': SPARK_MASTER_URL
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
    network_mode='data-engineering-project_app-network',
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
    docker_url='unix://var/run/docker.sock',
    network_mode='data-engineering-project_app-network',
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
    docker_url='unix://var/run/docker.sock',
    network_mode='data-engineering-project_app-network',
    mounts=COMMON_MOUNTS,
    dag=dag,
)

generate_rag_documents = DockerOperator(
    task_id='generate_rag_documents',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command='python /opt/src/rag/postgres_vector.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='data-engineering-project_app-network',
    mounts=COMMON_MOUNTS,
    environment={
        'OPEN_AI_KEY': Variable.get("OPEN_AI_KEY"),
    },
    dag=dag,
)

unpause_task = PythonOperator(
        task_id="unpause_stocks_daily",
        python_callable=unpause_stocks_daily,
)

fetch_data >> process_spark_crypto >> process_spark_stocks >> process_spark_wb >> process_spark_zillow >> create_gold_layer >> validate_data >> cleanup >> generate_rag_documents >> unpause_task