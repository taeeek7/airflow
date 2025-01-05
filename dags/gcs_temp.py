from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from utils.gcloud import GcloudUtils
from utils.slack import SlackUtils
import pendulum

# 실패 알림 메서드
def notify_failure(context) :
    SlackUtils.notify_failure(context)


# 데이터테이블 생성 정보
TARGET_DATASET = "cleanops" 
TABLE = "claim_set_point_log"

# MySQL 스키마 정보를 읽어 GCS 스키마 생성
def create_schema_handler():
    
    return GcloudUtils.generate_gcs_schema(
        mysql_conn_id= TARGET_DATASET,
        gcp_conn_id= 'bigquery-account',
        database_name= TARGET_DATASET,
        table_name= TABLE,
        gcs_bucket= 'airflow-ops',
        schema_path= f'schema/schema_{TABLE}.json',
    )

with DAG(
    dag_id=f"gcs_temp", # dag_id - 보통 파일명과 동일하게
    schedule="5 10 * * *", # cron 스케줄
    start_date=pendulum.datetime(2024, 9, 24, 10, 5, tz="Asia/Seoul"), # 시작일자
    catchup=False, # 과거 데이터 소급적용
    tags=["gcs", "bigquery", "temp"], # 태그값
    default_args= {
        'on_failure_callback' : notify_failure
    }
    
) as dag:
    # GCS 스키마 생성 태스크
    generate_schema_task = PythonOperator(
        task_id='generate_schema_task',
        python_callable=create_schema_handler
    )

    # MySQL 데이터를 GCS로 내보내기
    export_mysql_to_gcs = MySQLToGCSOperator(
        task_id='export_mysql_to_gcs',
        mysql_conn_id= TARGET_DATASET,  # MySQL에 대한 Airflow Connection ID
        gcp_conn_id= 'bigquery-account', # GCS Airflow Connection ID
        sql= f'SELECT * FROM {TABLE} ;',  # MySQL에서 가져올 데이터 쿼리
        bucket='airflow-ops',  # 데이터를 저장할 GCS 버킷 이름
        filename=f'data/{TABLE}.json',  # GCS에 저장될 파일명 (JSON 형식)
        ensure_utc= True
    )

    # GCS 데이터를 BigQuery로 적재하기
    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        gcp_conn_id= 'bigquery-account', # GCS Airflow Connection ID
        bucket='airflow-ops',  # GCS 버킷 이름
        source_objects=[f'data/{TABLE}.json'],  # GCS에 저장된 파일 이름
        destination_project_dataset_table=f'airflow-ops.{TARGET_DATASET}.{TABLE}',  # BigQuery 대상 테이블
        source_format='NEWLINE_DELIMITED_JSON',  # 파일 형식 (json)
        write_disposition='WRITE_TRUNCATE',  # 테이블에 데이터를 덮어쓰기
        create_disposition='CREATE_IF_NEEDED',
        schema_object=f'schema/schema_{TABLE}.json',  # GCS에 있는 스키마 파일 경로
    )

    # dag 작업 순서
    generate_schema_task >> export_mysql_to_gcs >> load_gcs_to_bigquery
    

    

