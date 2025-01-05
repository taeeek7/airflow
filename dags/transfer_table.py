from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.generic_transfer import GenericTransfer
from utils.slack import SlackUtils 
import pendulum

# 실패 알림
def notify_failure(context) :
    SlackUtils.notify_failure(context)

# 데이터테이블 생성 정보
DATASET = "cleanops"
TABLE = "client_claim_report"

with DAG(
    dag_id=f"tranfer_table", # dag_id - 보통 파일명과 동일하게 
    schedule=None, # cron 스케줄
    start_date=pendulum.datetime(2024, 12, 4, 0, 0, tz="Asia/Seoul"), # 시작일자
    catchup=False, # 과거 데이터 소급적용
    tags=["transfer", "event"], # 태그값
    default_args= {
        'on_failure_callback' : notify_failure
    }
    
) as dag:
    # 원본 DB MySQL로 내보내기
    transfer_t1 = GenericTransfer(
        task_id="transfer_t1",
        source_conn_id= "cleanops", # source DB 연결 id
        destination_conn_id= "cleanops", #destination 연결 정보
        destination_table= f"{TABLE}", # 저장될 곳의 table명
        # preoperator= f"TRUNCATE TABLE member_keeper ;", # sql문 실행하기 전에 실행할 sql문 , create table 등등
        sql= f"""select * from table ;"""
    )

    transfer_t1
