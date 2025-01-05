from airflow.providers.google.cloud.hooks.gcs import GCSHook
from utils.sql import SqlUtils
import pandas as pd
import json

class GcloudUtils :
    # MySQL 스키마 정보를 읽어 GCS 스키마 생성
    ### 주로 datetime 필드를 datetime으로 만들어야 할때 사용 !!!!
    def generate_gcs_schema(mysql_conn_id, gcp_conn_id, database_name, table_name, gcs_bucket, schema_path):
        raw_columns = SqlUtils.get_source_data(
            conn_id= mysql_conn_id,
            sql= f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM information_schema.COLUMNS
                WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{database_name}'
                ORDER BY ORDINAL_POSITION
                ;
                """
        )
        columns = pd.DataFrame(raw_columns, columns=["COLUMN_NAME", "DATA_TYPE"])

        # MySQL 데이터 타입 → BigQuery 스키마 변환
        type_mapping = {
            'datetime': 'DATETIME',
            'timestamp': 'TIMESTAMP',
            'varchar': 'STRING',
            'text': 'STRING',
            'int': 'INTEGER',
            'bigint': 'INTEGER',
            'tinyint': 'INTEGER',
            'decimal': 'FLOAT',
            'double': 'FLOAT',
            'float': 'FLOAT',
            # 필요 시 추가 매핑 가능
        }

        schema = []
        for _, row in columns.iterrows():
            schema.append({
                "name": row['COLUMN_NAME'],
                "type": type_mapping.get(row['DATA_TYPE'], 'STRING'),  # 기본값 STRING
                "mode": "NULLABLE"  # 기본 NULLABLE
            })

        # GCS에 스키마 파일 업로드
        schema_json = json.dumps(schema, indent=2)
        gcs_hook = GCSHook(gcp_conn_id= gcp_conn_id)
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=schema_path,
            data=schema_json
        )