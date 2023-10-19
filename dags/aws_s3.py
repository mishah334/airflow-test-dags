import time
from datetime import datetime

from airflow import settings
from airflow.decorators import (
    dag,
    task,
)
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

aws_conn_id = "aws_conn_s3"


@dag(
    schedule_interval="1-59/2 * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["aws"],
)
def aws_s3():
    @task()
    def list_s3():
        aws_conn_config = Connection(
            conn_id=aws_conn_id,
            conn_type="aws",
            extra={
                "region_name": "us-east-1",
                "role_arn": "arn:aws:iam::802301079447:role/AstroDBInstance-clnu2a51o001g01p4j71th36z",
                "assume_role_method": "assume_role_with_web_identity",
                "assume_role_with_web_identity_federation": "file",
            }
        )

        try:
            conn = BaseHook.get_connection(aws_conn_id)
            print(f"Found: {conn}")
        except AirflowNotFoundException:
            session = settings.Session()
            session.add(aws_conn_config)
            session.commit()

        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        file_list = s3_hook.list_keys(
            bucket_name='airflow-logs-clnu2a51o001g01p4j71th36z',
        )

        print("Listing files...")

        for file_name in file_list:
            print(file_name)

    list_s3()


datetime_printer = aws_s3()
