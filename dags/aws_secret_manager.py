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
from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook

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
def aws_secret_manager():
    @task()
    def list_secret():
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

        secret_manager_hook = SecretsManagerHook(aws_conn_id=aws_conn_id)

        secret_data = secret_manager_hook.get_secret_as_dict(
            secret_name="astrodbcreds-clnu2a51o001g01p4j71th36z"
        )

        print("Printing Secret...")
        print(secret_data)

    list_secret()


datetime_printer = aws_secret_manager()
