import time
from datetime import datetime

from airflow.decorators import (
    dag,
    task,
)

from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator
)


@dag(
    schedule_interval="1-59/2 * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["aws"],
)
def aws_basic():
    @task()
    def list_s3():
        """
        Print the date and time for five minute.
        """

        print("Hello Example")

        s3_file = S3ListOperator(
            task_id='list_3s_files',
            bucket='airflow-logs-clnu2a51o001g01p4j71th36z',
            prefix='clnued9n1166081mu9qk8l8ipr/',
            delimiter='/',
        )

        print(s3_file)

    return list_s3()


datetime_printer = aws_basic()
