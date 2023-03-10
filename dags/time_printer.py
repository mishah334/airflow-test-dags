import time
from datetime import datetime

from airflow.decorators import (
    dag,
    task,
)


@dag(
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["example"],
)
def time_printer():
    @task()
    def date_printer():
        """
        Print the date and time for several minutes
        """

        for i in range(900):
            print(datetime.now().strftime(f"{i=} %FT%T"))
            time.sleep(1)

    date_printer()


time_printer = time_printer()
