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
        "retries": 5,
    },
    tags=["example"],
)
def time_printer_failing():
    @task()
    def date_printer_failing():
        """
        Print the date and time for a while then raise an exception.
        """

        for i in range(20):
            print(datetime.now().strftime(f"{i=} %FT%T"))
            time.sleep(1)
            if i % 10 == 0:
                raise Exception("This is a forced exception to test exception handling.")

    date_printer_failing()


time_printer_failing = time_printer_failing()
