from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration
from mysql.full_etl import extract_full
# from datetime import datetime, timedelta


@dag(
    start_date=datetime(2025, 1, 1, 10, 30),
    schedule=duration(days=1),
    catchup=False,
    tags=['mysql', 'etl', 'clickhouse'],
    default_args={
        'retries': 0,
        'retry_delay': duration(minutes=1),
        'max_retry_delay': duration(hours=1)
        },
    dagrun_timeout=duration(minutes=20),
    # max_consecutive_failed_dag_runs=2, # if there is more than 2 fails in a row, pause the dag
    max_active_runs=1, # 1 execution at the same time is allowed
    # on_failure_callback=on_failure
)
def mysql_to_clickhouse_etl():
    '''
        Dag Description:

    '''
    
    @task.bash(
        env = {'param_1': 'hi there from param 1'}
    )
    def bash_test():
        import sys
        print(sys.path)
        return f'echo \"say sth, $param_1 \"'

    @task
    def extract_from_mysql():
        print('-----------')
        extract_full()

    bash_test() >> extract_from_mysql()



mysql_to_clickhouse_etl()