from airflow.sdk import DAG,task
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id = 'goldprice_etl',
    start_date = pendulum.parse('2026-05-04',tz = 'Asia/Bangkok'),
    schedule = None
) as dag :
    
    test_dbt = BashOperator(
        task_id = 'test_dbt',
        cwd = '{{ var.value.goldprice_dbt_project }}',
        bash_command = 'dbt_debug'
    )

    test_dbt