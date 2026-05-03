from airflow.sdk import DAG, task, Asset, Metadata, get_current_context, Variable
import pendulum


with DAG(
    dag_id = 'get_gold_price',
    start_date = pendulum.parse('2026-05-03',tz = 'Asia/Bangkok'),
    schedule = None,
) as dag :
    
    @task
    def reset_key_at_month_start():
        
        dag_run = get_current_context()['dag_run']
        print(type(dag_run))