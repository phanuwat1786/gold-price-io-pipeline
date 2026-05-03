from airflow.sdk import DAG, task, Asset, Metadata, get_current_context, Variable
from airflow.providers.http.hooks.http import HttpHook
import pendulum
import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

gold_price_asset = Asset(name = 'gold_price',uri = 'x-market-price://gold')

with DAG(
    dag_id = 'get_gold_price',
    start_date = pendulum.parse('2026-05-03',tz = 'Asia/Bangkok'),
    schedule = None,
) as dag :
    
    logger = logging.getLogger(__name__)

    @task
    def reset_key_at_month_start():
        
        if pendulum.now().day == 1 :
            Variable.set(key = 'current_gold_api_index',value = 1)

    @task()
    def get_gold_price_from_api():
        
        http_hook = HttpHook(method= 'GET', http_conn_id= 'gold-price-io-api')
        token_number = int(Variable.get("current_gold_api_index",default = 1))

        while True:
            header = {
                    "x-access-token": Variable.get(key = f'gold_price_api_key_{token_number}'),
                    "Content-Type": "application/json",
                }

            response = http_hook.run(endpoint = '/XAU/USD', headers= header,extra_options={'check_response': False})

            if response.ok:
                Variable.set(key = "current_gold_api_index",value = str(token_number))
                return response.json()
            else:
                logger.info(f'response : {response.text}')
                if response.status_code == 403 and 'Monthly API quota exceeded' in response.text :
                    if token_number == 7:
                        logger.error("last api-key out of quota.")
                        response.raise_for_status()
                    else:
                        logger.info("using next api_key")
                        token_number += 1
                        continue
                else:
                    response.raise_for_status()
    
    @task(
        outlets = [
            gold_price_asset
        ]
    )
    def save_gold_price(api_response:dict):
        df = pd.DataFrame([api_response])

        pg_hook = PostgresHook(
            postgres_conn_id = 'pg_market_price'
        )
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql(name = 'gold',con = engine, index = False, if_exists= 'append',schema = 'raw')

    t_reset_key_at_month_start = reset_key_at_month_start()
    t_get_gold_price_from_api = get_gold_price_from_api()
    t_save_gold_price = save_gold_price(api_response= t_get_gold_price_from_api)
    
    t_reset_key_at_month_start >> t_get_gold_price_from_api >> t_save_gold_price