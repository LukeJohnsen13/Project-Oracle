from prefect import flow, task
import subprocess  # To call ETL scripts

@task
def run_etl(script):
    subprocess.run(['python', f'src/{script}'])

@flow(log_prints=True)
def daily_pipeline():
    run_etl('etl_market.py')
    run_etl('etl_onchain.py')
    run_etl('etl_sentiment.py')
    run_etl('etl_macro.py')

if __name__ == '__main__':
    daily_pipeline()