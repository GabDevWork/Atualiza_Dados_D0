from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from airflow.kubernetes.secret import Secret
from airflow.operators.email_operator import EmailOperator

default_args = {
    # Responsável
    "owner": "Gabriel Montalvão", 
    # Data de início
    "start_date": datetime(2026, 3, 12),
    "retries": 0,
    "retry_delay": timedelta(minutes=15)
}

docs = """
# DAG para atualização da tabela do Snowflake <TABLE_TARGET> que acompanha status diário
"""

with DAG(
    "dbt_dag_atualiza_dados_d0",
    default_args=default_args,
    schedule_interval="7/15 7-19 * * 1-5", 
    catchup=False,
    tags=["<TAG_AREA>", "<MATRICULA>", "<NOME_PROJETO>", "atualiza_dados"], 
    doc_md=docs,
    max_active_runs=1
) as dag:
    
    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    task_1 = KubernetesPodOperator(
        task_id="dbt_step_1",
        name="atualiza_dados_d0",
        image="<AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/<NOME_IMAGEM>:latest",
        cmds=["python3"],
        arguments=["/usr/datamart/atualiza_dados_d0.py"],
        namespace="processing",        
        is_delete_operator_pod=True,
        image_pull_policy="Always", 
        in_cluster=True
    )

    inicio >> task_1 >> fim