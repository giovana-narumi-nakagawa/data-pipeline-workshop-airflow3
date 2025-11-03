from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

# Configuração padrão da DAG
default_args = {
    'owner': 'aula11',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
dag = DAG(
    'etl_vendas_pipeline',
    default_args=default_args,
    description='Pipeline ETL para dados de vendas',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['etl', 'vendas', 'aula11'],
)

def extract_data(**context):
    """Extrai dados do arquivo CSV"""
    file_path = '/opt/airflow/data/dados_vendas.csv'
    logging.info(f"Extraindo dados de: {file_path}")
    
    df = pd.read_csv(file_path)
    logging.info(f"Dados extraídos: {len(df)} registros")
    
    # Salva dados extraídos para próxima tarefa
    df.to_csv('/tmp/dados_extraidos.csv', index=False)
    return f"Extraídos {len(df)} registros"

def transform_data(**context):
    """Transforma os dados extraídos"""
    logging.info("Iniciando transformação dos dados")
    
    # Carrega dados extraídos
    df = pd.read_csv('/tmp/dados_extraidos.csv')
    
    # Limpeza: trata valores nulos
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce').fillna(0)
    df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce').fillna(0)
    
    # Transformação: calcula total de vendas
    df['TotalVenda'] = df['Valor'] * df['Quantidade']
    
    # Converte data para formato correto
    df['Data'] = pd.to_datetime(df['Data'])
    
    logging.info(f"Dados transformados: {len(df)} registros")
    
    # Salva dados transformados
    df.to_csv('/tmp/dados_transformados.csv', index=False)
    return f"Transformados {len(df)} registros"

def load_data(**context):
    """Carrega dados transformados no PostgreSQL"""
    logging.info("Carregando dados no PostgreSQL")
    
    # Carrega dados transformados
    df = pd.read_csv('/tmp/dados_transformados.csv')
    
    # Conecta ao PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    # Carrega dados na tabela
    df.to_sql('vendas', engine, if_exists='replace', index=False, method='multi')
    
    logging.info(f"Dados carregados: {len(df)} registros na tabela vendas")
    return f"Carregados {len(df)} registros"

# Tarefa para criar tabela
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS vendas (
        ID_Produto VARCHAR(10),
        Valor DECIMAL(10,2),
        Quantidade INTEGER,
        Data DATE,
        Regiao VARCHAR(20),
        TotalVenda DECIMAL(10,2)
    );
    """,
    dag=dag,
)

# Tarefa de extração
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Tarefa de transformação
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Tarefa de carregamento
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Definição das dependências
create_table >> extract_task >> transform_task >> load_task