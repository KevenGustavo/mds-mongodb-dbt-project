"""
===============================================================================
DAG: ELT End-to-End (MongoDB -> Postgres Bronze -> dbt Silver/Gold)
===============================================================================
Description:
    The ultimate Modern Data Stack pipeline. Orchestrates Python for the Extract
    and Load phase (EL), and leverages Astronomer Cosmos to execute dbt (T) 
    transformations directly within the Airflow graph.
===============================================================================
"""

import os
import json
from bson import json_util 
import logging
from datetime import datetime
from pathlib import Path

import pymongo
import psycopg2
from psycopg2.extras import execute_values

from airflow.decorators import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# ---------------------------------------------------------
# 1. Configurações e Credenciais
# ---------------------------------------------------------

MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_HOST = "mongo"

PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")
PG_HOST = "postgres-dw"

DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/dbt_transform")

profile_config = ProfileConfig(
    profile_name="dbt_transform",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dw",
        profile_args={"schema": "silver"},
    )
)

# ---------------------------------------------------------
# 2. DAG Definition
# ---------------------------------------------------------

default_args = {
    'owner': 'data_engineering_team',
    'start_date': datetime(2026, 7, 1),
    'retries': 1,
}

@dag(
    dag_id='elt_end_to_end_ecommerce',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['end-to-end', 'ingestion', 'dbt'],
    description='Pipeline completo: Extração bruta e Transformação Analítica',
)
def elt_end_to_end_ecommerce():

    @task 
    def extract_and_load_bronze():
        """
        Tarefa 1: Ingestão Bruta do MongoDB para a camada Bronze do PostgreSQL.
        """
        logging.info("Connecting to MongoDB...")
        mongo_client = pymongo.MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/", serverSelectionTimeoutMS=5000)
        mongo_collection = mongo_client["transactional"]["ecommerce_orders"]
        
        raw_documents = list(mongo_collection.find({}))
        mongo_client.close()

        if not raw_documents:
            logging.info("No documents to process. Exiting.")
            return

        clean_json_docs = json.loads(json_util.dumps(raw_documents))

        logging.info("Connecting to PostgreSQL Data Warehouse...")
        with psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASSWORD, port=5432) as pg_conn:
            with pg_conn.cursor() as pg_cursor:
                
                setup_queries = """
                    CREATE SCHEMA IF NOT EXISTS bronze;
                    CREATE TABLE IF NOT EXISTS bronze.ecommerce_orders_raw (
                        _id VARCHAR(50) PRIMARY KEY,
                        document JSONB,
                        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """
                pg_cursor.execute(setup_queries)

                values = [(str(doc["_id"]), json.dumps(doc)) for doc in clean_json_docs]

                insert_query = """
                    INSERT INTO bronze.ecommerce_orders_raw (_id, document)
                    VALUES %s
                    ON CONFLICT (_id) DO UPDATE 
                    SET document = EXCLUDED.document,
                        ingested_at = CURRENT_TIMESTAMP;
                """
                execute_values(pg_cursor, insert_query, values)
                
        logging.info(f"SUCCESS: {len(values)} documents safely upserted into Bronze.")

    # Tarefa 2: Transformação Analítica (Camadas Silver e Gold)
    
    transform_data_marts = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt")
    )

    # ---------------------------------------------------------
    # 3. A Orquestração Lógica
    # ---------------------------------------------------------

    extract_and_load_bronze() >> transform_data_marts

elt_end_to_end_ecommerce()