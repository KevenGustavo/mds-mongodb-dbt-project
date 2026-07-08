"""
===============================================================================
DAG: ELT - MongoDB to PostgreSQL (Bronze Layer)
===============================================================================
Description:
    Extracts complex JSON documents from MongoDB and loads them unchanged into 
    the PostgreSQL Bronze layer using the JSONB data type.
    
    Engineering Standards Applied:
    - TaskFlow API: Modern Airflow @dag and @task decorators.
    - Memory Management: Avoids XCom anti-pattern by isolating EL in one task.
    - Idempotency: Uses Postgres ON CONFLICT (Upsert) to prevent duplication.
    - Safe Connections: Implements Python context managers (with statement).
===============================================================================
"""

import os
import json
from bson import json_util 
import logging
from datetime import datetime
from airflow.decorators import dag, task
import pymongo
import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------
# 1. Configuration & Credentials
# ---------------------------------------------------------

MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_HOST = "mongo"

PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")
PG_HOST = "postgres-dw"

# ---------------------------------------------------------
# 2. DAG Definition
# ---------------------------------------------------------

default_args = {
    'owner': 'data_engineering_team',
    'start_date': datetime(2026, 7, 1),
    'retries': 1,
}

@dag(
    dag_id='extract_mongo_to_postgres_bronze',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['bronze', 'ingestion', 'ecommerce'],
    description='Extracts raw JSON from Mongo and loads into Postgres JSONB',
)
def extract_mongo_to_postgres_bronze():

    @task 
    def extract_and_load():
        # --- EXTRACT ---
        logging.info("Connecting to MongoDB...")

        mongo_client = pymongo.MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/", serverSelectionTimeoutMS=5000)
        mongo_collection = mongo_client["transactional"]["ecommerce_orders"]
        
        raw_documents = list(mongo_collection.find({}))
        logging.info(f"Extracted {len(raw_documents)} documents from MongoDB.")
        mongo_client.close()

        if not raw_documents:
            logging.info("No documents to process. Exiting.")
            return

        # Converte para JSON estrito eliminando complexidades do BSON
        clean_json_docs = json.loads(json_util.dumps(raw_documents))

        # --- LOAD ---
        logging.info("Connecting to PostgreSQL Data Warehouse...")
        
        with psycopg2.connect(
            host=PG_HOST,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            port=5432
        ) as pg_conn:
            
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

                values = [
                    (str(doc["_id"]), json.dumps(doc))
                    for doc in clean_json_docs
                ]

                insert_query = """
                    INSERT INTO bronze.ecommerce_orders_raw (_id, document)
                    VALUES %s
                    ON CONFLICT (_id) DO UPDATE 
                    SET document = EXCLUDED.document,
                        ingested_at = CURRENT_TIMESTAMP;
                """
                
                logging.info("Loading data into PostgreSQL (Bronze Layer)...")
                execute_values(pg_cursor, insert_query, values)
                
        logging.info(f"SUCCESS: {len(values)} documents safely upserted into bronze.ecommerce_orders_raw.")

    extract_and_load()

extract_mongo_to_postgres_bronze()