from dagster import job, op, DagsterInstance, execute_job, reconstructable, Output, Out
import pandas as pd
import pymongo
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_batch
import json
import numpy as np


# Data Extraction and  storing it in MongoDB

@op(out={"status": Out(), "d1_count": Out(), "d2_count": Out()})
def extract_and_store_csv_data(context):
    """Extract CSV data and store in MongoDB"""
    try:
        context.log.info("Starting CSV extraction...")
        
        # Process d1.csv (Mental Health Survey)
        d1_path = Path('employee_survey.csv')
        if not d1_path.exists():
            raise FileNotFoundError(f"Input file not found: {d1_path}")
        
        df_d1 = pd.read_csv(d1_path)
        context.log.info(f"Loaded {len(df_d1)} records from employee_survey.csv")
        
        # Process d2.csv (Employee Data)
        d2_path = Path('employee_mentalhealth2.csv')
        if not d2_path.exists():
            raise FileNotFoundError(f"Input file not found: {d2_path}")
        
        df_d2 = pd.read_csv(d2_path)
        context.log.info(f"Loaded {len(df_d2)} records from employee_mentalhealth.csv")
        
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        try:
            client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError:
            raise ConnectionError("Could not connect to MongoDB server")
            
        db = client["workforce_data"]
        
        # Store d1 data
        collection_d1 = db["mental_health_survey"]
        collection_d1.drop()
        result_d1 = collection_d1.insert_many(df_d1.to_dict('records'))
        context.log.info(f"Inserted {len(result_d1.inserted_ids)} documents to mental_health_survey")
        
        # Store d2 data
        collection_d2 = db["employee_data"]
        collection_d2.drop()
        result_d2 = collection_d2.insert_many(df_d2.to_dict('records'))
        context.log.info(f"Inserted {len(result_d2.inserted_ids)} documents to employee_data")
        
        client.close()
        
        yield Output("CSV data stored in MongoDB", output_name="status")
        yield Output(len(df_d1), output_name="d1_count")
        yield Output(len(df_d2), output_name="d2_count")
    except Exception as e:
        context.log.error(f"Error in extract_and_store_csv_data: {str(e)}")
        context.log.error(traceback.format_exc())
        raise
