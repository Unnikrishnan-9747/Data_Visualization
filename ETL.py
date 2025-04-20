from dagster import job, op, DagsterInstance, execute_job, reconstructable, Output, Out
import pandas as pd
import pymongo
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_batch
import json
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import plotly.express as px
import plotly.io as pio
import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from fpdf import FPDF
import os
import sys
from pathlib import Path
import logging
import traceback
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import seaborn as sns
from datetime import datetime

#  Data Extraction and MongoDB Storage 
@op(out={"status": Out(), "job_satisfaction_count": Out(), "mental_health_count": Out(), "employee_data_count": Out()})
def extract_and_store_data(context):
    """Extract all data sources and store in MongoDB"""
    try:
        context.log.info("Starting data extraction...")
        
        # 1. Job Satisfaction Data
        js_path = Path('Job_satisfaction.csv')
        if not js_path.exists():
            raise FileNotFoundError(f"Input file not found: {js_path}")
        
        df_js = pd.read_csv(js_path)
        context.log.info(f"Loaded {len(df_js)} records from Job_satisfaction.csv")
        
        # Clean job satisfaction data
        country_map = {
            'AT': 'Austria', 'BE': 'Belgium', 'BG': 'Bulgaria', 'CH': 'Switzerland',
            'CY': 'Cyprus', 'CZ': 'Czech Republic', 'DE': 'Germany', 'DK': 'Denmark',
            'EE': 'Estonia', 'EL': 'Greece', 'ES': 'Spain', 'FI': 'Finland',
            'FR': 'France', 'HR': 'Croatia', 'HU': 'Hungary', 'IE': 'Ireland',
            'IT': 'Italy', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'LV': 'Latvia',
            'MT': 'Malta', 'NL': 'Netherlands', 'NO': 'Norway', 'PL': 'Poland',
            'PT': 'Portugal', 'RO': 'Romania', 'SE': 'Sweden', 'SI': 'Slovenia',
            'SK': 'Slovakia'
        }
        df_js['geo'] = df_js['geo'].map(country_map)
        df_js['OBS_VALUE'] = pd.to_numeric(df_js['OBS_VALUE'], errors='coerce')
      
    # 2. Mental Health Data
        mh_path = Path('Mental_health.csv')
        if not mh_path.exists():
            raise FileNotFoundError(f"Input file not found: {mh_path}")
        
        df_mh = pd.read_csv(mh_path)
        context.log.info(f"Loaded {len(df_mh)} records from Mental_health.csv")
        # 3. Employee Data (JSON)
        emp_path = Path('Employee_salary.json')
        if not emp_path.exists():
            raise FileNotFoundError(f"Input file not found: {emp_path}")
        
        with open(emp_path) as f:
            emp_data = json.load(f)['data']
        
        df_emp = pd.DataFrame([x[8:] for x in emp_data], 
                             columns=["Name", "Job_Title", "Department", "Full_or_Part_Time", 
                                     "Salary_or_Hourly", "Typical_Hours", "Annual_Salary", "Hourly_Rate"])
        context.log.info(f"Loaded {len(df_emp)} records from Employee_salary.json")
        
        # Perform initial EDA
        perform_eda(df_js, "job_satisfaction", context)
        perform_eda(df_mh, "mental_health", context)
        perform_eda(df_emp, "employee_data", context)
        
        # Connect to MongoDB
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        try:
            client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError:
            raise ConnectionError("Could not connect to MongoDB server")
            
        db = client["workforce_analytics"]
        
         # Store job satisfaction data
        collection_js = db["job_satisfaction"]
        collection_js.drop()
        result_js = collection_js.insert_many(df_js.to_dict('records'))
        context.log.info(f"Inserted {len(result_js.inserted_ids)} documents to job_satisfaction")

        collection_mh = db["mental_health"]
        collection_mh.drop()
        result_mh = collection_mh.insert_many(df_mh.to_dict('records'))
        context.log.info(f"Inserted {len(result_mh.inserted_ids)} documents to mental_health")

        collection_emp = db["employee_data"]
        collection_emp.drop()
        result_emp = collection_emp.insert_many(df_emp.to_dict('records'))
        context.log.info(f"Inserted {len(result_emp.inserted_ids)} documents to employee_data")
        
        client.close()
        
        yield Output("All data stored in MongoDB", output_name="status")
        yield Output(len(df_js), output_name="job_satisfaction_count")
        yield Output(len(df_mh), output_name="mental_health_count")
        yield Output(len(df_emp), output_name="employee_data_count")
    except Exception as e:
        context.log.error(f"Error in extract_and_store_data: {str(e)}")
        context.log.error(traceback.format_exc())
        raise

# ETL Process :  Extracting data from MongoDB for preprocessing

@op
def extract_from_mongodb(context, status, js_count, mh_count, emp_count):
    """Extract data from MongoDB for processing"""
    try:
        context.log.info("Extracting data from MongoDB...")
        context.log.info(f"Record counts - Job Satisfaction: {js_count}, Mental Health: {mh_count}, Employee: {emp_count}")
        
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        try:
            client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError:
            raise ConnectionError("Could not connect to MongoDB server")
            
        db = client["workforce_analytics"]
        
        # Extract all datasets
        js_df = pd.DataFrame(list(db["job_satisfaction"].find({})))
        mh_df = pd.DataFrame(list(db["mental_health"].find({})))
        emp_df = pd.DataFrame(list(db["employee_data"].find({})))
        
        client.close()
        
        return {
            "job_satisfaction": js_df,
            "mental_health": mh_df,
            "employee_data": emp_df,
            "status": "All data extracted from MongoDB"
        }
    except Exception as e:
        context.log.error(f"Error in extract_from_mongodb: {str(e)}")
        context.log.error(traceback.format_exc())
        raise

