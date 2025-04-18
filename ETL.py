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



 --- Data Extraction and MongoDB Storage ---

@op(out={"status": Out(), "salary_count": Out(), "job_satisfaction_count": Out(), "mental_health_count": Out()})
def extract_and_store_data(context):
    """Extract all data sources and store in MongoDB"""
    try:
        context.log.info("Starting data extraction...")
        
        # Process Employee_salary.json
        salary_path = Path('Employee_salary.json')
        if not salary_path.exists():
            raise FileNotFoundError(f"Input file not found: {salary_path}")
        
        with open(salary_path) as f:
            salary_data = json.load(f)
        
   # Extract the data rows from the JSON structure
        salary_records = salary_data.get('data', [])
        salary_columns = ['row_id', 'uuid', 'position', 'created_at', 'created_meta', 
                         'updated_at', 'updated_meta', 'meta', 'date_day', 'statefips', 
                         'state_name', 'emp_ss40', 'emp_ss60', 'emp_ss65', 'emp_ss70']
        
        salary_df = pd.DataFrame(salary_records, columns=salary_columns)
        context.log.info(f"Loaded {len(salary_df)} records from Employee_salary.json")
# Process Job_satisfaction.csv
        job_satisfaction_path = Path('Job_satisfaction.csv')
        if not job_satisfaction_path.exists():
            raise FileNotFoundError(f"Input file not found: {job_satisfaction_path}")
        
        job_satisfaction_df = pd.read_csv(job_satisfaction_path)
        context.log.info(f"Loaded {len(job_satisfaction_df)} records from Job_satisfaction.csv")
     
  # Process Mental_health.csv
        mental_health_path =   Path('Mental_health.csv')
        if not mental_health_path.exists():
            raise FileNotFoundError(f"Input file not found : {mental_health_path}")
        
        mental_health_df  =   pd.read_csv(mental_health_path)
        context.log.info(f"Loaded {len(mental_health_df)}  records from Mental_health.csv")
        
        
