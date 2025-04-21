from dagster import job, op, DagsterInstance, execute_job, reconstructable, Output, Out
import pandas as pd
import pymongo
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_batch
import json
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler
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
import missingno as msno
from sklearn.impute import SimpleImputer

# function for performing EDA

def perform_eda(df, name, context=None):
   
    if context:
        context.log.info(f"\n=== EDA for {name} ===\n")
        context.log.info(f"Shape: {df.shape}")

        context.log.info("\nData Types:\n" + str(df.dtypes))
        context.log.info("\nMissing Values:\n" + str(df.isnull().sum()))

        context.log.info("\nDescriptive Stats:\n" + str(df.describe(include='all')))
    
    # Save EDA visualizations
    output_dirs = ["eda_visualizations"]
    for dir_name in output_dirs:
        dir_path = Path(dir_name)
        dir_path.mkdir(exist_ok=True)
    

    # Missing values matrix

    plt.figure(figsize=(10, 6))
    msno.matrix(df)
    plt.title(f'Missing Values - {name}')

    plt.savefig(f'eda_visualizations/missing_values_{name}.png')
    plt.close()
    
    # Numeric features distribution
    numeric_cols = df.select_dtypes(include=np.number).columns
    if len(numeric_cols) > 0:
        df[numeric_cols].hist(bins=20, figsize=(15, 10))

        plt.suptitle(f'Numeric Features Distribution - {name}')
        plt.savefig(f'eda_visualizations/numeric_dist_{name}.png')
        plt.close()
    
    # Correlation matrix 
    if len(numeric_cols) > 1:
        plt.figure(figsize=(10, 8))

        sns.heatmap(df[numeric_cols].corr(), annot=True, cmap='coolwarm')
        plt.title(f'Correlation Matrix - {name}')

        plt.savefig(f'eda_visualizations/correlation_{name}.png')
        plt.close()


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


        
        # Cleanning job satisfaction data

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
@op
def preprocess_data(context, data_dict):

    js_df = data_dict["job_satisfaction"]
    mh_df = data_dict["mental_health"]
    emp_df = data_dict["employee_data"]
    
    try:
        context.log.info("Starting data preprocessing...")
        
        # Job Satisfaction Data Preprocessing
        if '_id' in js_df.columns:
            js_df = js_df.drop('_id', axis=1)
        
        # Missing Values Handling
        js_df['OBS_VALUE'] = js_df['OBS_VALUE'].fillna(js_df['OBS_VALUE'].median())
        
        # Detection of Outliers
        js_outliers = detect_outliers_isolation_forest(js_df[['OBS_VALUE']].dropna(), ['OBS_VALUE'])
        js_df['is_outlier'] = False
        js_df.loc[js_outliers, 'is_outlier'] = True
        context.log.info(f"Detected {js_outliers.sum()} outliers in job satisfaction data")
        
         # 2.  Preprocessing of Mental Health Data
        
        if '_id' in mh_df.columns:
            
            mh_df = mh_df.drop('_id', axis=1)
        
        # Handlimg missing values
        
        numeric_cols = mh_df.select_dtypes(include=np.number).columns
        
        cat_cols =   mh_df.select_dtypes(exclude=np.number).columns
        
        # replace missing data in numerical variables
        
        num_imputer  = SimpleImputer(strategy='median ')

        mh_df[numeric_cols] =  num_imputer.fit_transform(mh_df[ numeric_cols ])
        
        # replace missing data in categorical variables
        
        for col in cat_cols:
            mh_df[col] = mh_df[col].fillna(mh_df[col].mode()[0])
        
        # detection of outliers 
        
        mh_out_cols = ['Age', 'Sleep_Hours', 'Work_Hours', 'Physical_Activity_Hours', 'Social_Media_Usage']

        mh_outliers =  detect_outliers_isolation_forest(mh_df[mh_out_cols], mh_out_cols)

        mh_df['is_outlier'] = False
        mh_df.loc[mh_outliers, 'is_outlier']   = True

        context.log.info(f" Detected {mh_outliers.sum()} outliers in mental health data")


        # 3.Preprocessing of Employee_salary data

        if '_id' in emp_df.columns:
            emp_df = emp_df.drop('_id', axis=1)
        

        # Handling the  missing values


        emp_df['Annual_Salary'] =  pd.to_numeric(emp_df['Annual_Salary'], errors='coerce')

        emp_df['Hourly_Rate'] =   pd.to_numeric(emp_df['Hourly_Rate'], errors='coerce')

        emp_df['Typical_Hours'] = pd.to_numeric(emp_df['Typical_Hours'], errors='coerce')
        

        emp_df['Annual_Salary'] = emp_df['Annual_Salary'].fillna(emp_df['Annual_Salary'].median())
        emp_df['Hourly_Rate']   = emp_df['Hourly_Rate'].fillna(emp_df['Hourly_Rate'].median()) 

        emp_df['Typical_Hours'] = emp_df['Typical_Hours'].fillna(40)  
        
        # Outlier detection

        emp_out_cols = ['Annual_Salary', 'Hourly_Rate']  

        emp_outliers = detect_outliers_isolation_forest(emp_df[emp_out_cols].dropna(), emp_out_cols)

        emp_df['is_outlier'] = False

        emp_df.loc[emp_outliers, 'is_outlier']   = True
        context.log.info(f"Detected {emp_outliers.sum()} outliers in employee data")
        
        # Saving the preprocessed data 

        perform_eda(js_df, "preprocessed_job_satisfaction", context)
        perform_eda(mh_df, "preprocessed_mental_health", context)
        perform_eda(emp_df, "preprocessed_employee_data", context)
        
        return {

            "job_satisfaction": js_df ,
            "mental_health" : mh_df, 
            "employee_data": emp_df ,
            "status" : "Data preprocessing complete "
        }
    
    except Exception as e:

        context.log.error(f"Preprocessing error: {str(e)}")
        raise
        
#Transform preprocessed data into structured format with feature engineering

@op
def transform_data(context, preprocessed_data):
    js_df = preprocessed_data["job_satisfaction"]
    mh_df = preprocessed_data["mental_health"]
    emp_df = preprocessed_data["employee_data"]
    
    try:
        context.log.info("Starting data transformation and feature engineering")
        
        # 1. Job Satisfaction Data Transformation
        
        js_df['country_code'] =js_df['geo'].apply(country_to_alpha3)
        
        js_df['satisfaction_category'] = pd.cut(js_df['OBS_VALUE'],bins=[0, 50, 75, 100, 150, 200],labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])

          #2. Mental Health Data Transformation
        mh_df['Gender']= mh_df['Gender'].str.lower()
        mh_df['Gender'] =mh_df['Gender'].replace({'male-ish': 'male','maile': 'male','trans-female': 'trans female',
                                                  'something kinda male?': 'other','cis female': 'female','cis male': 'male'})
        
        # Create mental health severity score
        severity_map = {'None': 0, 'Low': 1, 'Medium': 2, 'High': 3}
        mh_df['severity_score'] = mh_df['Severity'].map(severity_map)


        stress_map = {'Low': 0, 'Medium': 1, 'High': 2}
        mh_df['stress_numeric'] = mh_df['Stress_Level'].map(stress_map)
        
        # Feature engineering--->Worklife balance score
        scaler = MinMaxScaler()
        work_hours_scaled = scaler.fit_transform(mh_df[['Work_Hours']].values.reshape(-1, 1))
        mh_df['work_life_balance'] = (mh_df['Sleep_Hours'] / 8) * 0.4 + (1 - work_hours_scaled.flatten()) * 0.6
        
        # Feature engineering-- Health risk score
        mh_df['health_risk_score'] = (mh_df['Smoking_Habit'].map({
            'Non-Smoker': 0,
            'Occasional Smoker': 1,
            'Regular Smoker': 2,
            'Heavy Smoker': 3
        }) + mh_df['Alcohol_Consumption'].map({
            'Non-Drinker': 0,
            'Social Drinker': 1,
            'Regular Drinker': 2,
            'Heavy Drinker': 3
        })) * 0.5 + (1 - mh_df['Diet_Quality'].map({
            'Healthy': 0,
            'Average': 1,
            'Unhealthy': 2
        }) / 2)

        # 3. Employee Data Transformation


        # Annual salary for hourly workers
        emp_df['Estimated_Annual'] = emp_df.apply(
            lambda x: x['Hourly_Rate'] * x['Typical_Hours'] * 52 if pd.notna(x['Hourly_Rate']) else x['Annual_Salary'],
            axis=1
        )
        
        #job level categories

        def categorize_job(title):
            if pd.isna(title):
                return 'Unknown'
            title = str(title).lower()
            if any(word in title for word in ['manager', 'director', 'chief', 'head', 'lead']):
                return 'Management'
            
            elif any(word in title for word in ['senior', 'sr', 'principal']):
                return 'Senior'
            
            elif any(word in title for word in ['junior', 'jr', 'associate', 'assistant']):
                return 'Junior'
            
            elif any(word in title for word in ['intern', 'trainee']):
                return 'Intern'
            
            else:
                return 'Standard'
        
        emp_df['Job_Level'] = emp_df['Job_Title'].apply(categorize_job)
        
        # Feature engineering--Salary percentile
        emp_df['Salary_Percentile'] = emp_df['Estimated_Annual'].rank(pct=True) * 100
        
        # Feature engineering---Department size category
        dept_size = emp_df['Department'].value_counts()

        emp_df['Dept_Size_Category'] = emp_df['Department'].map(
            lambda x: 'Large' if dept_size[x] > 1000 else
                     'Medium' if dept_size[x] > 100 else 'Small'
        )
        
        
        emp_df['Log_Salary'] = np.log1p(emp_df['Estimated_Annual'])
        
        context.log.info(f"Transformed data shapes - Job Satisfaction: {js_df.shape}, Mental Health: {mh_df.shape}, Employee: {emp_df.shape}")
        
        # Sav ingfeature-engineered data stats
        perform_eda(js_df, "transformed_job_satisfaction", context)
        perform_eda(mh_df, "transformed_mental_health", context)
        perform_eda(emp_df, "transformed_employee_data", context)
        
        return {
            "transformed_js": js_df,
            "transformed_mh": mh_df,
            "transformed_emp": emp_df,
            "status": "Data transformation complete"
        }
    except Exception as e:
        context.log.error(f"Transformation error: {str(e)}")
        raise
        
