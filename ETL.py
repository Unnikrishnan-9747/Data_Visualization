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
       
# Connecting to Local Postgres Database

@op
def load_to_postgres(context, transformed_data):
    """Load structured data into PostgreSQL"""
    js_df = transformed_data["transformed_js"]
    mh_df = transformed_data["transformed_mh"]
    emp_df = transformed_data["transformed_emp"]
    
    context.log.info(f"Job Satisfaction columns: {js_df.columns.tolist()}")
    context.log.info(f"Mental Health columns: {mh_df.columns.tolist()}")
    context.log.info(f"Employee columns: {emp_df.columns.tolist()}")
    
    def convert_numpy_types(df):
        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = df[col].astype(object).where(df[col].notna(), None)
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].astype(object).where(df[col].notna(), None)
            elif pd.api.types.is_bool_dtype(df[col]):
                df[col] = df[col].astype(object).where(df[col].notna(), None)
        return df
    
    js_df = convert_numpy_types(js_df)
    mh_df = convert_numpy_types(mh_df)
    emp_df = convert_numpy_types(emp_df)
    
    conn = None
    try:
        conn = psycopg2.connect(
            database="postgres", 
            user="postgres", 
            password="123", 
            host="localhost", 
            port="5432",
            connect_timeout=5
        )
        cur = conn.cursor()
       
# Loading Job Satisfaction preprocessed Dataset to Postgres Database 
        
        cur.execute("""
        DROP TABLE IF EXISTS job_satisfaction;
        CREATE TABLE job_satisfaction (
            dataflow TEXT,
            last_update TEXT,
            freq TEXT,
            emp_cont TEXT,
            yes_no TEXT,
            lev_satis TEXT,
            age TEXT,
            sex TEXT,
            unit TEXT,
            geo TEXT,
            time_period INTEGER,
            obs_value FLOAT,
            obs_flag TEXT,
            conf_status TEXT,  -- Added this missing column
            is_outlier BOOLEAN,
            country_code TEXT,
            satisfaction_category TEXT
        )
        """)
        
        js_columns = js_df.columns.tolist()
        js_insert = f"""
        INSERT INTO job_satisfaction ({','.join(js_columns)})
        VALUES ({','.join(['%s']*len(js_columns))})
        """
        
        js_records = [tuple(None if pd.isna(x) else x for x in record) 
                     for record in js_df.to_records(index=False)]
       
        execute_batch(cur, js_insert, js_records, page_size=1000)
        
        # 2. Loading Mental Health data to postgres as new table
      
        cur.execute("""
        DROP TABLE IF EXISTS mental_health;
        CREATE TABLE mental_health (
            user_id INTEGER,
            age INTEGER,
            gender TEXT,
            occupation TEXT,
            country TEXT,
            mental_health_condition TEXT,
            severity TEXT,
            consultation_history TEXT,
            stress_level TEXT,
            sleep_hours FLOAT,
            work_hours INTEGER,
            physical_activity_hours INTEGER,
            social_media_usage FLOAT,
            diet_quality TEXT,
            smoking_habit TEXT,
            alcohol_consumption TEXT,
            medication_usage TEXT,
            is_outlier BOOLEAN,
            severity_score INTEGER,
            stress_numeric INTEGER,  
            work_life_balance FLOAT,
            health_risk_score FLOAT
        )
        """)
        
        mh_columns = mh_df.columns.tolist()
       
        mh_insert = f"""
        INSERT INTO mental_health ({','.join(mh_columns)})
        
        VALUES ({','.join(['%s']*len(mh_columns))})
        """
        
        mh_records = [tuple(None if pd.isna(x) else x for x in record) for record in mh_df.to_records(index=False)]
        execute_batch(cur, mh_insert, mh_records, page_size=1000)


       # 3. loading Employee salary data

        cur.execute("""
        DROP TABLE IF EXISTS employee_details;
        CREATE TABLE employee_details (
            name TEXT,
            job_title TEXT,
            department TEXT,
            full_or_part_time TEXT,
            salary_or_hourly TEXT,
            typical_hours FLOAT,
            annual_salary FLOAT,
            hourly_rate FLOAT,
            is_outlier BOOLEAN,
            estimated_annual FLOAT,
            job_level TEXT,
            salary_percentile FLOAT,
            dept_size_category TEXT,
            log_salary FLOAT
        )
        """)
        
        emp_columns = emp_df.columns.tolist()
        emp_insert = f"""
        INSERT INTO employee_details ({','.join(emp_columns)})
        VALUES ({','.join(['%s']*len(emp_columns))})
        """
        
        emp_records = [tuple(None if pd.isna(x) else x for x in record) for record in emp_df.to_records(index=False)]
        execute_batch(cur, emp_insert, emp_records, page_size=1000)
        conn.commit()

        context.log.info(f"Successfully loaded data to PostgreSQL")
        return {"status": "All data loaded to PostgreSQL"}
    


    except Exception as e:
        if conn:
            conn.rollback()
        context.log.error(f"Database error details: {str(e)}")
        raise Exception(f"Error loading to PostgreSQL: {str(e)}")
    
    
    finally:
        if conn:
            conn.close()

#Performing analysis on structured data in PostgreSQL

@op
def analyze_data(context, load_result):

    conn = None
    try:
        from sqlalchemy import create_engine

        engine = create_engine('postgresql://postgres:123@localhost:5432/postgres')   
       
         #Analysis of Job_satisfaction dataset
        satisfaction_by_country = pd.read_sql("""
            SELECT geo as country, country_code, 
                   AVG(obs_value) as avg_satisfaction,
                   COUNT(*) as count
            FROM job_satisfaction
            WHERE obs_value IS NOT NULL AND NOT is_outlier
            GROUP BY geo, country_code
            ORDER BY avg_satisfaction DESC
        """, engine)
        
        satisfaction_by_gender = pd.read_sql("""
            SELECT sex as gender, lev_satis as satisfaction_level,
                   AVG(obs_value) as avg_value,
                   COUNT(*) as count
            FROM job_satisfaction
            WHERE obs_value IS NOT NULL AND NOT is_outlier
            GROUP BY sex, lev_satis
            ORDER BY sex, avg_value DESC
        """, engine)
       
       # 2. Mental Health Analysis 
       
        mh_by_occupation = pd.read_sql("""
            SELECT occupation, 
                   COUNT(*) as total,
                   SUM(CASE WHEN mental_health_condition = 'Yes' THEN 1 ELSE 0 END) as with_condition,
                   CAST(AVG(severity_score) AS DECIMAL(10,2)) as avg_severity,
                   CAST(AVG(work_life_balance) AS DECIMAL(10,2)) as avg_work_life_balance,
                   CAST(AVG(health_risk_score) AS DECIMAL(10,2)) as avg_health_risk
                   
            FROM mental_health
            WHERE NOT is_outlier
            GROUP BY occupation
            ORDER BY with_condition DESC
        """, engine)
        
        # modifying the stress vs sleep query
       
        stress_vs_sleep = pd.read_sql("""
            SELECT stress_level, 
                   CAST(AVG(sleep_hours) AS DECIMAL(10,2)) as avg_sleep,
                   CAST(AVG(work_hours) AS DECIMAL(10,2)) as avg_work_hours,
                   CAST(AVG(work_life_balance) AS DECIMAL(10,2)) as avg_work_life_balance,
                   COUNT(*) as count,
                   stress_numeric
                   
            FROM mental_health
            WHERE NOT is_outlier
            GROUP BY stress_level, stress_numeric
            ORDER BY avg_sleep
        """, engine)


         # 3. Employee salary Data Analysis

        salary_by_department = pd.read_sql("""
            SELECT department, 
                   CAST(AVG(estimated_annual) AS DECIMAL(10,2)) as avg_salary,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY estimated_annual) as median_salary,
                   CAST(AVG(log_salary) AS DECIMAL(10,2)) as avg_log_salary,
                   COUNT(*) as count
            FROM employee_details
            WHERE estimated_annual IS NOT NULL AND NOT is_outlier
            GROUP BY department
            HAVING COUNT(*) > 10
            ORDER BY avg_salary DESC
        """, engine)
        
        employment_type_dist = pd.read_sql("""
            SELECT 
                COALESCE(full_or_part_time, 'Unknown') as full_or_part_time,
                COALESCE(salary_or_hourly, 'Unknown') as salary_or_hourly,
                COUNT(*) as count,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(10,1)) as percentage
            FROM employee_details
            WHERE NOT is_outlier
            GROUP BY full_or_part_time, salary_or_hourly
            ORDER BY count DESC
        """, engine)
        
        # Cross-dataset analysis

        occupation_salary_mh = pd.read_sql("""
            SELECT m.occupation,
                   CAST(AVG(e.estimated_annual) AS DECIMAL(10,2)) as avg_salary,
                   CAST(AVG(m.severity_score) AS DECIMAL(10,2)) as avg_severity,
                   CAST(AVG(m.work_life_balance) AS DECIMAL(10,2)) as avg_work_life_balance,
                   COUNT(*) as count
            FROM mental_health m
            JOIN employee_details e ON LOWER(SPLIT_PART(e.job_title, ' ', 1)) = LOWER(SPLIT_PART(m.occupation, ' ', 1))
            WHERE e.estimated_annual IS NOT NULL AND NOT m.is_outlier AND NOT e.is_outlier
            GROUP BY m.occupation
            HAVING COUNT(*) > 5
            ORDER BY avg_severity DESC
        """, engine)
        
        # analysis for dashboard
        
        work_hours_analysis = pd.read_sql("""
            SELECT 
                CASE 
                    WHEN work_hours < 35 THEN 'Under 35'
                    WHEN work_hours BETWEEN 35 AND 40 THEN '35-40'
                    WHEN work_hours BETWEEN 41 AND 50 THEN '41-50'
                    WHEN work_hours > 50 THEN 'Over 50'
                    ELSE 'Unknown'
                END as work_hours_group,
                CAST(AVG(severity_score) AS DECIMAL(10,2)) as avg_severity,
                CAST(AVG(work_life_balance) AS DECIMAL(10,2)) as avg_work_life_balance,
                COUNT(*) as count
            FROM mental_health
            WHERE NOT is_outlier
            GROUP BY work_hours_group
            ORDER BY avg_severity DESC
        """, engine)
        
        salary_satisfaction = pd.read_sql("""
            SELECT 
                CASE 
                    WHEN e.estimated_annual < 50000 THEN 'Under 50k'
                    WHEN e.estimated_annual BETWEEN 50000 AND 75000 THEN '50k-75k'
                    WHEN e.estimated_annual BETWEEN 75001 AND 100000 THEN '75k-100k'
                    WHEN e.estimated_annual > 100000 THEN 'Over 100k'
                    ELSE 'Unknown'
                END as salary_range,
                CAST(AVG(j.obs_value) AS DECIMAL(10,2)) as avg_satisfaction,
                COUNT(*) as count
            FROM employee_details e
            JOIN job_satisfaction j ON e.department = j.emp_cont
            WHERE e.estimated_annual IS NOT NULL AND j.obs_value IS NOT NULL 
                  AND NOT e.is_outlier AND NOT j.is_outlier
            GROUP BY salary_range
            ORDER BY avg_satisfaction DESC""", engine)
        
        health_risk_analysis = pd.read_sql("""
            SELECT 
                CASE 
                    WHEN health_risk_score < 1 THEN 'Low'
                    WHEN health_risk_score BETWEEN 1 AND 2 THEN 'Medium'
                    WHEN health_risk_score > 2 THEN 'High'
                    ELSE 'Unknown'
                END as health_risk_group,
                CAST(AVG(work_life_balance) AS DECIMAL(10,2)) as avg_work_life_balance,
                CAST(AVG(severity_score) AS DECIMAL(10,2)) as avg_severity,
                COUNT(*) as count
            FROM mental_health
            WHERE NOT is_outlier
            GROUP BY health_risk_group
            ORDER BY avg_work_life_balance""", engine)

        salary_dept_level = pd.read_sql("""
            SELECT 
                dept_size_category,
                job_level,
                ROUND(AVG(estimated_annual)::NUMERIC, 2) as avg_annual_salary,
                COUNT(*) as count
            FROM employee_details
            WHERE estimated_annual IS NOT NULL AND NOT is_outlier
            GROUP BY dept_size_category, job_level
            ORDER BY dept_size_category, job_level""", engine)



        correlation_data = pd.read_sql("""
            SELECT 
                m.sleep_hours,
                m.work_hours,
                m.severity_score,
                m.work_life_balance,
                m.health_risk_score,
                e.estimated_annual,
                e.log_salary,
                (CASE WHEN m.mental_health_condition = 'Yes' THEN 1 ELSE 0 END) as has_condition,
                (CASE WHEN m.stress_level = 'High' THEN 1 ELSE 0 END) as high_stress
            FROM mental_health m
            LEFT JOIN employee_details e ON LOWER(SPLIT_PART(e.job_title, ' ', 1)) = LOWER(SPLIT_PART(m.occupation, ' ', 1))
            WHERE NOT m.is_outlier AND (e.is_outlier IS NULL OR NOT e.is_outlier)""", engine)
        
        # Calculating correlation matrix

        correlation_matrix = correlation_data.corr()
        
        # PCA Analysis for dimensionality reduction 

        pca_data = correlation_data.dropna()
        if len(pca_data) > 0:
            pca = PCA(n_components=2)
            pca_result = pca.fit_transform(StandardScaler().fit_transform(pca_data))
            pca_df = pd.DataFrame(data=pca_result, columns=['PC1', 'PC2'])
            pca_df['stress_level'] = pca_data['high_stress'].map({0: 'Low', 1: 'High'})
            pca_variance = pca.explained_variance_ratio_
        else:
            pca_df = pd.DataFrame()
            pca_variance = [0, 0]
        
        return {
            "satisfaction_by_country": satisfaction_by_country,
            "satisfaction_by_gender": satisfaction_by_gender,
            "mh_by_occupation": mh_by_occupation,
            "stress_vs_sleep": stress_vs_sleep,
            "salary_by_department": salary_by_department,
            "employment_type_dist": employment_type_dist,
            "occupation_salary_mh": occupation_salary_mh,
            "work_hours_analysis": work_hours_analysis,
            "salary_satisfaction": salary_satisfaction,
            "health_risk_analysis": health_risk_analysis,
            "salary_dept_level": salary_dept_level,
            "correlation_matrix": correlation_matrix,
            "pca_data": pca_df,
            "pca_variance": pca_variance,
            "status": "Analysis complete"
        }


    except Exception as e:
        context.log.error(f"Analysis error details: {str(e)}")
        raise Exception(f"Analysis error: {str(e)}")
    
    
    finally:
        if conn:
            conn.close()



 # Creating visualizations 

@op
def create_visualizations(context, analysis_results):
    try:
        context.log.info("Starting visualization creation...")
        
        output_dirs = ["visualizations", "dashboard", "report_images", "eda_visualizations"]
        for dir_name in output_dirs:
            dir_path = Path(dir_name)
            dir_path.mkdir(exist_ok=True)
            context.log.info(f"Created directory: {dir_path.absolute()}")
        
        results = {}

        # need to add the codes for visualisations here

    except Exception as e:
        context.log.error(f"Error in create_visualizations: {str(e)}")
        context.log.error(traceback.format_exc())
        raise
