import os
import glob
import time
import shutil
import pyodbc
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load environment configuration
folder_name = os.getenv("SOURCE_FOLDER")
dest = os.getenv("ARCHIVE_FOLDER")
db_driver = os.getenv("DB_DRIVER")
db_server = os.getenv("DB_SERVER")
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")

# Get list of all credit CSV files
all_csv_files = glob.glob(folder_name + "/*_Credit_*.csv")
Reporting_Date = time.strftime("%Y-%m-%d")

# Define expected column names in the CSV
file_titles = [
    'Company_Code', 'Business_Partner_ID', 'Name', 'Customer', 'Credit_Segment',
    'Securitization_Amt', 'Currency', 'No_Secured_Credit_Limit', 'Group_Key', 'Description',
    'Credit_Check_Rule', 'Risk_Class', 'Buying_Group', 'Buying_Group_Name', 'Open_Order',
    'Open_Invoice', 'Delivery', 'Billing', 'Insurance', 'Qualified', 'Unqualified',
    'Credit_Mgmt_Blocked', 'Company_Code_Blocked'
]

# Process each CSV file
for filename in all_csv_files:
    base = os.path.basename(filename)
    print(f"Processing {base} ...")

    # Read CSV into DataFrame with explicit column names and dtypes
    df = pd.read_csv(filename, index_col=None, names=file_titles, header=None,
                     dtype={col: str for col in file_titles})

    # List of numeric columns to convert safely
    float_cols = [
        'Securitization_Amt', 'No_Secured_Credit_Limit', 'Open_Order', 'Open_Invoice',
        'Delivery', 'Billing', 'Insurance', 'Qualified', 'Unqualified'
    ]

    # Convert numeric columns from string, coerce errors, and fill NaNs
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('float')

    # Strip whitespace in key identifiers
    for col in ['Company_Code', 'Group_Key', 'Business_Partner_ID']:
        df[col] = df[col].str.strip()

    # Generate unique ID for each record
    df['Credit_Limit_ID'] = df['Company_Code'] + df['Business_Partner_ID'] + df['Group_Key']

    # Replace NaN strings with empty string
    df = df.replace('nan', '')

    # Add metadata columns
    df['CreatedDate'] = Reporting_Date
    df['CreatedBy'] = 'Arjun'
    df['LastModifiedDate'] = Reporting_Date
    df['LastModifiedBy'] = 'Arjun'
    df['Is_Active'] = 1

    # Create database connection string
    conn_str = f"Driver={{{db_driver}}};Server={db_server};Database={db_name};UID={db_username};PWD={db_password};"
    sql_conn = pyodbc.connect(conn_str)
    cursor = sql_conn.cursor()

    # Clear previous staging data
    cursor.execute("TRUNCATE TABLE s_credit_limit")
    print("Staging table truncated...")

    # Define insert query with placeholders
    insert_query = f"""
        INSERT INTO s_credit_limit (
            Credit_Limit_ID, Company_Code, Business_Partner_ID, Name, Customer, Credit_Segment,
            Securitization_Amt, Currency, No_Secured_Credit_Limit, Group_Key, Description,
            Credit_Check_Rule, Risk_Class, Buying_Group, Buying_Group_Name, Open_Order,
            Open_Invoice, Delivery, Billing, Insurance, Qualified, Unqualified,
            Credit_Mgmt_Blocked, Company_Code_Blocked, CreatedDate, CreatedBy,
            LastModifiedDate, LastModifiedBy, Is_Active
        ) VALUES ({','.join(['?' for _ in range(29)])})
    """

    # Load data into staging table
    for _, row in df.iterrows():
        values = [
            row['Credit_Limit_ID'], row['Company_Code'], row['Business_Partner_ID'], row['Name'], row['Customer'],
            row['Credit_Segment'], row['Securitization_Amt'], row['Currency'], row['No_Secured_Credit_Limit'],
            row['Group_Key'], row['Description'], row['Credit_Check_Rule'], row['Risk_Class'],
            row['Buying_Group'], row['Buying_Group_Name'], row['Open_Order'], row['Open_Invoice'],
            row['Delivery'], row['Billing'], row['Insurance'], row['Qualified'], row['Unqualified'],
            row['Credit_Mgmt_Blocked'], row['Company_Code_Blocked'], row['CreatedDate'], row['CreatedBy'],
            row['LastModifiedDate'], row['LastModifiedBy'], row['Is_Active']
        ]
        cursor.execute(insert_query, values)

    sql_conn.commit()
    cursor.close()
    sql_conn.close()

    # Update fact table from staging table
    sql_conn = pyodbc.connect(conn_str)
    cursor = sql_conn.cursor()

    print("Updating fact table...")
    cursor.execute("""
        UPDATE t_credit_limit
        SET Is_Active = 0
        WHERE Credit_Limit_ID IN (SELECT DISTINCT Credit_Limit_ID FROM s_credit_limit)
    """)

    cursor.execute("INSERT INTO t_credit_limit SELECT * FROM s_credit_limit")
    sql_conn.commit()
    cursor.close()
    sql_conn.close()

    # Move processed file to archive
    shutil.move(filename, dest)
    print(f"Archived {base} successfully.")

print("✅ All Credit Limit files processed and loaded successfully.")
