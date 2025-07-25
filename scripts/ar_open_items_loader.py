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

# Configuration from environment
folder_name = os.getenv("SOURCE_FOLDER")
dest = os.getenv("ARCHIVE_FOLDER")
db_driver = os.getenv("DB_DRIVER")
db_server = os.getenv("DB_SERVER")
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")

# Discover files
all_csv_files = glob.glob(folder_name + "/*_Openitems_*.csv")
Reporting_Date = time.strftime("%Y-%m-%d")

# Define your headers and dtype dictionary
file_titles = ['Interface_ID','Company_Code','Customer','Special_GL_IND','Clearing_Date','Clearing_Doc','Assignment','Fiscal_Year1','Doc_No','Line_Item1','Posting_Date','Doc_Date1','Entry_Date','Currency','Local_Currency','Reference1','Doc_Type','Posting_Period','Posting_Key','Debit_Credit_Ind','Business_Area','Tax_Code2','Amount_In_LC','Amount1','LC_Tax_Amount','Tax_Amount','Text','Branch_Account','Baseline_Payment_Date','Payment_Terms','Days1','Days2','Days_Net','Discpercent1','Discpercent2','Disc_Base','Cash_Disc_Amount','Cash_Disc','Payment_Method','Payment_Block','Fixed','Invoice_Ref','Fiscal_Year2','Line_Item2','Dunning_Block','Dunning_Key','Last_Dunned','Dunning_Level','Dunning_Area','Doc_Status','Follow_On_Doc_Type','VAT_Registration_No','Reason_Code','Pmt_Meth_Supplement','Ref_Key1','Ref_Key2','Gen_Ledger_Currency','Amount2','Payment_Amount','Name1','Name2','Name3','Name4','Postal_Code','City','Country','Street','PO_Box','PB_Postal_Code','Post_Bank_Acct_No','Bank_Account','Bank_Key','Bank_Country','Tax_No1','Tax_No2','Liable_For_VAT','Sales_Equalizatn_Tax','Region','Bank_Control_Key','Instruction_Key','Payment_Recipient','Language_Key','Bill_Of_Exch_Life','BOE_Tax_Code','Bill_Exchange_Tax_LC','Bill_Of_Exchange_Tax','LC_Collection_Charge','Collection_Charge','Tax_Code1','Issue_Date','Used_On','Planned_Usage','Domicile','Drawer','Cenbank_Loc','City_Of_BOE_Drawer','Drawee','City_Of_Drawee','Disc_Days','Disc_Rate','Bill_Of_Exchange_Acpd','Bill_Ex_Status','Bill_Protest_ID','Bill_On_Demand','Ref_Procedure','Ref_Doc','Ref_Org_Unit','Reversed_With','Sp_GL_Transtype','Negative_Posting','Reference2','Billing_Doc','Reference3','Doc_Date2','Net_Due_Date','Buying_Group','Name6']

# Simplified dtype assignment
dtype_dict = {col: 'object' for col in file_titles}
float_cols = ['Amount_In_LC','Amount1','LC_Tax_Amount','Tax_Amount','Discpercent1','Discpercent2','Disc_Base','Cash_Disc_Amount','Cash_Disc','Amount2','Payment_Amount','Bill_Of_Exch_Life','Bill_Exchange_Tax_LC','Bill_Of_Exchange_Tax','LC_Collection_Charge','Collection_Charge']
int_cols = ['Days1','Days2','Days_Net','Dunning_Level','Line_Item2','Disc_Days','Reference3']
for col in float_cols:
    dtype_dict[col] = 'float'
for col in int_cols:
    dtype_dict[col] = 'int'

print("ar_open_items_dtypes status file processing started...")

for filename in all_csv_files:
    base = os.path.basename(filename)
    print(f"Processing file: {base}")

    df = pd.read_csv(filename, index_col=None, names=file_titles, header=None, dtype=dtype_dict)
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

    df['Net_Due_Date'] = pd.to_datetime(df['Net_Due_Date'], errors='coerce')
    df['Reporting_Date'] = pd.to_datetime(Reporting_Date)
    df['Over_Due_Days'] = (df['Reporting_Date'] - df['Net_Due_Date']).dt.days

    for col in float_cols + ['Reference3']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    for col in int_cols:
        df[col] = df[col].astype(int, errors='ignore')

    def trim_fraction(text):
        return text.replace('.0', '') if isinstance(text, str) and '.0' in text else text

    for col in ['Billing_Doc', 'Doc_No', 'Ref_Key2', 'Branch_Account', 'Invoice_Ref']:
        df[col] = df[col].astype(str).apply(trim_fraction)

    df['Doc_No'] = df['Doc_No'].astype(str)
    df['AR_OpenItem_ID'] = df['Company_Code'] + df['Customer'] + df['Doc_No']
    df.replace('nan', '', inplace=True)
    df['CreatedDate'] = Reporting_Date
    df['CreatedBy'] = 'Arjun'
    df['LastModifiedDate'] = Reporting_Date
    df['LastModifiedBy'] = 'Arjun'
    df['Is_Active'] = '1'

    conn_str = f"Driver={{{db_driver}}};Server={db_server};Database={db_name};UID={db_username};PWD={db_password};"
    sql_conn = pyodbc.connect(conn_str)
    cursor = sql_conn.cursor()

    print("Truncating s_ar_open_items...")
    cursor.execute("TRUNCATE TABLE s_ar_open_items")

    print("Inserting into s_ar_open_items...")
    insert_query = f"""
        INSERT INTO s_ar_open_items ({','.join(file_titles + ['AR_OpenItem_ID','Reporting_Date','Over_Due_Days','CreatedDate','CreatedBy','LastModifiedDate','LastModifiedBy','Is_Active'])})
        VALUES ({','.join(['?' for _ in range(len(file_titles) + 8)])})
    """

    for _, row in df.iterrows():
        values = [row[col] if col in row else None for col in file_titles]
        values += [
            row['AR_OpenItem_ID'], row['Reporting_Date'], row['Over_Due_Days'],
            row['CreatedDate'], row['CreatedBy'], row['LastModifiedDate'],
            row['LastModifiedBy'], row['Is_Active']
        ]
        cursor.execute(insert_query, values)

    sql_conn.commit()
    cursor.close()
    sql_conn.close()

    sql_conn = pyodbc.connect(conn_str)
    cursor = sql_conn.cursor()
    cursor.execute("""
        UPDATE t_ar_open_items
        SET Is_Active = 0
        WHERE AR_OpenItem_ID IN (SELECT DISTINCT AR_OpenItem_ID FROM s_ar_open_items)
    """)
    cursor.execute("INSERT INTO t_ar_open_items SELECT * FROM s_ar_open_items")
    sql_conn.commit()
    cursor.close()
    sql_conn.close()

    shutil.move(filename, dest)
    print(f"Archived: {base}")

print("All ar open item files processed and loaded successfully.")
