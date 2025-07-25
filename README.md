# Credit Limit Processor

This project automates the ingestion and transformation of customer credit limit data from CSV files into a SQL Server database.

## Features

- Reads multiple `*_Credit_*.csv` files from an input directory
- Cleans and transforms data using pandas
- Calculates and assigns unique `Credit_Limit_ID`
- Loads cleaned data into a staging table (`s_credit_limit`)
- Moves records into the fact table (`t_credit_limit`)
- Archives processed files to avoid duplication

## Requirements

- Python 3.8+
- SQL Server ODBC Driver (e.g., SQL Server Native Client 11.0)
- Python packages:
  - pandas
  - numpy
  - pyodbc
  - python-dotenv

Install dependencies:
```bash
pip install -r requirements.txt
```

## Environment Variables

All sensitive and environment-specific variables are managed via a `.env` file.

### `.env` Template:
```env
# File Paths
SOURCE_FOLDER=./data/input
ARCHIVE_FOLDER=./data/archive

# SQL Server Connection
DB_DRIVER=SQL Server Native Client 11.0
DB_SERVER=YOUR_SQL_SERVER_NAME
DB_NAME=QA_CDM
DB_USERNAME=your_username
DB_PASSWORD=your_password
```

## Usage

1. Place incoming `*_Credit_*.csv`,*_Invoices_*.csv files into the `data/input/` folder.
2. Configure the `.env` file with valid values.
3. Run the script:
```bash
python scripts/credit_limit_loader.py 
python scripts/ar_open_items_loader.py
```

## Output

- Processed files are archived in `data/archive/`
- Cleaned data is inserted into `s_credit_limit`, then loaded into `t_credit_limit`

## Author

**Arjun** - Generative AI Engineer | Data Automation Specialist

---

© 2025 Credit Limit and Open Invoices Processor. All rights reserved.