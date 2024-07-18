# Narsinghani_Sajjan_DebtCollectionETL


## Instructions for Running the ETL Script

1. **Install Required Packages:**
   ```sh
   pip install pandas sqlalchemy psycopg2 argparse

Replace the placeholders with your actual database credentials and file paths.
```
python etl_script.py --username <your_username> --password <your_password> --host <your_host> --port 5432 --db_name postgres --directory_name 5k_borrowers_data.csv --schema_name public --table_name borrowers
```
