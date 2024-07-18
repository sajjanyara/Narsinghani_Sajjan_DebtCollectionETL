import pandas as pd
from sqlalchemy import create_engine
import argparse
import os
import sys


def create_table_query(schema_name, table_name):
    """
    Generate a SQL query to create a table if it doesn't exist.

    Parameters:
    schema_name (str): The schema name.
    table_name (str): The table name.

    Returns:
    str: The SQL query string.
    """
    return f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}   
                (name VARCHAR(256)   ENCODE lzo
                ,date_of_birth VARCHAR(256)   ENCODE lzo
                ,gender VARCHAR(256)   ENCODE lzo
                ,marital_status VARCHAR(256)   ENCODE lzo
                ,phone_number BIGINT   ENCODE az64
                ,email_address VARCHAR(256)   ENCODE lzo
                ,mailing_address VARCHAR(256)   ENCODE lzo
                ,language_preference VARCHAR(256)   ENCODE lzo
                ,geographical_location VARCHAR(256)   ENCODE lzo
                ,credit_score BIGINT   ENCODE az64
                ,loan_type VARCHAR(256)   ENCODE lzo
                ,loan_amount BIGINT   ENCODE az64
                ,loan_term BIGINT   ENCODE az64
                ,interest_rate DOUBLE PRECISION   ENCODE RAW
                ,loan_purpose VARCHAR(256)   ENCODE lzo
                ,emi DOUBLE PRECISION   ENCODE RAW
                ,ip_address VARCHAR(256)   ENCODE lzo
                ,geolocation VARCHAR(256)   ENCODE lzo
                ,repayment_history VARCHAR(65535)   ENCODE lzo
                ,days_left_to_pay_current_emi BIGINT   ENCODE az64
                ,delayed_payment VARCHAR(256)   ENCODE lzo
                )
                DISTSTYLE AUTO
;"""


def main(username, password, host, port, db_name, directory_name, schema_name, table_name):
    """
    Main function to read CSV data and upload it to a Postgres database.

    Parameters:
    username (str): Database username.
    password (str): Database password.
    host (str): Database host.
    port (int): Database port.
    db_name (str): Database name.
    directory_name (str): Path to the CSV file.
    schema_name (str): Schema name.
    table_name (str): Table name.
    """
    # NOTE: Error handling CSV file not found
    if not os.path.isfile(directory_name):
        print(f"Error: File '{directory_name}' not found.")
        sys.exit(1)
    try:
        # NOTE: Read CSV file
        df = pd.read_csv(directory_name)
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        sys.exit(1)

    # NOTE: reformatting/cleaning up column names
    new_column_names = ['_'.join(x.split(' ')).lower() for x in df.columns if len(x) > 1]
    df.columns = new_column_names

    try:
        # NOTE: Create a database connection
        tr_engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db_name}')
        rs_db_conn = tr_engine.raw_connection()
        tr_db_cursor = rs_db_conn.cursor()
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        sys.exit(1)

    try:
        # NOTE : Execute the create table query
        tr_db_cursor.execute(create_table_query(schema_name, table_name))
        rs_db_conn.commit()
    except Exception as e:
        print(f"Error creating the table: {e}")
        rs_db_conn.close()
        sys.exit(1)


    try:
        # NOTE: Upload Data to the specified table
        df.to_sql(table_name, tr_engine, schema=schema_name, if_exists='append', index=False, method='multi',
                  chunksize=1000)
    except Exception as e:
        print(f"Error uploading data to the table: {e}")
    finally:
        tr_db_cursor.close()
        rs_db_conn.close()


if __name__ == "__main__":
    # NOTE: Set up script arguments
    parser = argparse.ArgumentParser(description="Upload CSV data to Redshift")
    parser.add_argument('--username', required=True, help='Database username')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--host', required=True, help='Database host')
    parser.add_argument('--port', type=int, required=True, help='Database port')
    parser.add_argument('--db_name', required=True, help='Database name')
    parser.add_argument('--directory_name', required=True, help='Path to the CSV file')
    parser.add_argument('--schema_name', required=True, help='Schema name')
    parser.add_argument('--table_name', required=True, help='Table name')

    # NOTE: Parse command-line arguments
    args = parser.parse_args()

    # NOTE: Call main function with parsed arguments
    main(
        args.username,
        args.password,
        args.host,
        args.port,
        args.db_name,
        args.directory_name,
        args.schema_name,
        args.table_name
    )
