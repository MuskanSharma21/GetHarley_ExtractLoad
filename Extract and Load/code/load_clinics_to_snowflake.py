import snowflake.connector
import pandas as pd
import os

# Snowflake connection parameters
snowflake_config = {
    'account': 'kl10261.central-india.azure',
    'user': 'MUSKAN12',
    'password': 'VivMus@0405',
    'warehouse': 'COMPUTE_WH',
    'database': 'GETHARLEY',
    'schema': 'dev'
}

# Get the absolute path of the CSV file
current_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(current_dir, 'clinics_data.csv')

def setup_snowflake():
    """Set up Snowflake environment"""
    conn = snowflake.connector.connect(**snowflake_config)
    try:
        cursor = conn.cursor()
        
        # Set up the environment
        setup_commands = [
            f"USE WAREHOUSE {snowflake_config['warehouse']}",
            f"USE DATABASE {snowflake_config['database']}",
            f"CREATE SCHEMA IF NOT EXISTS {snowflake_config['schema']}",
            f"USE SCHEMA {snowflake_config['schema']}",
            """
            CREATE OR REPLACE TABLE clinics (
                name VARCHAR(255),
                code VARCHAR(50)
            )
            """
        ]
        
        for command in setup_commands:
            print(f"Executing: {command}")
            cursor.execute(command)
            
        print("Snowflake environment setup completed")
        
    finally:
        cursor.close()
        conn.close()

def load_data_to_snowflake():
    """Load CSV data to Snowflake"""
    try:
        # Read CSV file
        print("Reading CSV file...")
        df = pd.read_csv(csv_file_path)
        print(f"Read {len(df)} rows from CSV")
        
        # Connect to Snowflake
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(**snowflake_config)
        cursor = conn.cursor()
        
        # Use the correct database and schema
        cursor.execute(f"USE WAREHOUSE {snowflake_config['warehouse']}")
        cursor.execute(f"USE DATABASE {snowflake_config['database']}")
        cursor.execute(f"USE SCHEMA {snowflake_config['schema']}")
        
        # Create temporary stage
        print("Creating temporary stage...")
        cursor.execute("CREATE OR REPLACE TEMPORARY STAGE temp_stage")
        
        # Upload data to stage
        print("Uploading data to stage...")
        put_command = f"PUT 'file://{csv_file_path}' @temp_stage AUTO_COMPRESS=FALSE"
        cursor.execute(put_command)
        
        # Create file format
        print("Creating file format...")
        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT csv_format
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        """)
        
        # Copy data into table
        print("Copying data into table...")
        cursor.execute("""
            COPY INTO clinics
            FROM @temp_stage/clinics_data.csv
            FILE_FORMAT = csv_format
        """)
        
        # Get number of rows loaded
        cursor.execute("SELECT COUNT(*) FROM clinics")
        row_count = cursor.fetchone()[0]
        print(f"Successfully loaded {row_count} rows into clinics table")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("Starting Snowflake data load process...")
    setup_snowflake()
    load_data_to_snowflake()
    print("Process completed!")
