import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection parameters
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'GETHARLEY')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'DEV')

# Data files
CONVERSATIONS_FILE = 'data/conversations.csv'
MESSAGES_FILE = 'data/messages.csv'

def create_snowflake_connection():
    """Create and return a Snowflake connection"""
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    return conn

def execute_queries(conn, queries):
    """Execute a list of queries"""
    cur = conn.cursor()
    for query in queries:
        print(f"\nExecuting query:\n{query}")
        cur.execute(query)
    cur.close()

def setup_tables_and_formats():
    """Create tables and file formats if they don't exist"""
    setup_queries = [
        # Create conversations table
        """
        CREATE TABLE IF NOT EXISTS conversations (
            conversation_id VARCHAR(255),
            status VARCHAR(50),
            customer_id VARCHAR(255)
        );
        """,
        
        # Create messages table
        """
        CREATE TABLE IF NOT EXISTS messages (
            id VARCHAR(255),
            conversation_id VARCHAR(255),
            direction VARCHAR(50),
            direction_type VARCHAR(50),
            channel VARCHAR(50),
            date_time TIMESTAMP_TZ
        );
        """,
        
        # Create file format
        """
        CREATE OR REPLACE FILE FORMAT csv_format
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('');
        """
    ]
    return setup_queries

def load_data(conn, file_path, table_name):
    """Load data from CSV file into specified table"""
    cur = conn.cursor()
    
    # Get absolute path
    abs_file_path = os.path.abspath(file_path)
    
    # Create temporary stage
    stage_name = f'temp_stage_{table_name}'
    create_stage_query = f"""
    CREATE OR REPLACE TEMPORARY STAGE {stage_name}
        FILE_FORMAT = csv_format;
    """
    
    # Put file into stage
    put_command = f"PUT 'file://{abs_file_path}' @{stage_name}"
    
    # Copy into table
    copy_query = f"""
    COPY INTO {table_name}
        FROM @{stage_name}
        FILE_FORMAT = csv_format
        ON_ERROR = 'CONTINUE';
    """
    
    print(f"\nLoading data into {table_name}...")
    cur.execute(create_stage_query)
    print("Stage created.")
    
    cur.execute(put_command)
    print("File uploaded to stage.")
    
    result = cur.execute(copy_query).fetchall()
    print(f"Data loaded into {table_name}. Result:", result)
    
    cur.close()

def main():
    """Main function to orchestrate the data loading process"""
    try:
        # Create connection
        print("Connecting to Snowflake...")
        conn = create_snowflake_connection()
        
        # Setup tables and formats
        print("\nSetting up tables and file formats...")
        setup_queries = setup_tables_and_formats()
        execute_queries(conn, setup_queries)
        
        # Load data
        load_data(conn, CONVERSATIONS_FILE, 'conversations')
        load_data(conn, MESSAGES_FILE, 'messages')
        
        print("\nData loading completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        
    finally:
        if 'conn' in locals():
            conn.close()
            print("\nConnection closed.")

if __name__ == "__main__":
    main()
