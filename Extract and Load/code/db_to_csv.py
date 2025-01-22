import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
db_params = {
    'dbname': os.getenv('DB_NAME', 'defaultdb'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '25060'),
    'sslmode': 'require'
}

try:
    # Connect to the database
    print("Connecting to database...")
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    # List all schemas
    print("\nAvailable schemas:")
    cursor.execute("""
        SELECT schema_name 
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog');
    """)
    schemas = cursor.fetchall()
    for schema in schemas:
        print(f"- {schema[0]}")
    
    # List all tables in each schema
    for schema in schemas:
        schema_name = schema[0]
        print(f"\nTables in schema '{schema_name}':")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE';
        """, (schema_name,))
        tables = cursor.fetchall()
        for table in tables:
            print(f"- {table[0]}")

    # Tables to fetch
    tables = ['conversations', 'messages']
    schema = 'analytics_engineer'

    # Create output directory if it doesn't exist
    output_dir = 'data'
    os.makedirs(output_dir, exist_ok=True)

    # Fetch each table and save to CSV
    for table in tables:
        print(f"\nFetching {table} table...")
        query = f'SELECT * FROM {schema}.{table}'
        df = pd.read_sql_query(query, conn)
        
        # Save to CSV
        output_file = os.path.join(output_dir, f'{table}.csv')
        df.to_csv(output_file, index=False)
        print(f"Saved {table} to {output_file}")

    print("\nData extraction completed successfully!")

except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Close the database connection
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
        print("\nDatabase connection closed.")
