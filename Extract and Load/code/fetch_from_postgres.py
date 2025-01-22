import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
db_params = {
    'dbname': os.getenv('DB_NAME', 'gh_test_db'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '25060'),
    'sslmode': 'require'
}

# Create output directory if it doesn't exist
output_dir = 'data'
os.makedirs(output_dir, exist_ok=True)

def fetch_and_save_table(table_name, schema_name='analytics_engineer'):
    """Fetch data from PostgreSQL table and save to CSV"""
    try:
        # Connect to the database
        print(f"\nConnecting to database to fetch {table_name}...")
        conn = psycopg2.connect(**db_params)
        
        # Create the query
        query = f'SELECT * FROM {schema_name}.{table_name}'
        
        # Read data into pandas DataFrame
        print(f"Executing query for {table_name}...")
        df = pd.read_sql_query(query, conn)
        
        # Save to CSV
        output_file = os.path.join(output_dir, f'{table_name}.csv')
        df.to_csv(output_file, index=False)
        print(f"Saved {len(df)} rows to {output_file}")
        
        return True
        
    except Exception as e:
        print(f"Error fetching {table_name}: {str(e)}")
        return False
        
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    """Main function to fetch all required tables"""
    tables = ['conversations', 'messages']
    
    print("Starting data extraction process...")
    
    for table in tables:
        success = fetch_and_save_table(table)
        if success:
            print(f"Successfully extracted {table}")
        else:
            print(f"Failed to extract {table}")
    
    print("\nProcess completed!")

if __name__ == "__main__":
    main()
