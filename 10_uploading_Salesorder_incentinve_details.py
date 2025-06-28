import os
import sys
import logging
import pandas as pd
import pymysql
from pymysql import Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('database_import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def connect_to_database(
    host='103.180.186.207', 
    user='qrt', 
    password='t7%><rC)MC)8rdsYCj<E', 
    database='weekly'
):
    """
    Establish a connection to the MySQL database.
    
    Args:
    - host: Database host
    - user: Database username
    - password: Database password
    - database: Database name
    
    Returns:
    - Database connection object or None if connection fails
    """
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4',
            connect_timeout=10
        )
        logger.info('Successfully connected to MySQL database')
        return connection
    except Error as e:
        logger.error(f'Error connecting to MySQL database: {e}')
        return None

def validate_csv_file(file_path):
    """
    Validate the CSV file before processing.
    
    Args:
    - file_path: Path to the CSV file
    
    Returns:
    - Boolean indicating file validity
    """
    # Check if file exists
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False
    
    # Check file size (optional)
    if os.path.getsize(file_path) == 0:
        logger.error(f"Empty file: {file_path}")
        return False
    
    return True

def load_csv_to_dataframe(file_path):
    """
    Load CSV file into a pandas DataFrame.
    
    Args:
    - file_path (str): Path to the CSV file
    
    Returns:
    - DataFrame or None if loading fails
    """
    try:
        # Validate file first
        if not validate_csv_file(file_path):
            return None
        
        # Read the CSV file
        df = pd.read_csv(file_path, low_memory=False)
        
        # Log column names for verification
        logger.info(f"CSV Columns: {list(df.columns)}")
        
        # Define required columns
        required_columns = ['sale_order', 'payment_reference']
        
        # Check for missing required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing columns: {missing_columns}")
            raise ValueError(f"Required columns missing: {missing_columns}")
        
        # Rename columns to match database schema
        df = df.rename(columns={
            'sale_order': 'saleorder', 
            'payment_reference': 'payment_ref'
        })
        
        # Basic data cleaning
        df['saleorder'] = df['saleorder'].astype(str).str.strip()
        df['payment_ref'] = df['payment_ref'].astype(str).str.strip()
        
        # Remove duplicate rows if any
        df.drop_duplicates(subset=['saleorder', 'payment_ref'], keep='first', inplace=True)
        
        logger.info(f"Loaded {len(df)} records from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        return None

def insert_dataframe_to_database(connection, df, table_name='Insentive_given_saleorder', batch_size=1000):
    """
    Insert DataFrame into MySQL database in batches.
    
    Args:
    - connection: Database connection object
    - df: Pandas DataFrame to insert
    - table_name: Name of the target database table
    - batch_size: Number of records to insert in each batch
    """
    if df is None or len(df) == 0:
        logger.warning("Empty DataFrame. No data to insert.")
        return
    
    try:
        cursor = connection.cursor()
        
        # Disable foreign key checks and indexes temporarily
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute(f"ALTER TABLE {table_name} DISABLE KEYS;")
        
        # Prepare insert query
        insert_query = f"""
            INSERT IGNORE INTO {table_name} (`saleorder`, `payment_ref`, `tag`)
            VALUES (%s, %s, "10th week 25-26")
        """
        
        # Insert data in batches
        total_records = len(df)
        for start in range(0, total_records, batch_size):
            end = min(start + batch_size, total_records)
            batch_data = df[['saleorder', 'payment_ref']].iloc[start:end].values.tolist()
            
            logger.info(f"Inserting batch {start//batch_size + 1} with {len(batch_data)} records")
            
            cursor.executemany(insert_query, batch_data)
            connection.commit()
            
            logger.info(f"Completed batch {start//batch_size + 1}")
        
        # Re-enable foreign key checks and indexes
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        cursor.execute(f"ALTER TABLE {table_name} ENABLE KEYS;")
        
        logger.info(f"Successfully inserted total {total_records} records into {table_name} table")
    
    except Error as e:
        logger.error(f"Database insertion error: {e}")
        connection.rollback()
    
    except Exception as e:
        logger.error(f"Unexpected error during insertion: {e}")
        connection.rollback()
    
    finally:
        if cursor:
            cursor.close()

def main(file_path):
    """
    Main function to load CSV and insert into database.
    
    Args:
    - file_path (str): Path to the CSV file to process
    """
    connection = None
    try:
        # Establish database connection
        connection = connect_to_database()
        
        if connection:
            # Load CSV to DataFrame
            df = load_csv_to_dataframe(file_path)
            
            if df is not None:
                # Insert data into database
                insert_dataframe_to_database(connection, df)
    
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    
    finally:
        # Ensure connection is closed
        if connection:
            connection.close()
            logger.info("Database connection closed")

# Entry point of the script
if __name__ == "__main__":
    # Check if a file path is provided as an argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = 'incentive_dump_Final.csv'
    
    logger.info(f"Processing file: {input_file}")
    main(input_file)