import mysql.connector
import pandas as pd
import numpy as np

# Database connection details
DB_CONFIG = {
    "host": "103.180.186.207",
    "user": "qrt",
    "password": "t7%><rC)MC)8rdsYCj<E",
    "database": "weekly"
}

# CSV file path
CSV_FILE_PATH = "team_update_report.csv"

# Explicitly define the columns we want to keep
DESIRED_COLUMNS = [
    'so_id', 'sale_order', 'sale_order_untax', 'sale_order_amount_tax', 
    'sale_order_amount_total', 'sale_order_margin', 'sale_order_date', 
    'pre_salesman_user_id', 'p_id', 'name', 'state', 'payment_reference', 
    'payment_state', 'invoice_partner_display_name', 'invoice_origin', 
    'invoice_date', 'date', 'payment_untax', 'payment_amount_tax', 
    'payment_total', 'create_date', 'einvoice_status', 'salesperson1', 
    'salesperson2','salesperson3', 'salesperson1_name', 'salesperson2_name', 'salesperson3_name',
    'presalesperson_name', 'cost', 'final_amount', 'source', 'TL1', 'BM1', 'TL2'
]

def connect_to_db():
    """Establishes a connection to the MySQL database."""
    return mysql.connector.connect(**DB_CONFIG)

def truncate_weekly_incentive_master(connection):
    """Truncates the weekly_incentive_master table."""
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE weekly_incentive_master;")
    connection.commit()
    cursor.close()
    print("✅ Table weekly_incentive_master truncated successfully.")

def clean_data(value):
    """
    Clean and convert data for MySQL insertion.
    Converts numpy types and handles NaN values.
    """
    if pd.isna(value) or value is np.nan:
        return None
    
    # Convert numpy types to native Python types
    if isinstance(value, np.integer):
        return int(value)
    elif isinstance(value, np.floating):
        return float(value)
    
    return value

def debug_csv(csv_file):
    """Print CSV structure for debugging before inserting into MySQL."""
    df = pd.read_csv(csv_file)
    # Remove first column (Unnamed: 0)
    df = df.drop(df.columns[0], axis=1)
    
    # Select only the desired columns
    df = df[DESIRED_COLUMNS]

    
    return df

def insert_data_from_csv(connection, csv_file):
    """Reads data from CSV and inserts into MySQL."""
    
    df = debug_csv(csv_file)

    # Apply data cleaning to each row
    data_tuples = []
    for _, row in df.iterrows():
        # Clean each value in the row
        cleaned_row = [clean_data(value) for value in row]
        data_tuples.append(tuple(cleaned_row))

    # SQL Insert Query (Ensure it matches column count exactly)
    insert_query = """
    INSERT INTO weekly_incentive_master (
        so_id, sale_order, sale_order_untax, sale_order_amount_tax, sale_order_amount_total, 
        sale_order_margin, sale_order_date, pre_salesman_user_id, p_id, name, state, 
        payment_reference, payment_state, invoice_partner_display_name, invoice_origin, 
        invoice_date, date, payment_untax, payment_amount_tax, payment_total, create_date, 
        einvoice_status, salesperson1, salesperson2,salesperson3, salesperson1_name, salesperson2_name, salesperson3_name,
        presalesperson_name, cost, final_amount, source, TL1, BM1, TL2
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s
    );
    """

    cursor = connection.cursor()
    
    try:
        cursor.executemany(insert_query, data_tuples)
        connection.commit()
    except mysql.connector.Error as err:
        print(f"❌ MySQL Error: {err}")
        # Optional: Print more detailed error information
        print(f"Problematic Row: {data_tuples[0]}")
    finally:
        cursor.close()

def weekly_incentive_master():
    """Main function to execute the process."""
    connection = connect_to_db()

    try:
        truncate_weekly_incentive_master(connection)
        insert_data_from_csv(connection, CSV_FILE_PATH)
    finally:
        connection.close()
        print("✅ Database connection closed.")
