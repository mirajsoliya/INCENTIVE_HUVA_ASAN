import pandas as pd
import pymysql
import os

# Database connection details
DB_HOST = '103.180.186.207'  # Replace with your database host
DB_USER = 'qrt'  # Replace with your database user
DB_PASSWORD = 't7%><rC)MC)8rdsYCj<E'  # Replace with your database password
DB_DATABASE = 'weekly'  # Replace with your database name

# Function to load CSV file and handle empty or missing files
def load_csv_file(filename, default_columns):
    if not os.path.exists(filename) or os.stat(filename).st_size == 0:
        print(f"Warning: {filename} is missing or empty. Proceeding with default empty DataFrame.")
        return pd.DataFrame(columns=default_columns)
    try:
        return pd.read_csv(filename)
    except pd.errors.EmptyDataError:
        print(f"Warning: {filename} is empty or cannot be parsed. Proceeding with default empty DataFrame.")
        return pd.DataFrame(columns=default_columns)

# Function to fetch refund amount from both CSVs
def get_refund_amount(name, refund_bdm, refund_tl):
    refund_amount = 0.0

    # Check in Refund_Deduction_Summary_BDM
    if name in refund_bdm['Name'].values:
        refund_amount += refund_bdm.loc[refund_bdm['Name'] == name, 'Refund Deducted'].sum()

    # Check in Refund_Deduction_Summary_TL
    if name in refund_tl['Name'].values:
        refund_amount += refund_tl.loc[refund_tl['Name'] == name, 'Refund Deducted'].sum()

    return refund_amount

# Function to fetch contact number from the database
def get_contact_number(name, connection):
    query = "SELECT mobile_phone FROM employees_data WHERE name = %s"
    with connection.cursor() as cursor:
        cursor.execute(query, (name,))
        result = cursor.fetchone()
        if result:
            return result[0]  # Assuming mobile_phone is the first column in the result
    return None

# Function to process and generate the output
def process_and_generate_output(files, connection, refund_bdm, refund_tl, output_filename, filter_collection=None):
    merged_data = []

    # Process each file
    for file in files:
        df = load_csv_file(file, ['BDM Name', 'Total Collection', 'Incentive', 'Cases'])

        # Standardize column names
        df.columns = df.columns.str.strip().str.upper()

        # Filter rows based on Total Collection if filter_collection is specified
        if filter_collection is not None:
            df = df[pd.to_numeric(df['TOTAL COLLECTION'], errors='coerce').fillna(0) >= filter_collection]

        for _, row in df.iterrows():
            name = row['BDM NAME'] if 'BDM NAME' in row else row.get('TL NAME')  # Handle column name variations
            contact_number = get_contact_number(name, connection)
            refund_amount = get_refund_amount(name, refund_bdm, refund_tl)
            merged_data.append({
                'salesperson_name': name,
                'contact_number': contact_number,
                'refund_amount': refund_amount
            })

    # Convert to DataFrame and save as CSV
    merged_df = pd.DataFrame(merged_data)
    merged_df.to_csv(output_filename, index=False)

# Main function to process the reports
def process_sales_reports(qualified_files, not_qualified_files):
    # Load refund data from CSV files
    refund_bdm = load_csv_file('Refund_Deduction_Summary_BDM.csv', ['Name', 'Refund Deducted'])
    refund_tl = load_csv_file('Refund_Deduction_Summary_PSA.csv', ['Name', 'Refund Deducted'])

    # Connect to the database
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )

    try:
        # Process Qualified files
        process_and_generate_output(qualified_files, connection, refund_bdm, refund_tl, 'Qualified_Salespersons.csv')

        # Process Not Qualified files with Total Collection filter
        process_and_generate_output(not_qualified_files, connection, refund_bdm, refund_tl, 'Not_Qualified_Salespersons.csv', filter_collection=50000)

        print("Final CSV files generated:")
        print("- Qualified_Salespersons.csv")
        print("- Not_Qualified_Salespersons.csv")

    finally:
        # Close the database connection
        connection.close()

# Example usage
qualified_files = ['Qualified_BDM.csv', 'Qualified_PSA.csv']
not_qualified_files = ['Not_Qualified_BDM.csv', 'Not_Qualified_PSA.csv']

def whatsapp_msg():
    process_sales_reports(qualified_files, not_qualified_files)

# if __name__ == "__main__":
#     whatsapp_msg()
