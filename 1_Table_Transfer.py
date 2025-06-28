import pandas as pd
import psycopg2
import mysql.connector
import numpy as np
from decimal import Decimal
from multiprocessing import Pool
import json  # Import json module to handle dict serialization
import math
# Database connection parameters
pg_params = {
    "dbname": "egniol_production",
    "user": "odoo",
    "password": "TRQ@#$%QRT@#&*()!TRQ",
    "host": "103.180.186.233",
    "port": "5432",
}

mysql_params = {
    'host': '103.180.186.207',   # Change to your MySQL host
    'user': 'qrt',        # Change to your MySQL username
    'password': 't7%><rC)MC)8rdsYCj<E', # Change to your MySQL password
    'database': 'weekly' # Change to your database name
}

def get_column_type(df, column_name):
    # Check the maximum length of string values in the column
    if df[column_name].dtype == "object":
        # Get maximum length of non-null string values
        max_length = (
            df[column_name]
            .apply(
                lambda x: (
                    len(str(x))
                    if pd.notnull(x) and not isinstance(x, (bool, int, float))
                    else 0
                )   
            )
            .max()
        )

        if max_length > 16383:  # Using a lower threshold to be safe
            return "LONGTEXT"
        elif max_length > 190:
            return "TEXT"
        else:
            return "VARCHAR(255)"

    # Handle numeric types
    elif np.issubdtype(df[column_name].dtype, np.number):
        return "DOUBLE"

    # Handle boolean type
    elif df[column_name].dtype == "bool":
        return "TINYINT(1)"

    # Handle datetime type
    elif np.issubdtype(df[column_name].dtype, np.datetime64):
        return "DATETIME"

    # Default to TEXT for any other types
    return "TEXT"


def truncate_selected_tables(mysql_conn, table_names):
    mysql_cursor = mysql_conn.cursor()
    try:
        # Truncate only the specified tables
        for table_name in table_names:
            truncate_sql = f"TRUNCATE TABLE `{table_name}`;"
            mysql_cursor.execute(truncate_sql)
            print(f"Truncated table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error truncating tables: {err}")
    finally:
        mysql_cursor.close()

def transfer_table(pg_table_name):
    pg_conn = psycopg2.connect(**pg_params)
    pg_cursor = pg_conn.cursor()
    mysql_conn = mysql.connector.connect(**mysql_params)
    mysql_cursor = mysql_conn.cursor()

    try:
        # Fetching the column names to create the table
        pg_cursor.execute(f"SELECT * FROM {pg_table_name} LIMIT 1")
        sample_data = pg_cursor.fetchall()
        column_names = [desc[0] for desc in pg_cursor.description]

        sample_df = pd.DataFrame(sample_data, columns=column_names)

        # Create the MySQL table if it doesn't exist
        column_definitions = []
        for col in sample_df.columns:
            col_type = get_column_type(sample_df, col)
            # Quote column names that are reserved keywords in MySQL
            col_quoted = f"`{col}`" if col in ["function", "type"] else col
            column_definitions.append(f"{col_quoted} {col_type}")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{pg_table_name}` (
            {', '.join(column_definitions)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        mysql_cursor.execute(create_table_sql)

        # Fetch all data from PostgreSQL
        pg_cursor.execute(f"SELECT * FROM {pg_table_name}")
        while True:
            data = pg_cursor.fetchmany(10000)
            if not data:
                break

            df = pd.DataFrame(data, columns=column_names)
            df = df.where(pd.notnull(df), None)


            boolean_columns = [
                "is_storno",
                "is_subscription",
                "is_move_sent",
                "always_tax_exigible",
                "to_check",
                "posted_before",
            ]
            for col in boolean_columns:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: 1 if x is True else 0 if x is False else None
                    )

            # Convert Decimal values in object columns to float
            for col in df.select_dtypes(include=["object"]):
                if df[col].apply(lambda x: isinstance(x, Decimal)).any():
                    df[col] = df[col].apply(
                        lambda x: float(x) if isinstance(x, Decimal) else x
                    )

                # Convert dict and list values to JSON strings
                if df[col].apply(lambda x: isinstance(x, dict) or isinstance(x, list)).any():
                    df[col] = df[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                    )

            # Handle datetime columns formatting
            datetime_columns = df.select_dtypes(include=["datetime64[ns]"]).columns.tolist()
            for col in datetime_columns:
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

            columns = ", ".join([f"`{col}`" for col in df.columns])  # Quote column names
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_sql = f"INSERT INTO `{pg_table_name}` ({columns}) VALUES ({placeholders})"

            batch_size = 10000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i : i + batch_size]
                values = [tuple(None if pd.isna(x) else int(x) if isinstance(x, np.int64) else x for x in row) for row in batch.values]
                try:
                    mysql_cursor.executemany(insert_sql, values)
                    mysql_conn.commit()
                    print(f"Inserted {len(values)} rows into MySQL table: {pg_table_name}")
                except mysql.connector.Error as err:
                    # Log the error with the table name
                    print(f"Error inserting batch into table {pg_table_name}: {err}")
                    continue

    except Exception as e:
        print(f"An error occurred while transferring table {pg_table_name}: {e}")
        raise

    finally:
        pg_cursor.close()
        pg_conn.close()
        mysql_cursor.close()
        mysql_conn.close()

def main(table_names):
    mysql_conn = mysql.connector.connect(**mysql_params)
    truncate_selected_tables(mysql_conn, table_names)  # Truncate only the selected tables
    mysql_conn.close()

    with Pool(processes=math.floor(len(table_names) / 2)) as pool:
    # with Pool(processes=15) as pool:
        pool.map(transfer_table, table_names)

if __name__ == "__main__":
    table_names = ["account_move", "account_payment", "crm_team", "crm_team_member", "res_partner", "res_users", 
                   "sale_gamification_data", "sale_order",
                   "product_template","product_product","sale_order_line","agreement","operation_task_activity",
                   "sale_person_collection_report","activity_type_config_project_project_rel","activity_type_config","project_project",
                   "account_analytic_line","hr_employee","hr_department","crm_stage","crm_lead","project_task","account_journal","account_tax",
                   "account_tax_sale_order_line_rel","res_country_state","employee_branch"]  # List of tables to transfer


    
#     table_names = ["account_move",
# "sale_order",
# "res_partner",
# "res_users",
# "sale_gamification_data"]

    print(f"Starting data transfer for tables: {table_names}")
    main(table_names)

