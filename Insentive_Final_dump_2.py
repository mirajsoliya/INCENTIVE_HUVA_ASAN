# import mysql.connector
# import pandas as pd
# import psycopg2
# import numpy as np

# # Step 1: Connect to MySQL Database
# def connect_to_database(host, user, password, database):
#     """Establish a connection to the MySQL database."""
#     connection = mysql.connector.connect(
#         host=host,
#         user=user,
#         password=password,
#         database=database
#     )
#     print("Connect to MySQL the database...")
#     return connection

# def connect_to_postgresql(host, user, password, database):
#     """Establish a connection to the PostgreSQL database."""
#     connection = psycopg2.connect(
#         host=host,
#         user=user,
#         password=password,
#         database=database
#     )
#     print("Connect to PostgreSQL database")
#     return connection


# def fetch_mysql_data(connection, query):
#     """Fetch data from MySQL specifically for Insentive_given_saleorder."""
#     df = pd.read_sql(query, connection)
#     print(f"Data fetched successfully from MySQL with {len(df)} records.")
#     return df

# def fetch_postgresql_data(pgconnection, query):
#     """Fetch data from PostgreSQL."""
#     df = pd.read_sql(query, pgconnection)
#     print(f"Data fetched successfully from PostgreSQL with {len(df)} records.")
#     return df

# # Step 3: Process DataFrame (if needed)
# def process_dataframe(df):
#     """Process the DataFrame by cleaning it."""
#     print("Processing DataFrame...")
#     df_cleaned = df.dropna()
#     print("DataFrame processed. Remaining records:", len(df_cleaned))
#     return df_cleaned

# # Step 4: Define SQL Queries
# def get_account_move_query():
#     """Return the SQL query for fetching account move data."""
#     return """
#     SELECT 
#         id AS p_id,
#         name,
#         state,
#         payment_reference,
#         payment_state,
#         invoice_partner_display_name,
#         invoice_origin,
#         invoice_date,
#         date,
#         amount_untaxed AS payment_untax,
#         amount_tax AS payment_amount_tax,
#         amount_total AS payment_total,
#         create_date,
#         einvoice_status 
#     FROM 
#         account_move 
#     WHERE 
#         state = 'posted' 
#         AND payment_state = 'paid' AND invoice_origin LIKE 'S%';
#     """

# def get_sale_order_query():
#     """Return the SQL query for fetching sale order data."""
#     return """
#     SELECT 
#         id AS so_id,
#         name AS sale_order,
#         amount_untaxed AS sale_order_untax,
#         amount_tax AS sale_order_amount_tax,
#         amount_total AS sale_order_amount_total,
#         margin AS sale_order_margin,
#         create_date AS sale_order_date,
#         pre_salesman_user_id
#     FROM 
#         sale_order where name like 'S%';
#     """

# def get_salesperson_query(order_ids):
#     """Return the SQL query for fetching salesperson data based on the order_ids."""
#     order_ids_str = ', '.join(map(str, order_ids))  # Convert list to a comma-separated string
#     return f"""
#     SELECT 
#         order_id,
#         salesperson_id 
#     FROM 
#         sale_gamification_data 
#     WHERE 
#         order_id IN ({order_ids_str});
#     """

# # Step 5: Inner Join DataFrames
# def inner_join_dataframes(df_sale_order, df_account_move):
#     """Perform an inner join on the two DataFrames."""
#     print("Performing inner join on sale order and account move DataFrames...")
#     merged_df = pd.merge(df_sale_order, df_account_move, left_on='sale_order', right_on='invoice_origin', how='inner')
#     print("Inner join completed. Merged records:", len(merged_df))
#     return merged_df


# def get_salesperson_names(pgconnection, salesperson_ids):
#     """Get the names of salespersons based on their salesperson IDs."""
#     if not salesperson_ids:
#         return {}

#     # Remove NaN or invalid values from salesperson_ids
#     valid_salesperson_ids = [sid for sid in salesperson_ids if pd.notna(sid)]

#     if not valid_salesperson_ids:
#         print("No valid salesperson IDs available.")
#         return {}

#     # Step 1: Get partner_ids from res_users using the salesperson_ids
#     ids_str = ', '.join(map(str, valid_salesperson_ids))
#     partner_id_query = f"""
#     SELECT id, partner_id 
#     FROM res_users 
#     WHERE id IN ({ids_str});
#     """
#     partner_id_df = fetch_postgresql_data(pgconnection, partner_id_query)

#     if partner_id_df.empty:
#         print("No partner_ids found for given salesperson_ids.")
#         return {}  # Return empty dict if no partner_ids are found

#     # Create a mapping of salesperson_id to partner_id
#     partner_id_map = dict(zip(partner_id_df['id'], partner_id_df['partner_id']))

#     # Step 2: Get names from res_partner using the partner_ids
#     partner_ids = list(partner_id_map.values())
#     if not partner_ids:
#         print("No partner_ids available to fetch names.")
#         return {}  # Return empty dict if no partner_ids are available

#     partner_ids_str = ', '.join(map(str, partner_ids))
#     name_query = f"""
#     SELECT id, name 
#     FROM res_partner 
#     WHERE id IN ({partner_ids_str});
#     """
#     name_df = fetch_postgresql_data(pgconnection, name_query)

#     # Create a mapping of partner_id to name
#     name_map = dict(zip(name_df['id'], name_df['name']))

#     # Create a final mapping of salesperson_id to name
#     salesperson_name_map = {}
#     for salesperson_id, partner_id in partner_id_map.items():
#         salesperson_name_map[salesperson_id] = name_map.get(partner_id, None)

#     return salesperson_name_map


# # Update the fetch_salesperson_data function to use batch fetching
# def fetch_salesperson_data(pgconnection, merged_df):
#     """Fetch salesperson data for the given merged DataFrame and get their names."""
#     order_ids = merged_df['so_id'].unique()  # Get unique order IDs
#     query = get_salesperson_query(order_ids)
#     salesperson_df = fetch_postgresql_data(pgconnection, query)
    
#     # Initialize salesperson dictionary
#     salesperson_dict = {order_id: {'salesperson1': None, 'salesperson2': None} for order_id in order_ids}
#     for _, row in salesperson_df.iterrows():
#         order_id = row['order_id']
#         salesperson_id = row['salesperson_id']
        
#         # Assign salesperson IDs based on their availability
#         if salesperson_dict[order_id]['salesperson1'] is None:
#             salesperson_dict[order_id]['salesperson1'] = salesperson_id
#             salesperson_dict[order_id]['salesperson2'] = salesperson_id
#         else:
#             salesperson_dict[order_id]['salesperson2'] = salesperson_id

#     # Fetch names for all unique salesperson IDs
#     unique_salesperson_ids = {salesperson_id for salespersons in salesperson_dict.values() for salesperson_id in salespersons.values() if salesperson_id}
#     salesperson_names = get_salesperson_names(pgconnection, unique_salesperson_ids)

#     # Create a DataFrame from the salesperson IDs and fetch names
#     salesperson_list = []
#     for order_id in merged_df['so_id']:
#         salesperson_info = salesperson_dict[order_id]
#         salesperson_info['salesperson1_name'] = salesperson_names.get(salesperson_info['salesperson1'], None)
#         salesperson_info['salesperson2_name'] = salesperson_names.get(salesperson_info['salesperson2'], None)
#         salesperson_list.append(salesperson_info)
    
#     # Convert to DataFrame and concatenate with the merged DataFrame
#     salesperson_df = pd.DataFrame(salesperson_list)
#     result_df = pd.concat([merged_df.reset_index(drop=True), salesperson_df], axis=1)

#     print("Salesperson data added to the merged DataFrame.")
#     return result_df

# def remove_existing_payments(connection, result_df):
#     """Remove rows from result_df where payment_reference exists in the Insentive_given_saleorder."""
#     # Step 1: Fetch payment_ref from the Insentive_given_saleorder
#     payment_ref_query = "SELECT payment_ref FROM weekly.Insentive_given_saleorder;"
#     payment_ref_df = fetch_mysql_data(connection, payment_ref_query)
    
#     # Step 2: Convert payment_ref DataFrame to a set for faster lookup
#     existing_payment_refs = set(payment_ref_df['payment_ref'].dropna())  # Using dropna to avoid None values

#     # Step 3: Filter out rows in result_df where payment_reference exists in existing_payment_refs
#     filtered_result_df = result_df[~result_df['payment_reference'].isin(existing_payment_refs)]
    
#     print("Rows removed. Remaining records:", len(filtered_result_df))
#     return filtered_result_df


# # ..............................ek j vaar cost add karse........................... 
# def add_cost_and_final_amount(connection, result_df):
#     """Add 'cost' and 'final_amount' columns to the result DataFrame, updating costs for duplicates
#     and setting cost to 0 if sale_order exists in Insentive_given_saleorder."""
#     print("Adding 'cost' and 'final_amount' columns...")

#     # Step 1: Fetch sale_orders from Insentive_given_saleorder table
#     incentive_query = "SELECT saleorder FROM weekly.Insentive_given_saleorder;"
#     incentive_df = fetch_mysql_data(connection, incentive_query)
#     incentive_sale_orders = set(incentive_df['saleorder'].dropna())

#     # Step 2: Calculate 'cost' initially for all rows
#     result_df['cost'] = result_df['sale_order_untax'] - result_df['sale_order_margin']

#     # Step 3: Set cost to 0 if sale_order exists in the incentive_sale_orders list
#     result_df.loc[result_df['sale_order'].isin(incentive_sale_orders), 'cost'] = 0

#     # Step 4: Identify duplicate sale_order_ids and adjust costs if necessary
#     duplicates = result_df[result_df.duplicated(subset=['sale_order'], keep=False)]

#     # If there are duplicates, set costs for all but one of them to 0
#     if not duplicates.empty:
#         for sale_order_id in duplicates['sale_order'].unique():
#             mask = (result_df['sale_order'] == sale_order_id)
#             # Set cost for all but the first occurrence to 0
#             result_df.loc[mask, 'cost'] = 0  # Reset all costs for duplicates
#             first_index = result_df[mask].index[0]  # Get the first occurrence index
#             # Calculate cost only for the first occurrence if it doesn't exist in incentive_sale_orders
#             if result_df.loc[first_index, 'sale_order'] not in incentive_sale_orders:
#                 result_df.loc[first_index, 'cost'] = result_df.loc[first_index, 'sale_order_untax'] - result_df.loc[first_index, 'sale_order_margin']

#     # Step 5: Calculate 'final_amount' using the payment_total column
#     result_df['final_amount'] = (result_df['payment_total'] / 1.18) - result_df['cost']

#     print("Columns added successfully.")
#     return result_df

# def get_presalesperson_names(pgconnection, presalesperson_ids):
#     """Fetch presalesperson names based on their user IDs."""
#     if isinstance(presalesperson_ids, pd.Series):
#         presalesperson_ids = presalesperson_ids.dropna().unique()  # Convert to a unique array

#     presalesperson_ids = presalesperson_ids.tolist() if isinstance(presalesperson_ids, np.ndarray) else presalesperson_ids

#     if len(presalesperson_ids) == 0:  # Explicitly check for an empty list
#         print("No valid presalesperson IDs available.")
#         return {}
#     # Step 1: Get partner_ids from res_users using the presalesperson_ids
#     ids_str = ', '.join(map(str, presalesperson_ids))
#     partner_id_query = f"""
#     SELECT id, partner_id 
#     FROM res_users 
#     WHERE id IN ({ids_str});
#     """
#     partner_id_df = fetch_postgresql_data(pgconnection, partner_id_query)

#     if partner_id_df.empty:
#         print("No partner_ids found for given presalesperson_ids.")
#         return {}

#     # Create a mapping of presalesperson_id to partner_id
#     partner_id_map = dict(zip(partner_id_df['id'], partner_id_df['partner_id']))

#     # Step 2: Get names from res_partner using the partner_ids
#     partner_ids = list(partner_id_map.values())
#     if len(partner_ids) == 0:
#         print("No partner_ids available to fetch names.")
#         return {}

#     partner_ids_str = ', '.join(map(str, partner_ids))
#     name_query = f"""
#     SELECT id, name 
#     FROM res_partner 
#     WHERE id IN ({partner_ids_str});
#     """
#     name_df = fetch_postgresql_data(pgconnection, name_query)

#     # Create a mapping of partner_id to name
#     name_map = dict(zip(name_df['id'], name_df['name']))

#     # Create a final mapping of presalesperson_id to name
#     presalesperson_name_map = {pid: name_map.get(partner_id_map[pid], None) for pid in presalesperson_ids}
#     print("Presalesperson names fetched successfully.")
#     return presalesperson_name_map


# def add_presalesperson_name(pgconnection, result_df):
#     """Add presalesperson names to the result DataFrame."""
#     print("Adding presalesperson names...")

#     # Extract unique presalesperson IDs from the DataFrame
#     presalesperson_ids = result_df['pre_salesman_user_id'].dropna().unique()

#     # Fetch presalesperson names
#     presalesperson_names = get_presalesperson_names(pgconnection, presalesperson_ids)

#     # Map presalesperson names to the DataFrame
#     result_df['presalesperson_name'] = result_df['pre_salesman_user_id'].map(presalesperson_names)

#     print("Presalesperson names added successfully.")
#     return result_df


# # Function to split duplicate payment references and remove them
# def calculate_disputes(result_df):
#     # Find payment references that are duplicated (appears more than once)
#     duplicate_payment_refs = result_df['payment_reference'][result_df['payment_reference'].duplicated(keep=False)].unique()
    
#     # Filter out rows that have duplicate payment references
#     duplicate_payments = result_df[result_df['payment_reference'].isin(duplicate_payment_refs)]
    
#     # Keep only rows with unique payment references
#     unique_payments = result_df[~result_df['payment_reference'].isin(duplicate_payment_refs)]
    
#     return unique_payments, duplicate_payments

# # Step 7: Main function to execute the workflow
# def Insentive_Final_dump():

#     # MYSQL connection (For all other tables)
#     host = '103.180.186.207'
#     user = 'qrt'
#     password = 't7%><rC)MC)8rdsYCj<E'
#     database='weekly'


#     # PostgreSQL connection (For all other tables)
#     postgres_host = '103.180.186.233'
#     postgres_user = 'odoo'
#     postgres_password = 'TRQ@#$%QRT@#&*()!TRQ'
#     postgres_database = 'egniol_production'
 
    
#     # Connect to the database
#     connection = connect_to_database(host, user, password, database)
#     postgres_connection = connect_to_postgresql(postgres_host, postgres_user, postgres_password, postgres_database)
    
#     try:
#         # Fetch and process data for account_move
#         account_move_query = get_account_move_query()
#         df_account_move = fetch_postgresql_data(postgres_connection, account_move_query)
#         df_account_move_processed = process_dataframe(df_account_move)

#         # Fetch and process data for sale_order
#         sale_order_query = get_sale_order_query()
#         df_sale_order = fetch_postgresql_data(postgres_connection, sale_order_query)
#         df_sale_order_processed = df_sale_order

#         # Perform inner join
#         merged_df = inner_join_dataframes(df_sale_order_processed, df_account_move_processed)

#         # Fetch salesperson data based on the merged DataFrame
#         result_df = fetch_salesperson_data(postgres_connection, merged_df)

#         result_df = add_presalesperson_name(postgres_connection, result_df)
        
#         # Remove rows with existing payment_reference
#         result_df = remove_existing_payments(connection, result_df)

#         # Add cost and final_amount columns
#         result_df = add_cost_and_final_amount(connection ,result_df)
#         #  result_df = add_cost_and_final_amount(result_df)
        
#         # Calculate Disputes
#         unique_payments, duplicate_payments = calculate_disputes(result_df)

#         # Save the resulting DataFrame to a CSV file
#         unique_payments.to_csv('incentive_dump.csv')
#         duplicate_payments.to_csv('disputes.csv')
#         print("Result saved to 'incentive_dump.csv' & 'disputes.csv'.")

#     finally:
#         # Close the database connection
#         connection.close()
#         print("Database connection closed.")

# # if __name__ == "__main__":
# #     Insentive_Final_dump()






import mysql.connector
import pandas as pd
import psycopg2
import numpy as np

# Step 1: Connect to MySQL Database
def connect_to_database(host, user, password, database):
    """Establish a connection to the MySQL database."""
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    print("Connect to MySQL the database...")
    return connection

def connect_to_postgresql(host, user, password, database):
    """Establish a connection to the PostgreSQL database."""
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    print("Connect to PostgreSQL database")
    return connection


def fetch_mysql_data(connection, query):
    """Fetch data from MySQL specifically for Insentive_given_saleorder."""
    df = pd.read_sql(query, connection)
    print(f"Data fetched successfully from MySQL with {len(df)} records.")
    return df

def fetch_postgresql_data(pgconnection, query):
    """Fetch data from PostgreSQL."""
    df = pd.read_sql(query, pgconnection)
    print(f"Data fetched successfully from PostgreSQL with {len(df)} records.")
    return df

# Step 3: Process DataFrame (if needed)
def process_dataframe(df):
    """Process the DataFrame by cleaning it."""
    print("Processing DataFrame...")
    df_cleaned = df.dropna()
    print("DataFrame processed. Remaining records:", len(df_cleaned))
    return df_cleaned

# Step 4: Define SQL Queries
def get_account_move_query():
    """Return the SQL query for fetching account move data."""
    return """
    SELECT 
        id AS p_id,
        name,
        state,
        payment_reference,
        payment_state,
        invoice_partner_display_name,
        invoice_origin,
        invoice_date,
        date,
        amount_untaxed AS payment_untax,
        amount_tax AS payment_amount_tax,
        amount_total AS payment_total,
        create_date,
        einvoice_status 
    FROM 
        account_move 
    WHERE 
        state = 'posted' 
        AND payment_state = 'paid' AND invoice_origin LIKE 'S%';
    """

def get_sale_order_query():
    """Return the SQL query for fetching sale order data."""
    return """
    SELECT 
        id AS so_id,
        name AS sale_order,
        amount_untaxed AS sale_order_untax,
        amount_tax AS sale_order_amount_tax,
        amount_total AS sale_order_amount_total,
        margin AS sale_order_margin,
        create_date AS sale_order_date,
        pre_salesman_user_id
    FROM 
        sale_order where name like 'S%';
    """

def get_salesperson_query(order_ids):
    """Return the SQL query for fetching salesperson data based on the order_ids."""
    order_ids_str = ', '.join(map(str, order_ids))  # Convert list to a comma-separated string
    return f"""
    SELECT 
        order_id,
        salesperson_id 
    FROM 
        sale_gamification_data 
    WHERE 
        order_id IN ({order_ids_str});
    """

# Step 5: Inner Join DataFrames
def inner_join_dataframes(df_sale_order, df_account_move):
    """Perform an inner join on the two DataFrames."""
    print("Performing inner join on sale order and account move DataFrames...")
    merged_df = pd.merge(df_sale_order, df_account_move, left_on='sale_order', right_on='invoice_origin', how='inner')
    print("Inner join completed. Merged records:", len(merged_df))
    return merged_df


def get_salesperson_names(pgconnection, salesperson_ids):
    """Get the names of salespersons based on their salesperson IDs."""
    if not salesperson_ids:
        return {}

    # Remove NaN or invalid values from salesperson_ids
    valid_salesperson_ids = [sid for sid in salesperson_ids if pd.notna(sid)]

    if not valid_salesperson_ids:
        print("No valid salesperson IDs available.")
        return {}

    # Step 1: Get partner_ids from res_users using the salesperson_ids
    ids_str = ', '.join(map(str, valid_salesperson_ids))
    partner_id_query = f"""
    SELECT id, partner_id 
    FROM res_users 
    WHERE id IN ({ids_str});
    """
    partner_id_df = fetch_postgresql_data(pgconnection, partner_id_query)

    if partner_id_df.empty:
        print("No partner_ids found for given salesperson_ids.")
        return {}  # Return empty dict if no partner_ids are found

    # Create a mapping of salesperson_id to partner_id
    partner_id_map = dict(zip(partner_id_df['id'], partner_id_df['partner_id']))

    # Step 2: Get names from res_partner using the partner_ids
    partner_ids = list(partner_id_map.values())
    if not partner_ids:
        print("No partner_ids available to fetch names.")
        return {}  # Return empty dict if no partner_ids are available

    partner_ids_str = ', '.join(map(str, partner_ids))
    name_query = f"""
    SELECT id, name 
    FROM res_partner 
    WHERE id IN ({partner_ids_str});
    """
    name_df = fetch_postgresql_data(pgconnection, name_query)

    # Create a mapping of partner_id to name
    name_map = dict(zip(name_df['id'], name_df['name']))

    # Create a final mapping of salesperson_id to name
    salesperson_name_map = {}
    for salesperson_id, partner_id in partner_id_map.items():
        salesperson_name_map[salesperson_id] = name_map.get(partner_id, None)

    return salesperson_name_map


# Updated fetch_salesperson_data function to set salesperson2 = salesperson1 if blank
def fetch_salesperson_data(pgconnection, merged_df):
    """Fetch salesperson data for the given merged DataFrame and get their names."""
    order_ids = merged_df['so_id'].unique()  # Get unique order IDs
    query = get_salesperson_query(order_ids)
    salesperson_df = fetch_postgresql_data(pgconnection, query)
    
    # Initialize salesperson dictionary with three salespeople
    salesperson_dict = {order_id: {'salesperson1': None, 'salesperson2': None, 'salesperson3': None} for order_id in order_ids}
    for _, row in salesperson_df.iterrows():
        order_id = row['order_id']
        salesperson_id = row['salesperson_id']
        
        # Assign salesperson IDs based on their availability
        if salesperson_dict[order_id]['salesperson1'] is None:
            salesperson_dict[order_id]['salesperson1'] = salesperson_id
            salesperson_dict[order_id]['salesperson2'] = salesperson_id  # Set salesperson2 to salesperson1 by default
        elif salesperson_dict[order_id]['salesperson2'] is None or salesperson_dict[order_id]['salesperson2'] == salesperson_dict[order_id]['salesperson1']:
            salesperson_dict[order_id]['salesperson2'] = salesperson_id
        elif salesperson_dict[order_id]['salesperson3'] is None:
            salesperson_dict[order_id]['salesperson3'] = salesperson_id

    # Fetch names for all unique salesperson IDs
    unique_salesperson_ids = {salesperson_id for salespersons in salesperson_dict.values() for salesperson_id in salespersons.values() if salesperson_id}
    salesperson_names = get_salesperson_names(pgconnection, unique_salesperson_ids)

    # Create a DataFrame from the salesperson IDs and fetch names
    salesperson_list = []
    for order_id in merged_df['so_id']:
        salesperson_info = salesperson_dict[order_id]
        salesperson_info['salesperson1_name'] = salesperson_names.get(salesperson_info['salesperson1'], None)
        salesperson_info['salesperson2_name'] = salesperson_names.get(salesperson_info['salesperson2'], None)
        salesperson_info['salesperson3_name'] = salesperson_names.get(salesperson_info['salesperson3'], None)
        salesperson_list.append(salesperson_info)
    
    # Convert to DataFrame and concatenate with the merged DataFrame
    salesperson_df = pd.DataFrame(salesperson_list)
    result_df = pd.concat([merged_df.reset_index(drop=True), salesperson_df], axis=1)

    print("Salesperson data added to the merged DataFrame.")
    return result_df

def remove_existing_payments(connection, result_df):
    """Remove rows from result_df where payment_reference exists in the Insentive_given_saleorder."""
    # Step 1: Fetch payment_ref from the Insentive_given_saleorder
    payment_ref_query = "SELECT payment_ref FROM weekly.Insentive_given_saleorder;"
    payment_ref_df = fetch_mysql_data(connection, payment_ref_query)
    
    # Step 2: Convert payment_ref DataFrame to a set for faster lookup
    existing_payment_refs = set(payment_ref_df['payment_ref'].dropna())  # Using dropna to avoid None values

    # Step 3: Filter out rows in result_df where payment_reference exists in existing_payment_refs
    filtered_result_df = result_df[~result_df['payment_reference'].isin(existing_payment_refs)]
    
    print("Rows removed. Remaining records:", len(filtered_result_df))
    return filtered_result_df


def add_cost_and_final_amount(connection, result_df):
    """Add 'cost' and 'final_amount' columns to the result DataFrame, updating costs for duplicates
    and setting cost to 0 if sale_order exists in Insentive_given_saleorder."""
    print("Adding 'cost' and 'final_amount' columns...")

    # Step 1: Fetch sale_orders from Insentive_given_saleorder table
    incentive_query = "SELECT saleorder FROM weekly.Insentive_given_saleorder;"
    incentive_df = fetch_mysql_data(connection, incentive_query)
    incentive_sale_orders = set(incentive_df['saleorder'].dropna())

    # Step 2: Calculate 'cost' initially for all rows
    result_df['cost'] = result_df['sale_order_untax'] - result_df['sale_order_margin']

    # Step 3: Set cost to 0 if sale_order exists in the incentive_sale_orders list
    result_df.loc[result_df['sale_order'].isin(incentive_sale_orders), 'cost'] = 0

    # Step 4: Identify duplicate sale_order_ids and adjust costs if necessary
    duplicates = result_df[result_df.duplicated(subset=['sale_order'], keep=False)]

    # If there are duplicates, set costs for all but one of them to 0
    if not duplicates.empty:
        for sale_order_id in duplicates['sale_order'].unique():
            mask = (result_df['sale_order'] == sale_order_id)
            # Set cost for all but the first occurrence to 0
            result_df.loc[mask, 'cost'] = 0  # Reset all costs for duplicates
            first_index = result_df[mask].index[0]  # Get the first occurrence index
            # Calculate cost only for the first occurrence if it doesn't exist in incentive_sale_orders
            if result_df.loc[first_index, 'sale_order'] not in incentive_sale_orders:
                result_df.loc[first_index, 'cost'] = result_df.loc[first_index, 'sale_order_untax'] - result_df.loc[first_index, 'sale_order_margin']

    # Step 5: Calculate 'final_amount' using the payment_total column
    result_df['final_amount'] = (result_df['payment_total'] / 1.18) - result_df['cost']

    print("Columns added successfully.")
    return result_df

def get_presalesperson_names(pgconnection, presalesperson_ids):
    """Fetch presalesperson names based on their user IDs."""
    if isinstance(presalesperson_ids, pd.Series):
        presalesperson_ids = presalesperson_ids.dropna().unique()  # Convert to a unique array

    presalesperson_ids = presalesperson_ids.tolist() if isinstance(presalesperson_ids, np.ndarray) else presalesperson_ids

    if len(presalesperson_ids) == 0:  # Explicitly check for an empty list
        print("No valid presalesperson IDs available.")
        return {}
    # Step 1: Get partner_ids from res_users using the presalesperson_ids
    ids_str = ', '.join(map(str, presalesperson_ids))
    partner_id_query = f"""
    SELECT id, partner_id 
    FROM res_users 
    WHERE id IN ({ids_str});
    """
    partner_id_df = fetch_postgresql_data(pgconnection, partner_id_query)

    if partner_id_df.empty:
        print("No partner_ids found for given presalesperson_ids.")
        return {}

    # Create a mapping of presalesperson_id to partner_id
    partner_id_map = dict(zip(partner_id_df['id'], partner_id_df['partner_id']))

    # Step 2: Get names from res_partner using the partner_ids
    partner_ids = list(partner_id_map.values())
    if len(partner_ids) == 0:
        print("No partner_ids available to fetch names.")
        return {}

    partner_ids_str = ', '.join(map(str, partner_ids))
    name_query = f"""
    SELECT id, name 
    FROM res_partner 
    WHERE id IN ({partner_ids_str});
    """
    name_df = fetch_postgresql_data(pgconnection, name_query)

    # Create a mapping of partner_id to name
    name_map = dict(zip(name_df['id'], name_df['name']))

    # Create a final mapping of presalesperson_id to name
    presalesperson_name_map = {pid: name_map.get(partner_id_map[pid], None) for pid in presalesperson_ids}
    print("Presalesperson names fetched successfully.")
    return presalesperson_name_map


def add_presalesperson_name(pgconnection, result_df):
    """Add presalesperson names to the result DataFrame."""
    print("Adding presalesperson names...")

    # Extract unique presalesperson IDs from the DataFrame
    presalesperson_ids = result_df['pre_salesman_user_id'].dropna().unique()

    # Fetch presalesperson names
    presalesperson_names = get_presalesperson_names(pgconnection, presalesperson_ids)

    # Map presalesperson names to the DataFrame
    result_df['presalesperson_name'] = result_df['pre_salesman_user_id'].map(presalesperson_names)

    print("Presalesperson names added successfully.")
    return result_df


# Function to split duplicate payment references and remove them
def calculate_disputes(result_df):
    # Find payment references that are duplicated (appears more than once)
    duplicate_payment_refs = result_df['payment_reference'][result_df['payment_reference'].duplicated(keep=False)].unique()
    
    # Filter out rows that have duplicate payment references
    duplicate_payments = result_df[result_df['payment_reference'].isin(duplicate_payment_refs)]
    
    # Keep only rows with unique payment references
    unique_payments = result_df[~result_df['payment_reference'].isin(duplicate_payment_refs)]
    
    return unique_payments, duplicate_payments

# Step 7: Main function to execute the workflow
def Insentive_Final_dump():

    # MYSQL connection (For all other tables)
    host = '103.180.186.207'
    user = 'qrt'
    password = 't7%><rC)MC)8rdsYCj<E'
    database='weekly'


    # PostgreSQL connection (For all other tables)
    postgres_host = '103.180.186.233'
    postgres_user = 'odoo'
    postgres_password = 'TRQ@#$%QRT@#&*()!TRQ'
    postgres_database = 'egniol_production'
 
    
    # Connect to the database
    connection = connect_to_database(host, user, password, database)
    postgres_connection = connect_to_postgresql(postgres_host, postgres_user, postgres_password, postgres_database)
    
    try:
        # Fetch and process data for account_move
        account_move_query = get_account_move_query()
        df_account_move = fetch_postgresql_data(postgres_connection, account_move_query)
        df_account_move_processed = process_dataframe(df_account_move)

        # Fetch and process data for sale_order
        sale_order_query = get_sale_order_query()
        df_sale_order = fetch_postgresql_data(postgres_connection, sale_order_query)
        df_sale_order_processed = df_sale_order

        # Perform inner join
        merged_df = inner_join_dataframes(df_sale_order_processed, df_account_move_processed)

        # Fetch salesperson data based on the merged DataFrame
        result_df = fetch_salesperson_data(postgres_connection, merged_df)

        result_df = add_presalesperson_name(postgres_connection, result_df)
        
        # Remove rows with existing payment_reference
        result_df = remove_existing_payments(connection, result_df)

        # Add cost and final_amount columns
        result_df = add_cost_and_final_amount(connection ,result_df)
        
        # Calculate Disputes
        unique_payments, duplicate_payments = calculate_disputes(result_df)

        # Save the resulting DataFrame to a CSV file
        unique_payments.to_csv('incentive_dump.csv')
        duplicate_payments.to_csv('disputes.csv')
        print("Result saved to 'incentive_dump.csv' & 'disputes.csv'.")

    finally:
        # Close the database connection
        connection.close()
        print("Database connection closed.")

# if __name__ == "__main__":
#     Insentive_Final_dump()