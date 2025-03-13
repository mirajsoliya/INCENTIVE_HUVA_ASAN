import pymysql
import pandas as pd

# Database connection details
DB_HOST = '103.180.186.207'
DB_USER = 'qrt'
DB_PASSWORD = 't7%><rC)MC)8rdsYCj<E'
DB_DATABASE = 'weekly'


def connect_db():
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Database connection successful!")
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


def get_employee_info(cursor, salesperson):
    if pd.isna(salesperson) or not salesperson.strip():
        return None
    
    query = """
        SELECT team_leader, role, branch_manager
        FROM weekly.sales_incentive_team 
        WHERE sales_person = %s
    """
    cursor.execute(query, (salesperson,))
    return cursor.fetchone()


def match_bdm_with_db(connection, df):
    try:
        cursor = connection.cursor()
        
        bdm_data = []
        missing_salesperson_rows = []
        modified_rows = []
        bdm_total_collection = {}
        bdm_roles = {}
        salesperson_companies = {}
        psa_total_collection = {}
        bdm_cases = {}

        for index, row in df.iterrows():
            salesperson1 = str(row.get('salesperson1_name', '')).strip() if isinstance(row.get('salesperson1_name'), str) else ''
            salesperson2 = str(row.get('salesperson2_name', '')).strip() if isinstance(row.get('salesperson2_name'), str) else ''
            presalesperson = str(row.get('presalesperson_name', '')).strip() if isinstance(row.get('presalesperson_name'), str) else ''
            final_amount = row.get('final_amount', 0)
            company = row.get('invoice_partner_display_name', '').strip()

            if not presalesperson or presalesperson.isspace():
                presalesperson = None

            bdm1_info = get_employee_info(cursor, salesperson1)
            bdm2_info = get_employee_info(cursor, salesperson2)
            psa_info = get_employee_info(cursor, presalesperson) if presalesperson else None
            
            if not bdm1_info and not bdm2_info:
                missing_salesperson_rows.append(row)
                continue

            tl1 = bdm1_info['team_leader'] if bdm1_info else 'Unknown'
            bm1 = bdm1_info['branch_manager'] if bdm1_info else 'Unknown'
            tl2 = bdm2_info['team_leader'] if bdm2_info else 'Unknown'
            bm2 = bdm2_info['branch_manager'] if bdm2_info else 'Unknown'

            if bdm1_info and bdm1_info['role'] == 'TL':
                salesperson1 = bdm1_info['team_leader']
            if bdm2_info and bdm2_info['role'] == 'TL':
                salesperson2 = bdm2_info['team_leader']

            if bdm1_info:
                bdm_roles[salesperson1] = bdm1_info['role']
            if bdm2_info:
                bdm_roles[salesperson2] = bdm2_info['role']
            if presalesperson:
                bdm_roles[presalesperson] = 'PSA'
                psa_total_collection[presalesperson] = psa_total_collection.get(presalesperson, 0) + (final_amount * 0.2)
            
            modified_row = row.copy()
            modified_row['salesperson1_name'] = salesperson1
            modified_row['salesperson2_name'] = salesperson2
            modified_row['TL1'] = tl1
            modified_row['BM1'] = bm1
            modified_row['TL2'] = tl2
            modified_row['BM2'] = bm2
            modified_rows.append(modified_row)

            collection = final_amount * 0.4 if presalesperson else final_amount * 0.5
            bdm_total_collection[salesperson1] = bdm_total_collection.get(salesperson1, 0) + collection
            bdm_total_collection[salesperson2] = bdm_total_collection.get(salesperson2, 0) + collection
            
            if presalesperson:
                psa_total_collection[presalesperson] = psa_total_collection.get(presalesperson, 0) + 1
                bdm_cases[salesperson1] = bdm_cases.get(salesperson1, 0) + 0.5
                bdm_cases[salesperson2] = bdm_cases.get(salesperson2, 0) + 0.5
                bdm_cases[presalesperson] = bdm_cases.get(presalesperson, 0) + 1
            else:
                bdm_cases[salesperson1] = bdm_cases.get(salesperson1, 0) + 0.5
                bdm_cases[salesperson2] = bdm_cases.get(salesperson2, 0) + 0.5
        
        for bdm, total_collection in {**bdm_total_collection, **psa_total_collection}.items():
            print(f"{bdm} Final Collection: {total_collection}")  # Debugging line
            incentive = total_collection * 0.05
            role = bdm_roles.get(bdm, 'Unknown')
            bdm_data.append({
                'BDM Name': bdm,
                'Role': role,
                'Total Collection': total_collection,
                'Incentive': incentive,
                'Cases': bdm_cases.get(bdm, 0)
            })

        bdm_df = pd.DataFrame(bdm_data)
        modified_df = pd.DataFrame(modified_rows)
        modified_df.to_csv('team_update_report.csv', index=False)
        print("Modified report saved to 'team_update_report.csv'.")

        if missing_salesperson_rows:
            missing_df = pd.DataFrame(missing_salesperson_rows)
            missing_df.to_csv('missing_salespersons.csv', index=False)
            print("Missing salesperson report saved to 'missing_salespersons.csv'.")
        else:
            print("No missing salesperson data found.")

        bdm_df.to_csv('incentive_report.csv', index=False)
        print("BDM incentive report saved to 'incentive_report.csv'.")

    except Exception as e:
        print(f"Error during BDM matching: {e}")


def load_csv_data(file_path):
    try:
        df = pd.read_csv(file_path)
        print("CSV data loaded successfully!")
        return df
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None


def incentive_Report():
    csv_file_path = 'incentive_dump_Final.csv'
    connection = connect_db()
    if connection is None:
        return

    df = load_csv_data(csv_file_path)
    if df is None:
        return

    match_bdm_with_db(connection, df)
    connection.close()






