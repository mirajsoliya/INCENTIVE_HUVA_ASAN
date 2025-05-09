# import pandas as pd
# import pymysql

# # Database connection details
# DB_HOST_MASTER = '103.180.186.207'
# DB_USER_MASTER = 'qrt'
# DB_PASSWORD_MASTER = 't7%><rC)MC)8rdsYCj<E'
# DB_DATABASE_MASTER = 'weekly'

# # Function to connect to the MySQL database
# def create_db_connection():
#     return pymysql.connect(
#         host=DB_HOST_MASTER,
#         user=DB_USER_MASTER,
#         password=DB_PASSWORD_MASTER,
#         database=DB_DATABASE_MASTER
#     )

# # Function to fetch refund details for a BDM
# def get_refund_details(connection, bdm_name):
#     with connection.cursor() as cursor:
#         # Query to check if the BDM name exists in either bdm1 or bdm2
#         query = f"SELECT bdm1, bdm2, amountOfRefund FROM weekly.refundDetails WHERE (bdm1 = %s OR bdm2 = %s) and actualRefund in ('REFUND' ,'CA COMMISSION') AND counted IS NULL;"
#         cursor.execute(query, (bdm_name, bdm_name))
#         return cursor.fetchall()

# # Function to process each dataframe (TL and BDM)
# def process_incentive_report(df, deduct_list, role, connection):
#     # Loop through each row in the dataframe
#     for index, row in df.iterrows():
#         bdm_name = row['BDM Name']
#         total_collection = row['Total Collection']
#         cases = row['Cases']

#         # Get refund details for the BDM name
#         refund_details = get_refund_details(connection, bdm_name)


#         total_refund_deducted = 0  # To track total refund deducted for this person

#         if refund_details:
#             for refund in refund_details:
#                 bdm1, bdm2, amount_of_refund = refund
#                 refund_deduct=0
#                 # Convert amount_of_refund to a floating-point number with 2 decimal precision
#                 amount_of_refund = round(float(amount_of_refund), 2)
#                 # Calculate the refund deduction
#                 if bdm1 == bdm_name and bdm2 == bdm_name:
#                     # Both bdm1 and bdm2 match, apply the first refund deduction formula
#                     refund_deduct = (amount_of_refund/1.18)*1.1
#                 elif bdm1 == bdm_name or bdm2 == bdm_name:
#                     # Only one of bdm1 or bdm2 matches, apply the second refund deduction formula
#                     refund_deduct = ((amount_of_refund/1.18)*1.1) / 2
#                 else:
#                     # No match, no refund deduction
#                     refund_deduct = 0   
#                 print(refund_deduct , "refund deduct")
#                 # Update the total refund deducted
#                 total_refund_deducted += refund_deduct

#                 # Apply the refund deduction and update the total collection
#             df.at[index, 'Total Collection'] -= total_refund_deducted
#             # Save the total refund deduction for this BDM in the summary list
#             deduct_list.append({'Name': bdm_name, 'Refund Deducted': round(total_refund_deducted, 2)})

#         else:
#             # If no refund details are found, retain the original Incentive from the file
#             print(f"No refund details found for BDM: {bdm_name}")

#         # Calculate the Incentive
#         incentive = (df.at[index, 'Total Collection']) * 0.05
#         df.at[index, 'Incentive'] = round(incentive, 2)

#     return df

# # Function to process the incentive reports and generate the final CSVs



# def process_incentive_reports(PSA_file, bdm_file, connection=None):
#     if connection is None:
#         connection = create_db_connection()

#     df_PSA = pd.read_csv(PSA_file)
#     df_bdm = pd.read_csv(bdm_file)

#     # Create DataFrames to track refund deductions
#     deduct_PSA = []
#     deduct_bdm = []


#     df_PSA_updated = process_incentive_report(df_PSA, deduct_PSA, 'PSA', connection)
#     df_bdm_updated = process_incentive_report(df_bdm, deduct_bdm, 'BDM', connection)

#     # Save the updated DataFrames to new CSV files
#     df_PSA_updated.to_csv(f'PSA_Final_incentive_report.csv', index=False)
#     df_bdm_updated.to_csv(f'BDM_Final_incentive_report.csv', index=False)

#     # Convert deduction lists to DataFrames and save separate summaries
#     refund_summary_PSA = pd.DataFrame(deduct_PSA)
#     refund_summary_bdm = pd.DataFrame(deduct_bdm)

#     refund_summary_PSA.to_csv(f'Refund_Deduction_Summary_PSA.csv', index=False)
#     refund_summary_bdm.to_csv(f'Refund_Deduction_Summary_BDM.csv', index=False)


#     connection.close()


# # Example usage
# def refund():
#     process_incentive_reports('PSA_Final_incentive_report.csv', 'BDM_Final_incentive_report.csv')
#     # process_incentive_reports('BDM_Final_incentive_report.csv')


# # if __name__ == "__main__":
# #     process_incentive_reports('TL_Final_incentive_report.csv', 'BDM_Final_incentive_report.csv')





import pandas as pd
import pymysql

# Database connection details
DB_HOST_MASTER = '103.180.186.207'
DB_USER_MASTER = 'qrt'
DB_PASSWORD_MASTER = 't7%><rC)MC)8rdsYCj<E'
DB_DATABASE_MASTER = 'weekly'

# Function to connect to the MySQL database
def create_db_connection():
    return pymysql.connect(
        host=DB_HOST_MASTER,
        user=DB_USER_MASTER,
        password=DB_PASSWORD_MASTER,
        database=DB_DATABASE_MASTER
    )

# Function to fetch refund details for a BDM
def get_refund_details(connection, bdm_name):
    with connection.cursor() as cursor:
        # Query to check if the BDM name exists in either bdm1 or bdm2
        query = f"SELECT bdm1, bdm2, amountOfRefund FROM weekly.refundDetails WHERE (bdm1 = %s OR bdm2 = %s) and actualRefund in ('REFUND' ,'CA COMMISSION') AND counted IS NULL;"
        cursor.execute(query, (bdm_name, bdm_name))
        return cursor.fetchall()

# Function to process each dataframe (TL and BDM)
def process_incentive_report(df, deduct_list, role, connection):
    # Loop through each row in the dataframe
    for index, row in df.iterrows():
        bdm_name = row['BDM Name']
        total_collection = row['Total Collection']
        cases = row['Cases']

        # Get refund details for the BDM name
        refund_details = get_refund_details(connection, bdm_name)


        total_refund_deducted = 0  # To track total refund deducted for this person

        if refund_details:
            for refund in refund_details:
                bdm1, bdm2, amount_of_refund = refund
                refund_deduct=0
                # Convert amount_of_refund to a floating-point number with 2 decimal precision
                amount_of_refund = round(float(amount_of_refund), 2)
                # Calculate the refund deduction
                if bdm1 == bdm_name and bdm2 == bdm_name:
                    # Both bdm1 and bdm2 match, apply the first refund deduction formula
                    refund_deduct = (amount_of_refund/1.18)*1.1
                elif bdm1 == bdm_name or bdm2 == bdm_name:
                    # Only one of bdm1 or bdm2 matches, apply the second refund deduction formula
                    refund_deduct = ((amount_of_refund/1.18)*1.1) / 2
                else:
                    # No match, no refund deduction
                    refund_deduct = 0   
                print(refund_deduct , "refund deduct")
                # Update the total refund deducted
                total_refund_deducted += refund_deduct

                # Apply the refund deduction and update the total collection
            df.at[index, 'Total Collection'] -= total_refund_deducted
            # Save the total refund deduction for this BDM in the summary list
            deduct_list.append({'Name': bdm_name, 'Refund Deducted': round(total_refund_deducted, 2)})

        else:
            # If no refund details are found, retain the original Incentive from the file
            print(f"No refund details found for BDM: {bdm_name}")

        # Calculate the Incentive
        incentive = (df.at[index, 'Total Collection']) * (0.05 if df.at[index, 'Total Collection'] >= 150000 else 0.03)
        df.at[index, 'Incentive'] = round(incentive, 2)

    return df

# Function to process the incentive reports and generate the final CSVs
def process_incentive_reports(PSA_file, bdm_file, connection=None):
    if connection is None:
        connection = create_db_connection()

    df_PSA = pd.read_csv(PSA_file)
    df_bdm = pd.read_csv(bdm_file)

    # Create DataFrames to track refund deductions
    deduct_PSA = []
    deduct_bdm = []


    df_PSA_updated = process_incentive_report(df_PSA, deduct_PSA, 'PSA', connection)
    df_bdm_updated = process_incentive_report(df_bdm, deduct_bdm, 'BDM', connection)

    # Save the updated DataFrames to new CSV files
    df_PSA_updated.to_csv(f'PSA_Final_incentive_report.csv', index=False)
    df_bdm_updated.to_csv(f'BDM_Final_incentive_report.csv', index=False)

    # Convert deduction lists to DataFrames and save separate summaries
    refund_summary_PSA = pd.DataFrame(deduct_PSA)
    refund_summary_bdm = pd.DataFrame(deduct_bdm)

    refund_summary_PSA.to_csv(f'Refund_Deduction_Summary_PSA.csv', index=False)
    refund_summary_bdm.to_csv(f'Refund_Deduction_Summary_BDM.csv', index=False)


    connection.close()


# Example usage
def refund():
    process_incentive_reports('PSA_Final_incentive_report.csv', 'BDM_Final_incentive_report.csv')
    # process_incentive_reports('BDM_Final_incentive_report.csv')


# if __name__ == "__main__":
#     process_incentive_reports('TL_Final_incentive_report.csv', 'BDM_Final_incentive_report.csv')