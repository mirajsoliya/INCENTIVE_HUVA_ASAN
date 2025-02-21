import pandas as pd
import mysql.connector
from mysql.connector import Error

def insert_refund_data(csv_file_path):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)
        df.columns = df.columns.str.strip()  # Remove leading/trailing spaces from column names

        # Strip leading and trailing whitespace from all string columns
        df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

        # Convert 'Date of Refund' to datetime format, handle invalid dates
        def parse_date(date):
            try:
                return pd.to_datetime(date, format='%d-%m-%Y').date()
            except ValueError:
                return pd.to_datetime('0001-01-01').date()  # Use placeholder for invalid dates

        df['Date of Refund'] = df['Date of Refund'].apply(parse_date)

        # Clean the 'Amount of Refund' column by removing currency symbols and commas, then convert to float
        df['Amount of Refund'] = df['Amount of Refund'].replace({'₹': '', ',': ''}, regex=True).astype(float)

        # Replace NaN values with None
        df = df.where(pd.notnull(df), None)

        # Establish a connection to the MySQL database
        connection = mysql.connector.connect(
            host='103.180.186.207',  # Replace with your host
            user='qrt',              # Replace with your username
            password='t7%><rC)MC)8rdsYCj<E',  # Replace with your password
            database='weekly'        # Replace with your database name
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Prepare the SQL insert statement
            insert_query = """
            INSERT INTO refundDetails (
                partyName, bdm1, bdm2, branch, bookingID, amountOfRefund,
                actualRefund, serviceDetails, service, reasonForRefund,
                esp_finance, dateOfRefund
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Iterate over the DataFrame rows and insert each row into the table
            for index, row in df.iterrows():
                try:
                    data = (
                        row['Name of Party'],
                        row['BDE1'],
                        row['BDE2'],
                        row['BRANCH'],
                        row['BOOKING ID'],
                        row['Amount of Refund'],
                        row['Actual Refund'],
                        row['SERVICE DETAILS'],
                        row['SERVICE'],
                        row['Reason for refund'],
                        row['ESPL/Finance'],
                        row['Date of Refund']
                    )
                    cursor.execute(insert_query, data)
                except Exception as row_error:
                    print(f"Error inserting row at index {index}: {row_error}")

            # Commit the transaction
            connection.commit()
            print(f"{cursor.rowcount} records inserted successfully.")

    except Error as db_error:
        print(f"Database connection error: {db_error}")
    except Exception as general_error:
        print(f"Error: {general_error}")
    finally:
        # Close the connection if it exists
        try:
            if connection.is_connected():
                cursor.close()
                connection.close()
        except NameError:
            print("Connection was not established.")

# Path to your CSV file
csv_file_path = 'data.csv'  # Replace with your actual file path
insert_refund_data(csv_file_path)
