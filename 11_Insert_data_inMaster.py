import pandas as pd
import mysql.connector
from mysql.connector import Error
import configparser
import requests

def load_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

config_file = './send_msg/config.ini'
config = load_config(config_file)

# Load the updated CSV data into a DataFrame
df = pd.read_csv('Updated_incentive_dump_Final.csv')

def send_whatsapp_message():
    token = config['whatsapp']['ultramsg_token']
    to  = "120363361979418877@g.us"
    message = "🚀 *Refund Counted 1 also update in databse* ✅"
    """Send a WhatsApp message using UltraMsg API."""
    url = config['whatsapp']['ultramsg_chat_endpoint']
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "token": token,
        "to": to,
        "body": message
    }

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        print(f"Message sent successfully to {to}.")
    else:
        print(f"Failed to send message to {to}. Status code: {response.status_code}")

# Replace NaN with None for all columns explicitly
df = df.where(pd.notnull(df), None)

# Replace empty strings with None (optional)
df = df.replace('', None)

# Replace None with 'Unknown' in the entire DataFrame
df = df.fillna('Unknown')

try:
    connection = mysql.connector.connect(
        host='103.180.186.207',  
        database='weekly',  
        user='qrt',  
        password='t7%><rC)MC)8rdsYCj<E'
    )

    if connection.is_connected():
        print("Connected to MySQL database")

        insert_query = """
        INSERT INTO incentive_master (
            sale_order, sale_order_untax, sale_order_amount_tax, sale_order_amount_total,
            sale_order_margin, sale_order_date, name, state, payment_reference, payment_state,
            invoice_partner_display_name, invoice_date, date, payment_untax, payment_amount_tax,
            payment_total, create_date, einvoice_status, salesperson1_name, TL1, BM1, salesperson2_name,
            salesperson3_name,
            TL2, BM2, presalesperson_name, cost, final_amount, type, tag
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor = connection.cursor()

        for index, row in df.iterrows():
            try:
                # Prepare the data to be inserted
                data = (
                    row['sale_order'],
                    row['sale_order_untax'],
                    row['sale_order_amount_tax'],
                    row['sale_order_amount_total'],
                    row['sale_order_margin'],
                    row['sale_order_date'],
                    row['name'],
                    row['state'],
                    row['payment_reference'],
                    row['payment_state'],
                    row['invoice_partner_display_name'],
                    row['invoice_date'],
                    row['date'],
                    row['payment_untax'],
                    row['payment_amount_tax'],
                    row['payment_total'],
                    row['create_date'],
                    row['einvoice_status'],
                    row['salesperson1_name'],
                    row['TL1'],
                    row['BM1'],
                    row['salesperson2_name'],
                    row['salesperson3_name'],
                    row['TL2'],
                    row['BM2'],
                    row['presalesperson_name'],
                    row['cost'],
                    row['final_amount'],
                    row['source'],
                    '10TH week (25-26): 21-06-2025'
                )

                # Execute the insert query
                cursor.execute(insert_query, data)
            except Error as row_error:
                print(f"Error inserting row {index}: {row_error}")
                print(f"Row data: {row}")  # Optional: For debugging specific row data

        # Commit the transaction for inserts
        connection.commit()
        print(f"Records inserted successfully.")

        # Execute the UPDATE query
        update_query = """
        UPDATE weekly.refundDetails 
        SET counted = 1 
        WHERE counted IS NULL;
        """
        try:
            cursor.execute(update_query)
            send_whatsapp_message()
            connection.commit()
            print("refundDetails table updated successfully.")
        except Error as update_error:
            print(f"Error updating refundDetails: {update_error}")

except Error as e:
    print(f"Error: {e}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed.")