import pandas as pd
import configparser
import requests

def load_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

# Load configuration settings
config_file = 'config.ini'
config = load_config(config_file)

def send_whatsapp_message(token, to, message):
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

# Load the main sales data CSV file
df = pd.read_csv('../team_update_report.csv')

# Ensure numeric columns are numeric, handle non-numeric or NaN values
df['cost'] = pd.to_numeric(df['cost'], errors='coerce').fillna(0)
df['final_amount'] = pd.to_numeric(df['final_amount'], errors='coerce').fillna(0)

# Load the salesperson names and contact numbers from the CSV file
# Assuming columns are named 'salesperson_name', 'contact_number', and 'refund_amount'
salespersons = pd.read_csv('../Not_Qualified_Salespersons.csv')

# Initialize an empty list to store the individual results
all_results = []

# Loop through each salesperson name and apply the filter, calculations
for index, row in salespersons.iterrows():
    salesperson_name = row['salesperson_name']
    contact_number = row['contact_number']  # Contact number for WhatsApp

    # Fetch refund amount for the current salesperson from the CSV
    refund_amount = row.get('refund_amount', 0)  # Default to 0 if column is missing
    formatted_refund_amount = f"₹{refund_amount:,.2f}"

    # Filter for rows where salesperson1_name or salesperson2_name matches the specified name
    filtered_df = df[
        (df['salesperson1_name'] == salesperson_name) | 
        (df['salesperson2_name'] == salesperson_name)
    ]
    
    # Select the columns needed
    result_df = filtered_df[['sale_order', 'salesperson1_name', 'salesperson2_name', 'cost', 'final_amount']].copy()
    
    result_df = filtered_df[['sale_order', 'salesperson1_name', 'salesperson2_name','presalesperson_name' ,'cost', 'final_amount']].copy()

    result_df['adjusted_amount'] = result_df.apply(
    lambda x: x['final_amount'] * 0.8 if pd.notna(x['presalesperson_name']) and x['presalesperson_name'] != "" else x['final_amount'], 
    axis=1
    )

    def calculate_counted_amount(row):
        if pd.notna(row['presalesperson_name']) and row['presalesperson_name'] != "":
            return row['adjusted_amount'] if row['salesperson1_name'] == row['salesperson2_name'] else row['adjusted_amount'] * 0.5
        else:
            return row['adjusted_amount'] if row['salesperson1_name'] == row['salesperson2_name'] else row['adjusted_amount'] * 0.5  # Normal split

    result_df['counted_amount'] = result_df.apply(calculate_counted_amount, axis=1)
    
    # Calculate 'incentive' as 5% of 'counted_amount'
    result_df['incentive'] = result_df['counted_amount'] * 0.05
    
    # Calculate the total incentive for this salesperson
    total_incentive = result_df['incentive'].sum()
    
    # Calculate the total counted amount for this salesperson
    total_counted_amount = result_df['counted_amount'].sum()
    
    # Calculate the counted amount after deducting the refund
    counted_after_refund = total_counted_amount - refund_amount
    incentive_after_refund = counted_after_refund * 0.05  # Updated incentive

    # Add the salesperson's name to each row for tracking
    result_df['salesperson'] = salesperson_name

    # Append this salesperson's result to the all_results list
    all_results.append(result_df)

    # Create a detailed message with calculations for each sale order
    message = (
        f"🌟 Hello *{salesperson_name}*,\n\n"
        f"⏳ *You're almost there!* You’re very close to qualifying for the incentive this week!\n\n"
        f"📊 *Here’s your current incentive breakdown:*\n\n"
    )


    # Add the breakdown for each sale order with improved formatting
    for _, sale in result_df.iterrows():
        message += (
            f"🔹 *Sale Order:* {sale['sale_order']}\n"
            f"   👤 *Salesperson 1:* {sale['salesperson1_name']}\n"
            f"   👤 *Salesperson 2:* {sale['salesperson2_name']}\n"
            f"   👤 *PSA :* {sale['presalesperson_name'] if pd.notna(sale['presalesperson_name']) and sale['presalesperson_name'] != '' else '-'}\n"
            f"   💼 *Cost:* ₹{sale['cost']:,}\n"
            f"   💰 *Final Amount:* ₹{sale['final_amount']:,}\n"
            f"   💰 *PSA Amount:* {'₹' + f'{sale['final_amount'] * 0.2:,.2f}' if pd.notna(sale['presalesperson_name']) and sale['presalesperson_name'] != '' else '-'}\n"
            f"   📈 *Counted Amount:* ₹{sale['counted_amount']:,}\n"
            f"   🎯 *Incentive:* ₹{sale['incentive']:.2f}\n\n"
    )

    # Add refund and updated calculation messages
    message += (
        f"🚀 *You're really close to qualifying for this week’s incentive!*\n"
        f"💵 *Your Collection:* ₹{total_counted_amount:,.2f}\n"
        f"💵 *Your Refund:* {formatted_refund_amount}\n"
        f"📉 *Your Counted Amount (After Refund):* ₹{counted_after_refund:,.2f}\n"
        f"🎯 *Incentive (After Refund):* ₹{incentive_after_refund:,.2f}\n\n"
        f"Keep pushing, {salesperson_name}, you are just a few steps away from qualifying!\n\n"
         f"Best Regards,\n*QRT Team*"
    )

    # Send the WhatsApp message using UltraMsg API
    send_whatsapp_message(config['whatsapp']['ultramsg_token'], contact_number, message)

# Concatenate all individual results into a single DataFrame
final_result = pd.concat(all_results, ignore_index=True)

# Save the final result to a new CSV file with all detailed calculations
final_result.to_csv('qaulified_salespersons_incentives.csv', index=False)
print("\nDetailed incentive data per salesperson saved in qaulified_salespersons_incentives.csv")
