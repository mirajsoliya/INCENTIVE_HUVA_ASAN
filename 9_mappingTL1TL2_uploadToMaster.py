import pandas as pd

# Load the CSV files
incentive_df = pd.read_csv('incentive_dump_Final.csv')
team_update_df = pd.read_csv('team_update_report.csv')

# Clean column names by stripping any extra spaces
incentive_df.columns = incentive_df.columns.str.strip()
team_update_df.columns = team_update_df.columns.str.strip()

# Remove duplicates in team_update_df based on 'sale_order'
team_update_df = team_update_df.drop_duplicates(subset=['sale_order'])

# Merge the DataFrames on the 'sale_order' column
merged_df = pd.merge(
    incentive_df,
    team_update_df[['sale_order', 'TL1', 'BM1', 'TL2', 'BM2']],
    on='sale_order',
    how='left',
    suffixes=('_incentive', '_team')
)

# Debugging Step: Check column names in merged_df
print("\nColumns in merged_df after merge:")
print(merged_df.columns)

# Ensure salesperson1_name and salesperson2_name from incentive_dump_Final.csv are retained
merged_df['salesperson1_name'] = merged_df['salesperson1_name']
merged_df['salesperson2_name'] = merged_df['salesperson2_name']
merged_df['presalesperson_name'] = merged_df['presalesperson_name']

# Define the required columns (from both incentive_dump_Final.csv and team_update_report.csv)
final_columns = [
    'Unnamed: 0', 'so_id', 'sale_order', 'sale_order_untax', 'sale_order_amount_tax',
    'sale_order_amount_total', 'sale_order_margin', 'sale_order_date', 'p_id', 'name', 'state',
    'payment_reference', 'payment_state', 'invoice_partner_display_name', 'invoice_origin',
    'invoice_date', 'date', 'payment_untax', 'payment_amount_tax', 'payment_total', 'create_date',
    'einvoice_status', 'salesperson1', 'salesperson2','salesperson3', 
    'salesperson1_name', 'salesperson2_name','salesperson3_name' ,'presalesperson_name',
    'cost', 'final_amount', 'source', 'TL1', 'BM1', 'TL2', 'BM2'
]

# Ensure the columns exist in the merged DataFrame
final_columns_in_df = [col for col in final_columns if col in merged_df.columns]

# Select only the desired columns
final_df = merged_df[final_columns_in_df]

# Save the final DataFrame to a new CSV file
final_df.to_csv('Updated_incentive_dump_Final.csv', index=False)

# Validate the row counts to ensure consistency
print(f"\nRow count of incentive_dump_Final.csv: {len(incentive_df)}")
print(f"Row count of Updated_incentive_dump_Final.csv: {len(final_df)}")

print("Final CSV saved as 'Updated_incentive_dump_Final.csv'")
