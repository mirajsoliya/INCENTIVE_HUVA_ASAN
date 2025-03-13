import pandas as pd

# Load the CSV file
df = pd.read_csv('../team_update_report.csv')

# Define the salesperson name to search
salesperson_name = "NILAY JANANI"

# Filter for rows where salesperson1_name or salesperson2_name matches the specified name
filtered_df = df[
    (df['salesperson1_name'] == salesperson_name) | 
    (df['salesperson2_name'] == salesperson_name)
]

# Select the required columns (fixed missing comma)
result_df = filtered_df[['sale_order', 'salesperson1_name', 'salesperson2_name', 
                         'presalesperson_name', 'cost', 'final_amount']]

# Apply presalesperson deduction if it exists
result_df['adjusted_amount'] = result_df.apply(
    lambda x: x['final_amount'] * 0.8 if pd.notna(x['presalesperson_name']) and x['presalesperson_name'] != "" else x['final_amount'], 
    axis=1
)

# Calculate 'counted_amount' based on presalesperson and salesperson conditions
def calculate_counted_amount(row):
    if pd.notna(row['presalesperson_name']) and row['presalesperson_name'] != "":
        return row['adjusted_amount'] if row['salesperson1_name'] == row['salesperson2_name'] else row['adjusted_amount'] * 0.5
    else:
        return row['adjusted_amount'] if row['salesperson1_name'] == row['salesperson2_name'] else row['adjusted_amount'] * 0.5  # Normal split

result_df['counted_amount'] = result_df.apply(calculate_counted_amount, axis=1)

# Calculate 'incentive' as 5% of 'counted_amount'
result_df['incentive'] = result_df['counted_amount'] * 0.05

# Calculate the total incentive for all rows
total_incentive = result_df['incentive'].sum()

# Display the result with the additional columns
print(result_df)

# Print the total incentive
print("\nTotal Incentive:", total_incentive)

# Save the result to a new CSV file
result_df.to_csv('filtered_sales_data_with_incentives.csv', index=False)
