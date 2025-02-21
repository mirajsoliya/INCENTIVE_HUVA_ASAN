import pandas as pd

def split_report_by_role(input_csv):
    # Load the original CSV file
    df = pd.read_csv(input_csv)
    
    # Get unique roles in the 'Role' column
    roles = df['Role'].unique()
    
    # Loop through each unique role and create a separate CSV for each role
    for role in roles:
        # Filter the data by role
        role_df = df[df['Role'] == role]
        
        # Create a new filename for the role
        filename = f'{role}_Final_incentive_report.csv'
        
        # Save the filtered data to a new CSV file
        role_df.to_csv(filename, index=False)
        
        print(f"Saved incentive report for {role} to {filename}")

# Example of how to use the function
def split_role():
    split_report_by_role('incentive_report.csv')


# if __name__ == "__main__":
#     split_role()