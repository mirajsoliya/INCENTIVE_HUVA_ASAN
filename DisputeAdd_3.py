import pandas as pd
import datetime

def process_csv():
    # Load the data from disputes.csv
    input_file = "disputes.csv"
    output_file = f"incentive_dump_Final.csv"  # New output file name with the timestamp

    try:
        # Read the incentive_dump.csv if it exists, else create an empty DataFrame
        try:
            existing_df = pd.read_csv("incentive_dump.csv")
            existing_df['source'] = 'Main'  # Add the source column for existing data
            print(f"Loaded {len(existing_df)} rows from incentive_dump.csv")
        except FileNotFoundError:
            existing_df = pd.DataFrame()  # If the file doesn't exist, start with an empty DataFrame
            print("incentive_dump.csv not found. Starting with an empty DataFrame.")
        
        # Read the disputes.csv into a DataFrame
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} rows from disputes.csv")

        # Check if 'payment_reference' column exists
        if 'payment_reference' not in df.columns:
            print("Error: 'payment_reference' column not found in the CSV file.")
            return

        # Filter rows where 'payment_reference' != "1"
        filtered_df = df[df['payment_reference'] != "1"]
        print(f"Filtered to {len(filtered_df)} rows where 'payment_reference' != '1'")

        # Add a source column to indicate this is from disputes
        filtered_df['source'] = 'disputes'

        # Append filtered disputes data to the existing incentive dump data
        combined_df = pd.concat([existing_df, filtered_df], ignore_index=True)
        print(f"Combined DataFrame has {len(combined_df)} rows after merging")

        # Clean the sale_order column by stripping any leading/trailing whitespace
        combined_df['sale_order'] = combined_df['sale_order'].str.strip()

        # Check the first few rows of the 'sale_order' column for any anomalies
        print("Sample values from 'sale_order' column:")
        print(combined_df['sale_order'].head())

        # Filter out rows where 'sale_order' does not start with 's'
        combined_df = combined_df[combined_df['sale_order'].str.startswith('S', na=False)]
        print(f"Filtered to {len(combined_df)} rows where 'sale_order' starts with 'S'")

        # Save the combined and filtered data to the new incentive dump file
        combined_df.to_csv(output_file, index=False, mode='w', header=True)
        print(f"Combined data saved to {output_file}")

    except FileNotFoundError:
        print(f"Error: File {input_file} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# if __name__ == "__main__":
#     process_csv()
