import os
import shutil

# Define the folder name to remove
folder_name = "CSV"

# Get the current working directory
current_directory = os.getcwd()

# Full path to the folder
folder_path = os.path.join(current_directory, folder_name)

# Remove the "CSV" folder if it exists
if os.path.exists(folder_path) and os.path.isdir(folder_path):
    try:
        shutil.rmtree(folder_path)
        print(f"Folder '{folder_name}' has been removed successfully.")
    except Exception as e:
        print(f"Error occurred while removing the folder '{folder_name}': {e}")
else:
    print(f"Folder '{folder_name}' does not exist in the current directory.")

# Remove all Excel files in the current directory
try:
    excel_extensions = ('.xls', '.xlsx', '.xlsm','.csv','.csv_cleaned.csv')
    excel_files = [file for file in os.listdir(current_directory) if file.endswith(excel_extensions)]

    if excel_files:
        for excel_file in excel_files:
            os.remove(os.path.join(current_directory, excel_file))
            print(f"Removed Excel file: {excel_file}")
    else:
        print("No Excel files found in the current directory.")
except Exception as e:
    print(f"Error occurred while removing Excel files: {e}")
