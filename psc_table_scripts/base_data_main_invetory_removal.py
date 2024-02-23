import pandas as pd
import os
from get_sharepoint_files import get_username, get_sharepoint_folder
from clean_upload_data import get_final_result, replace_quotes_in_columns, upload_to_s3, \
    check_col_in_df, check_cell_in_col, filter_df_by_value, extract_id_value_pairs

import io
from dotenv import load_dotenv

load_dotenv() # Load environment variables
# final_file_name = 'df_other_sim.csv'  # Specify final file name as a csv file to upload to s3
# target_file = 'json_other_sim.xlsx'

# SIM required data
assign_folder_name_col = 'assignedfolder'
assign_folder_id_value = [
    "a6a3374a-76ae-4df4-9607-00c59185a127",  # Parent folder id
    "c5d81cf4-9a32-497b-b090-4fe82fc672eb",  # Executive Inventory Removal Requests
    "b850ac6c-a367-4733-8f31-698f59b215ac",  # Non-Executive Escalations
    "a1c453fc-bf1a-42f5-84b6-82fe0932d8c8",  # Program Action SIMs
    "58841026-814c-4be8-a487-c823bb4133d4",  # Removal Order Cancellation
    "10265259-86e9-476a-9a5e-9ca05c4f349c",  # Removal Stoppage
    "fdb53825-486b-440f-a132-0cfe920984f4" # FC OUTREACH
]
# S3 required data
bucket = 'quicksigth-dashboards'
target_columns = ['customfields_number', 'customfields_date']  # Specify target columns inside the excel file

# Sharepoint required data
file_path = os.getenv('FILE_PATH')  # Local path where files were stored
site_url = "https://share.amazon.com/sites/ComplianceGlobalOperationsSP/"
username, password = get_username()
folder = get_sharepoint_folder(username, password, site_url)

files = folder.files


def run_base_data_invetory_removal():
    for file in files:
        file_name = file['Name']
        if file_name.endswith('.xlsx') and file_name == "ps_sim_issues_folders_base_data_psc.xlsx":
            file_contents = folder.get_file(file_name)
            print(f"Reading {file_name}...")

            # Read the Excel file into a DataFrame
            df = pd.read_excel(io.BytesIO(file_contents))

            # Check if assign_folder_id exists in df
            if not check_col_in_df(df, assign_folder_name_col):
                continue

            # Loop over each value in the list and check if it exists in the DataFrame
            for value in assign_folder_id_value:
                if not check_cell_in_col(df, value, assign_folder_name_col):
                    continue

            # Filter df by assign_folder_id
            df = filter_df_by_value(df, assign_folder_name_col, assign_folder_id_value)

            # csv file path
            csv_file_path = os.path.join(file_path, file_name.split('.', 1)[0] + '.csv')

            # clean data
            df = replace_quotes_in_columns(df, target_columns)  # Replace single quotes with double quotes

            get_final_result(df, target_columns=target_columns, final_file_name=csv_file_path,
                             json_function=extract_id_value_pairs)  # Get final result. Specify json_function to use
            # extract_id_value_pairs function
            # to extract id-value pairs from json columns
            # extract_id_value_checked_pairs function
            # to id-value-checked pairs

            # upload to s3
            upload_to_s3(bucket, csv_file_path, 'sim_issues/' + file_name.split('.', 1)[0] + '.csv')


        else:
            print(f'{file_name} is not an excel file or is not from base data', file_name)
            continue

    print('Done!')
