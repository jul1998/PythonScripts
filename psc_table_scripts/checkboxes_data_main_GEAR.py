import pandas as pd
import os
from get_sharepoint_files import get_username, get_sharepoint_folder
from clean_upload_data import get_final_result, replace_quotes_in_columns, upload_to_s3, \
    check_col_in_df, check_cell_in_col, modify_id, pivot_json_values, concat_df, remove_useless_columns

import io

# final_file_name = 'df_other_sim.csv'  # Specify final file name as a csv file to upload to s3
# target_file = 'json_other_sim.xlsx'

# SIM required data
assign_folder_name_col = 'assignedfolder'
assign_folder_id_value = "60f08ec3-1653-4e47-8af9-a056efe920ac"

# S3 required data
bucket = 'quicksigth-dashboards'

target_columns = ['value']  # Specify target columns inside the excel file

# Sharepoint required data
file_path = "C:/Users/julsola/Documents/Py Tests/"  # Local path where files were stored
site_url = "https://share.amazon.com/sites/ComplianceGlobalOperationsSP/"
username, password = get_username()
folder = get_sharepoint_folder(username, password, site_url)

files = folder.files


def run_checkboxes_data_GEAR():
    for file in files:
        file_name = file['Name']
        if file_name.endswith('.xlsx') and file_name == "GEAR Checkboxes.xlsx":
            file_contents = folder.get_file(file_name)
            print(f"Reading {file_name}...")

            # Read the Excel file into a DataFrame
            df = pd.read_excel(io.BytesIO(file_contents))
            print(df['value'][0])

            # Check if assign_folder_id exists in df
            if not check_col_in_df(df, assign_folder_name_col):
                continue

            # Check if assign_folder_id value exists in df
            if not check_cell_in_col(df, assign_folder_id_value, assign_folder_name_col):
                continue

            # Filter df by assign_folder_id
            # df = filter_df_by_value(df, assign_folder_name_col, assign_folder_id_value)

            # csv file path
            csv_file_path = os.path.join(file_path, file_name.split('.', 1)[0] + '.csv')

            # clean data
            df = replace_quotes_in_columns(df, target_columns)  # Replace single quotes with double quotes

            # process and pivot
            df = modify_id(df)

            values_check_df = pivot_json_values(df, "replaced_values")

            df = concat_df(df, values_check_df)
            df = remove_useless_columns(df, 'replaced_values')

            # export to csv
            df.to_csv(csv_file_path, index=False)



            # get_final_result(df, target_columns=target_columns, final_file_name=csv_file_path,
            #                  json_function=extract_id_value_checked_pairs)  # Get final result. Specify json_function
            # to use
            # extract_id_value_pairs function
            # to extract id-value pairs from json columns
            # extract_id_value_checked_pairs function for
            # id-value-checked pairs

            # upload to s3
            upload_to_s3(bucket, csv_file_path, 'sim_issues/' + file_name.split('.', 1)[0] + '.csv')


        else:
            print(f'{file_name} is not an excel file or is not from checkboxes data', file_name)
            continue

    print('Done!')
