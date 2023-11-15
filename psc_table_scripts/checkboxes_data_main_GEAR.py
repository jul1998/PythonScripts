import io
import os
from dotenv import load_dotenv
import pandas as pd
from clean_upload_data import replace_quotes_in_columns, upload_to_s3, \
    check_col_in_df, check_cell_in_col, get_value_checked_if_true, pivot_json_values, concat_df, remove_useless_columns, \
    replace_commas_with_semicolon
from get_sharepoint_files import get_username, get_sharepoint_folder

load_dotenv() # Load environment variables


# final_file_name = 'df_other_sim.csv'  # Specify final file name as a csv file to upload to s3
# target_file = 'json_other_sim.xlsx'

# SIM required data
assign_folder_name_col = 'assignedfolder'
assign_folder_id_value = "60f08ec3-1653-4e47-8af9-a056efe920ac"

# S3 required data
bucket = 'quicksigth-dashboards'

target_columns = ['impacted_region', 'region_', 'country_', 'support_needed_from_pars_spx_team', 'related_pars_program_', 'impacted_country']  # Specify target columns inside the excel file

# Sharepoint required data
file_path = os.getenv('FILE_PATH')  # Local path where files were stored
site_url = "https://share.amazon.com/sites/ComplianceGlobalOperationsSP/"
username, password = get_username()
folder = get_sharepoint_folder(username, password, site_url)

files = folder.files


def run_checkboxes_data_GEAR():
    for file in files:
        file_name = file['Name']
        if file_name.endswith('.xlsx') and file_name == "ps_sim_issues_folders_gear_checkboxes_psc.xlsx":
            file_contents = folder.get_file(file_name)
            print(f"Reading {file_name}...")

            # Read the Excel file into a DataFrame
            df = pd.read_excel(io.BytesIO(file_contents))


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

            # Get checked values from json columns
            value_checked_df = get_value_checked_if_true(df, target_columns)


            df = concat_df(df, value_checked_df)


            # Replace commas by semicolon
            df = replace_commas_with_semicolon(df)

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
