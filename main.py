import pandas as pd
import os
from get_sharepoint_files import get_username, get_sharepoint_folder, get_sharepoint_files
from clean_upload_data import get_final_result, replace_quotes_in_columns, upload_to_s3

import io

if __name__ == '__main__':
    # final_file_name = 'df_other_sim.csv'  # Specify final file name as a csv file to upload to s3
    # target_file = 'json_other_sim.xlsx'

    # S3 required data
    bucket = 'quicksigth-dashboards'
    target_columns = ['customfields_number', 'customfields_date']  # Specify target columns inside the excel file

    # Sharepoint required data
    file_path = "C:/Users/julsola/Documents/Py Tests/"  # Local path where files were stored
    site_url = "https://share.amazon.com/sites/ComplianceGlobalOperationsSP/"
    username, password = get_username()
    folder = get_sharepoint_folder(username, password, site_url)

    files = folder.files

    for file in files:
        file_name = file['Name']
        if file_name.endswith('.xlsx'):
            file_contents = folder.get_file(file_name)
            print(f"Reading {file_name}...")

            # Read the Excel file into a DataFrame
            df = pd.read_excel(io.BytesIO(file_contents))

            # csv file path
            csv_file_path = os.path.join(file_path, file_name.split('.', 1)[0] + '.csv')

            # clean data
            df = replace_quotes_in_columns(df, target_columns) # Replace single quotes with double quotes
            get_final_result(df, target_columns=target_columns, final_file_name=csv_file_path) # Get final result

            # upload to s3
            upload_to_s3(bucket, csv_file_path, 'sim_issues/' + file_name.split('.', 1)[0] + '.csv')


        else:
            print('Not an excel file', file_name)
            continue

    print('Done!')





