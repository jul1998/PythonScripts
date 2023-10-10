from shareplum import Site
from shareplum.site import Version
from requests_ntlm import HttpNtlmAuth
import getpass
import pandas as pd
import io
import os
from dotenv import load_dotenv

load_dotenv() # Load environment variables

FOLDER = os.getenv("FOLDER") # Get environment variables


# username and password
def get_username():
    username = getpass.getuser()
    password = None  # password is not needed as Windows credentials will be used
    return username, password

def get_sharepoint_folder(username, password, site_url):
    # Specify the document library or folder and file name
    auth = HttpNtlmAuth(username, password)
    site = Site(site_url, version=Version.v2016, auth=auth, verify_ssl=False)
    folder = site.Folder(FOLDER)  # Replace with the actual library or folder name
    return folder



def get_sharepoint_files(folder, file_path):
    files = folder.files
    df_store = []
    for file in files:
        file_name = file['Name']
        if file_name.endswith('.xlsx'):
            file_contents = folder.get_file(file_name)
            print(f"Reading {file_name}...")

            # Read the Excel file into a DataFrame
            df = pd.read_excel(io.BytesIO(file_contents))

            # csv file path
            csv_file_path = os.path.join(file_path, file_name.split('.', 1)[0] + '.csv')

            # Convert the DataFrame to csv
            df.to_csv(csv_file_path, index=False)

            print(f"{file_name} stored in list")
            df_store.append(df)


        else:
            print('Not an excel file', file_name)
            continue
    return df_store

