import pandas as pd
import json


def replace_single_quotes_with_double(json_str):
    """
    Replace single quotes with double quotes in a JSON string.
    :param json_str: JSON string
    :return: str JSON string
    """
    if isinstance(json_str, str): # Check if the input is a string
        return json_str.replace("'", "\"") # Replace single quotes with double quotes
    return json_str

def replace_quotes_in_columns(df, columns_to_replace):
    """
    Replace single quotes with double quotes in specified columns of a DataFrame.
    :param df: DataFrame
    :param columns_to_replace: List of column names to process
    :return: DataFrame
    """
    for col in columns_to_replace: # Iterate over columns
        df[col] = df[col].apply(replace_single_quotes_with_double) # Apply the function to each column
    return df

def extract_id_value_pairs(json_str):
    """
    Extract id-value pairs from json string
    :param json_str: JSON string
    :return:  dict of id-value pairs
    """
    try:
        data = json.loads(json_str)

        id_value_dict = {item['id']: item['value'] for item in data}
        # print(id_value_dict)
        return id_value_dict

    except (json.JSONDecodeError, TypeError):
        return {}

def remove_useless_columns(df, *cols_to_remove):
    """
    Remove useless columns
    :param df: DataFrame
    :param cols_to_remove: List of column names to remove
    :return:  DataFrame
    """
    df_copy = df.copy()
    df_copy.drop(columns=list(cols_to_remove), inplace=True)
    return df_copy


def insert_json_cols(df, target_columns):
    """
    Insert JSON columns for multiple target columns.
    :param df: DataFrame
    :param target_columns: List of target column names
    :return: DataFrame
    """
    df_copy = df.copy()

    for col in target_columns:
        df_copy[f'{col}_dict'] = df_copy[col].apply(extract_id_value_pairs) # Extract id-value pairs
        customfields_df = pd.DataFrame(df_copy[f'{col}_dict'].tolist(), index=df_copy.index) # Convert dict to DataFrame
        df_copy = pd.concat([df_copy, customfields_df], axis=1) # Concatenate the two DataFrames
        df_copy = remove_useless_columns(df_copy, f'{col}_dict') # Remove the useless column, Comment this line if you want to keep the column


    return df_copy


def concat_df(*df):
    """
    Concatenate dataframes
    :param df:
    :return: DataFrame
    """
    return pd.concat(df, axis=1)

def get_final_result(target_df, target_columns , final_file_name='final_df.xlsx' ):
    """
    Get final result
    :param target_df:
    :param remove_cols:
    :param final_file_name:
    :return:
    """

    final_df = insert_json_cols(target_df, target_columns) # Insert json columns

    final_df.to_excel(final_file_name, index=False)



if __name__ == '__main__':
    final_file_name = 'df_base_final.xlsx'
    target_file = 'base_final_json.xlsx'

    target_columns = ['customfields_number'] # Specify target columns

    df = pd.read_excel(target_file)  # Read the excel file
    df = replace_quotes_in_columns(df, target_columns) # Replace single quotes with double quotes

    get_final_result(df, target_columns=target_columns, final_file_name=final_file_name) # Get final result

    print('Done')



