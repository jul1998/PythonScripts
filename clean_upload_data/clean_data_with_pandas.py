import pandas as pd
import json

def replace_single_quotes_with_double(json_str):
    """
    Replace single quotes with double quotes in a JSON string.
    :param json_str: JSON string
    :return: str JSON string
    """
    if isinstance(json_str, str):  # Check if the input is a string
        return json_str.replace("'", "\"")  # Replace single quotes with double quotes
    return json_str


def replace_quotes_in_columns(df, columns_to_replace):
    """
    Replace single quotes with double quotes in specified columns of a DataFrame.
    :param df: DataFrame
    :param columns_to_replace: List of column names to process
    :return: DataFrame
    """
    try:
        for col in columns_to_replace:
            if col in df.columns:  # Check if the column exists in the DataFrame
                df[col] = df[col].apply(replace_single_quotes_with_double)
            else:
                print(f"Column '{col}' does not exist in the DataFrame. Skipping...")
                continue
        return df
    except KeyError:
        print("An error occurred while processing columns.")
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
        df_copy[f'{col}_dict'] = df_copy[col].apply(extract_id_value_pairs)  # Extract id-value pairs
        customfields_df = pd.DataFrame(df_copy[f'{col}_dict'].tolist(),
                                       index=df_copy.index)  # Convert dict to DataFrame
        df_copy = pd.concat([df_copy, customfields_df], axis=1)  # Concatenate the two DataFrames
        df_copy = remove_useless_columns(df_copy,
                                         f'{col}_dict')  # Remove the useless column, Comment this line if you want to keep the column

    return df_copy


def concat_df(*df):
    """
    Concatenate dataframes
    :param df: List of DataFrames
    :return: DataFrame
    """
    return pd.concat(df, axis=1)


def get_final_result(target_df, target_columns, final_file_name='final_df.xlsx'):
    """
    Get final result
    :param target_df: DataFrame
    :param remove_cols:     List of columns to remove
    :param final_file_name: Final file name
    :return: DataFrame
    """

    print("Getting final csv results...")

    existing_cols = []
    df_columns = target_df.columns.tolist()  # Get column names
    for target in target_columns:
        if target not in df_columns:
            print(f"Column '{target}' does not exist in the DataFrame. Skipping...")
            continue
        existing_cols.append(target)

    final_df = insert_json_cols(target_df, existing_cols)  # Insert json columns

    final_df.to_csv(final_file_name, index=False) # Save to csv file in documents folder


def check_col_in_df(df, col):
    """
    Check if a column exists in a DataFrame
    :param df: DataFrame
    :param col: Column name
    :return: Boolean
    """
    if col in df.columns:
        print(f"Column '{col}' exists in the DataFrame.")
        return True
    else:
        print(f"Column '{col}' does not exist in the DataFrame.")
        return False

def check_cell_in_col(df, value_to_check , col_to_check):
    """
    Check if a cell exists in a DataFrame
    :param df: DataFrame
    :param cell_value: Cell value
    :return: Boolean
    """
    result = df[col_to_check].isin([value_to_check])
    if result.any():
        print(f"Cell '{value_to_check}' exists in the DataFrame.")
        return True
    else:
        print(f"Cell '{value_to_check}' does not exist in the DataFrame.")
        return False

def filter_df_by_value(df, col, value):
    """
    Filter DataFrame by value
    :param df: DataFrame
    :param col: Column name
    :param value: Value to filter
    :return: DataFrame
    """

    filtered_df = df[df[col] == value]
    return filtered_df
