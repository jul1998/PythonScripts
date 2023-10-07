import pandas as pd
import json



def extract_id_value_pairs(json_str):
    """
    Extract id-value pairs from json string
    :param json_str:
    :return:  dict
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
    :param df:
    :param cols_to_remove:
    :return:  DataFrame
    """
    df_copy = df.copy()
    df_copy.drop(columns=list(cols_to_remove), inplace=True)
    return df_copy

def insert_json_cols(df, target_columns:str):
    """
    Insert json columns
    :param df:
    :param target_columns:
    :return:
    """
    df_copy = df.copy()
    df_copy['customfields_dict'] = df_copy[target_columns].apply(extract_id_value_pairs)
    customfields_df = pd.DataFrame(df_copy['customfields_dict'].tolist(), index=df_copy.index)

    result_df = pd.concat([df_copy, customfields_df], axis=1)
    return result_df


def concat_df(*df):
    """
    Concatenate dataframes
    :param df:
    :return: DataFrame
    """
    return pd.concat(df, axis=1)

def get_final_result(target_df):

    final_df = insert_json_cols(target_df, 'customfields_number_1')

    # Remove json columns
    final_df = remove_useless_columns(final_df, 'customfields_number_1', 'customfields_dict')

    final_df.to_excel('final_df.xlsx', index=False)

if __name__ == '__main__':
    df = pd.read_excel('base_final_json.xlsx')
    get_final_result(df)
    print('Done')