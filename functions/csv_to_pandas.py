import os
import pandas as pd

def import_csv_to_df(file_path: str, csv_name: str, delimiter: str = ',', index_col: str = 'raceId') -> pd.DataFrame:
    """ Use pandas to import a CSV to a dataframe and set index if required. Current working directory is set to the project root for the user.
    Args:
        file_path (str): relative path to CSV file i.e 'source-data'. 
        delimiter (str, optional): specific delimiter to use. Defaults to ','.
        csv_name (str): name of the CSV file including file extension. If the file is not found, an exception is raised.
        index_col (str, optional): name of the column to set as index. Defaults to 'raceId'.

    Returns:
        pd.DataFrame: DataFrame containing the imported CSV data
    
    Example usage:
        import_csv_to_df(file_path='source-data', csv_name='races.csv', delimiter=',', index_col='raceId')
        import_csv_to_df(file_path='/source-data/', csv_name='races.csv', delimiter=',', index_col='raceId')
        import_csv_to_df(file_path='source-data', csv_name='races.csv')
        import_csv_to_df(file_path='source-data', csv_name='races.csv', delimiter=',')
        import_csv_to_df(file_path='source-data', csv_name='races.csv', index_col='raceId')
    """
       
    """ Ensure the file path ends with a slash """
    if not file_path.endswith('/'):
        file_path += '/'
    
    """ Ensure the file path starts with a slash and ends with a slash"""  
    if file_path.startswith('/'):
        file_path = os.getcwd() + file_path
    elif file_path.startswith('./'):
        file_path = os.getcwd() + file_path[1:]
    elif file_path.startswith('.'):
        file_path = os.getcwd() + '/' + file_path[1:]
    else: # relative path
        file_path = os.getcwd() + '/' + file_path

    df = pd.read_csv(file_path+csv_name, delimiter=delimiter)
    if index_col:
        df.set_index(keys=index_col, inplace=True)
    return df