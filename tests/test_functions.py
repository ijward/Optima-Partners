import sys
import os
os.chdir(os.path.dirname(os.getcwd()))

from functions.csv_to_pandas import import_csv_to_df
import pytest


def test_import_csv_to_df_no_index():
    # Importing a CSV file without index should keep 'raceId' as a column
    df_test = import_csv_to_df('source-data', 'races.csv', delimiter=',', index_col=None)
    assert not df_test.empty
    assert 'raceId' in df_test.columns
    assert 'name' in df_test.columns
    assert df_test.shape[0] > 0
    
def test_import_csv_to_df_no_delimiter():
    # Importing a CSV file without delimiter should use default ','
    df_test = import_csv_to_df('source-data', 'races.csv', index_col=None)
    assert not df_test.empty
    assert 'raceId' in df_test.columns
    assert 'name' in df_test.columns
    assert df_test.shape[0] > 0


def test_import_csv_to_df_with_index_in_races():
    # Importing a CSV file with index should set the index name
    df_test = import_csv_to_df('source-data', 'races.csv', delimiter=',', index_col='raceId')
    assert not df_test.empty
    assert df_test.index.name == 'raceId'
    assert 'name' in df_test.columns
    assert df_test.shape[0] > 0
    
def test_import_csv_to_df_with_index_in_results():
    # Importing a CSV file with index should set the index name
    df_test = import_csv_to_df('source-data', 'results.csv', delimiter=',', index_col='raceId')
    assert not df_test.empty
    assert df_test.index.name == 'raceId'
    assert 'driverId' in df_test.columns
    assert df_test.shape[0] > 0

"""
def test_import_csv_to_df_with_index_false_positive():
    # Test to force failure when index_col does not exist in dataframe
    df_test = import_csv_to_df('source-data', 'races.csv', delimiter=',', index_col='this_column_does_not_exist')
    assert not df_test.empty
    assert df_test.index.name == 'raceId'
    assert 'name' in df_test.columns
    assert df_test.shape[0] > 0 
"""
def test_csv_to_def_file_path_has_leading_trailing_slashes():
    # Test that leading/trailing slashes in file_path are handled correctly
    df_test = import_csv_to_df('/source-data/','races.csv')
    assert not df_test.empty
    df_test = import_csv_to_df('.source-data/','results.csv')
    assert not df_test.empty
    df_test = import_csv_to_df('./source-data','results.csv')
    assert not df_test.empty
    df_test = import_csv_to_df('source-data/','results.csv')
    
def test_import_csv_to_df_missing_file_raises():
    # Requesting a non-existent file should raise FileNotFoundError (propagated from pandas)
    with pytest.raises(Exception):
        import_csv_to_df('source-data', 'non_existent_file.csv', delimiter=',', index_col='raceId')


def test_import_csv_to_df_index_col_missing_raises():
    # If the requested index_col does not exist in the dataframe, pandas should raise a KeyError
    with pytest.raises(KeyError):
        import_csv_to_df('source-data', 'races.csv', delimiter=',', index_col='this_column_does_not_exist')


def test_each_race_has_position_one_winner():
    # For each raceId in df_races, there should be a raceId in df_results where position=1
    df_races = import_csv_to_df('source-data', 'races.csv', index_col=None)
    df_results = import_csv_to_df('source-data', 'results.csv', index_col=None)
    
    # Filter results to only position 1 (winners)
    df_winners = df_results[df_results['position'] == 1]
    
    # Get unique raceIds from both dataframes
    race_ids = set(df_races['raceId'].unique())
    winner_race_ids = set(df_winners['raceId'].unique())
    
    # Every raceId in df_races should have a corresponding position=1 in df_results
    assert race_ids.issubset(winner_race_ids), \
        f"Some races don't have a position=1 winner: {race_ids - winner_race_ids}"
