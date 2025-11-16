import sys
import os
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from functions.csv_to_pandas import import_csv_to_df


def test_report_missing_winner_race_ids():
    # Load CSVs without setting an index so raceId is a column
    df_races = import_csv_to_df('source-data', 'races.csv', index_col=None)
    df_results = import_csv_to_df('source-data', 'results.csv', index_col=None)
    df_results['position'] = df_results['position'].fillna(-1).astype('int64')  # replace missing position as -1 so returns int


    # Ensure numeric comparison for positions and raceId columns
    race_ids = set(pd.to_numeric(df_races['raceId'], errors='coerce').dropna().astype(int).unique())
    results_ids = set(pd.to_numeric(df_results.loc[df_results['position'] == 1, 'raceId'], errors='coerce').dropna().astype(int).unique())

    # Compute missing raceIds that have no position=1 winner
    missing = sorted(list(race_ids - results_ids))

    assert not missing, f"The following raceId(s) have no position=1 in results.csv: {missing}"
