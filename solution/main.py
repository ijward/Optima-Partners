import pandas as pd
import os

# set cwd to repository root.
project_root = os.path.dirname(os.path.abspath(__name__))
os.chdir(project_root)
print(f'Working directory set to: {project_root}')
from solution.functions import import_csv_to_df



print('IMPORTING THE CSVs TO PANDAS DATAFRAMES')
df_races = import_csv_to_df('source-data', 'races.csv')
df_results = import_csv_to_df('source-data', 'results.csv')

print('APPLYING SPECIFIC REQUIREMENTS TO df_races')
df_races['time'] = df_races['time'].fillna('00:00:00')  # If time is null, use 00:00:00
df_races['calc_datetime'] = (
    pd.to_datetime(df_races['date']
                   + ' '
                   + df_races['time'], format='%Y-%m-%d %H:%M:%S'))  # Calculate datetime field
df_races = df_races.drop(columns=(['date', 'time']))

print('APPLYING SPECIFIC REQUIREMENTS TO df_results')
df_results['position'] = df_results['position'].fillna(-1).astype('int64')  # replace missing position as -1 so returns int
df_results = df_results[df_results['position'].astype(int) == 1]

# JOIN df_races and df_results using index
df = pd.merge(
    df_races,
    df_results,
    left_index=True, right_index=True, how='inner'
).sort_values(by=['year', 'round', 'position'])

# RENAME COLUMNS IF THEY EXIST
df = df.rename(columns={
    'round': 'Race Round'
    , 'name': 'Race Name'
    , 'datetime': 'Race DateTime'
    , 'driverId': 'Race Winning driverId'
    , 'fastestLapTime': 'Race Fastest Lap'
    , 'calc_datetime': 'Race Datetime'
}, errors='ignore')

df_layout = column_names = [
    'year',
    'Race Name',
    'Race Round',
    'Race Datetime',
    'Race Winning driverId',
    'Race Fastest Lap'
]
df = df.reindex(columns=df_layout)
print(df.head())

# CLEAN UP RESOURCES
del df_races, df_results, column_names, df_layout

yrs = df['year'].unique()
for yr in yrs:
    f_name=f'stats_{yr}.json'
    print(f'Generating json file {f_name}')
    df.loc[df['year'] == yr, df.columns != 'year'].to_json(f'{project_root}/results/{f_name}'
                                                           , orient='records'
                                                           , date_format='iso'
                                                           , index=False
                                                           , indent=4)
del df
