# Optima-Partners

This repository is for the Optima Partners engineering assignments assessment. Specifically, the repositary is for the data engineering assessment.
<https://github.com/optima-partners/engineering-recruitment-assignments/blob/main/data-engineering/datapipeline/>

## Project layout

- `source-data/` : Raw CSV inputs used by the analysis. Key files:
  - `races.csv` — high-level data for each race including date and time. Requirements are to combine date and time as one utc field, where null, time can be set as 00:00:00
  - `results.csv` — finishing position of each driver per race including fastest lap.
- `results/`    : Generated outputs (JSON files) by year, e.g. `stats_2018.json` through `stats_2024.json`.
- `solution/`   :
  - `main.py` — script to import the csvs and join based on raceId. Requirements specify fields to be exported and their names, creating one JSON file per year.

## Quick Start

### Prerequisites

- Python 3.13.9 or higher
- pandas library
- Git (to clone the repository)

### Installation and Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/ijward/Optima-Partners.git
   cd Optima-Partners
   ```

2. **Install dependencies**

   ```bash
   pip install pandas
   ```

3. **Run the data pipeline**

   From the project root directory, execute:

   ```bash
   python3 solution/main.py
   ```

   Or, if using Python 3 as default:

   ```bash
   python solution/main.py
   ```

### Expected Output

When successful, the script will:

- Read CSV files from `source-data/` directory:
  - `races.csv` — race metadata with dates and times
  - `results.csv` — driver finishing positions and lap data
- Join the datasets based on `raceId`
- Process the combined data according to specification
- Generate JSON output files in `results/` directory, organized by year:
  - `stats_2018.json` through `stats_2024.json`

Each output JSON file contains the transformed and filtered race statistics for that year.

### Troubleshooting

- **ModuleNotFoundError: No module named 'pandas'**: Install pandas using `pip install pandas`
- **FileNotFoundError**: Ensure you're running the script from the project root directory
- **Permission denied**: On macOS/Linux, you may need to use `python3` instead of `python`

## Testing

The project includes comprehensive unit tests and quality checks:

### Running Tests

Execute all tests with coverage reporting:

```bash
pytest tests/ --cov=functions --cov-report=html
```

### Test Coverage

- **test_functions.py**: Core functionality tests
  - `test_import_csv_to_df_no_index()` — validates CSV import without index
  - `test_import_csv_to_df_no_delimiter()` — validates default delimiter handling
  - `test_import_csv_to_df_with_index_in_races()` — validates index setting for races data
  - `test_import_csv_to_df_with_index_in_results()` — validates index setting for results data
  - `test_csv_to_def_file_path_has_leading_trailing_slashes()` — handles path normalization
  - `test_import_csv_to_df_missing_file_raises()` — validates error handling for missing files
  - `test_import_csv_to_df_index_col_missing_raises()` — validates error handling for invalid index columns
  - `test_each_race_has_position_one_winner()` — validates race-winner relationships

- **test_quality.py**: Data quality assurance
  - `test_report_missing_winner_race_ids()` — ensures every race has exactly one position=1 winner in the results

### Coverage Report

HTML coverage reports are generated in `htmlcov/` directory. Open `htmlcov/index.html` in a browser to view detailed coverage metrics.
