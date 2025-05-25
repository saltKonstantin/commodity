# IMF Commodity Data Fetcher

This project contains a Python script (`main.py`) designed to fetch, process, and store commodity price data from the International Monetary Fund (IMF) API.

## Features

*   **Data Retrieval**: Fetches data from the IMF\'s Primary Commodity Price System (PCPS) dataset.
*   **Data Processing**: Parses JSON responses from the API and processes the data using the pandas library.
*   **Database Storage**: Stores the processed commodity data in an SQLite database (`imf_commodities.sqlite`). This includes historical observations for various commodities.
*   **Metadata Management**: 
    *   Creates and updates a table (`indicators_metadata`) in the database to keep track of available commodity indicators, their descriptions, and their activity status based on the latest API data structure.
    *   Generates a text file (`commodity_indicators_list.txt`) listing all currently active commodity indicators and their descriptions as reported by the IMF API.
*   **Logging**: Maintains a detailed log of operations, API requests, and any errors encountered in `imf_data_fetch.log`.

## How it Works

1.  **Initialization**:
    *   Sets up logging to both the console and the `imf_data_fetch.log` file.
    *   Initializes the SQLite database (`imf_commodities.sqlite`), creating tables (`commodity_prices`, `indicators_metadata`) if they don\'t already exist.

2.  **Fetch Data Structure**:
    *   Makes an API call to fetch the data structure for the PCPS dataset. This provides information on available frequencies, reference areas, indicators (commodities), and units of measure.
    *   Parses this structure to identify currently active commodity indicators.

3.  **Update Indicator Metadata**:
    *   Updates the `indicators_metadata` table in the database:
        *   Marks all previously known indicators as inactive.
        *   For each indicator currently reported by the API:
            *   If the indicator is new, it\'s added to the table with its description and marked as active.
            *   If the indicator already exists, its description is updated (if changed), and it\'s marked as active.
    *   Writes the list of currently active indicators and their descriptions to `commodity_indicators_list.txt`.

4.  **Fetch Commodity Data**:
    *   Constructs API request keys for each active commodity indicator, targeting monthly (\'M\') and annual (\'A\') frequencies, the global/world reference area (\'W00\'), and appropriate units (USD for prices, IX for indices).
    *   Iterates through these series configurations:
        *   Makes an API call to fetch the time series data for each specific commodity, frequency, and unit.
        *   Introduces a 1-second delay between API calls to respect API rate limits.

5.  **Process and Store Data**:
    *   For each successfully fetched series:
        *   Processes the JSON data into a pandas DataFrame.
        *   Transforms the observation dates and values into a structured format.
        *   Saves the data to the `commodity_prices` table in the SQLite database. An `ON CONFLICT` clause is used to update existing records if the observation value has changed, ensuring data integrity and avoiding duplicates.

6.  **Logging and Summary**:
    *   Logs all significant actions, API calls, successes, and errors.
    *   Provides a session summary indicating the total number of new or updated observation records written to the database.

## Files

*   `main.py`: The main Python script.
*   `imf_commodities.sqlite`: The SQLite database file where commodity data and metadata are stored.
*   `imf_data_fetch.log`: Log file for script operations.
*   `commodity_indicators_list.txt`: A text file listing currently active commodity indicators from the IMF PCPS dataset.
*   `README.md`: This file.

## Prerequisites

*   Python 3.x
*   The following Python libraries (which can be installed via pip):
    *   `requests`
    *   `pandas`

## Setup and Usage

1.  **Install Dependencies**:
    ```bash
    pip install requests pandas
    ```
2.  **Run the Script**:
    ```bash
    python main.py
    ```
    The script will then:
    *   Connect to/create the `imf_commodities.sqlite` database.
    *   Fetch the latest data structure from the IMF API.
    *   Update `commodity_indicators_list.txt` and the `indicators_metadata` table.
    *   Proceed to fetch and store commodity price data.
    *   Log its progress to the console and `imf_data_fetch.log`.

## Database Schema

### `commodity_prices` Table

| Column                  | Type      | Description                                                                 |
| ----------------------- | --------- | --------------------------------------------------------------------------- |
| `id`                    | INTEGER   | Primary Key, Autoincrement                                                  |
| `frequency_code`        | TEXT      | Frequency of the observation (e.g., M, A)                                   |
| `area_code`             | TEXT      | Reference area code (e.g., W00 for World)                                   |
| `indicator_code`        | TEXT      | Commodity indicator code (e.g., PCOALAU for Coal, Australia)                |
| `unit_code`             | TEXT      | Unit of measure (e.g., USD, IX)                                             |
| `observation_date`      | TEXT      | Date of the observation (YYYY-MM-DD, YYYY-MM, or YYYY)                      |
| `observation_value`     | REAL      | Value of the observation                                                    |
| `data_series_key`       | TEXT      | Composite key for the data series (e.g., M.W00.PGOLD.USD)                   |
| `last_fetched_script`   | TIMESTAMP | Timestamp of when the record was last fetched/updated by the script         |
| *Unique Constraint*     |           | `(data_series_key, observation_date)`                                       |

### `indicators_metadata` Table

| Column                        | Type      | Description                                                              |
| ----------------------------- | --------- | ------------------------------------------------------------------------ |
| `indicator_code`              | TEXT      | Primary Key, Commodity indicator code                                    |
| `description`                 | TEXT      | Description of the commodity indicator                                   |
| `first_seen_script_run`       | TIMESTAMP | Timestamp of when the indicator was first recorded by the script         |
| `last_seen_active_script_run` | TIMESTAMP | Timestamp of the last script run where this indicator was seen as active |
| `is_currently_active`         | INTEGER   | Boolean (0 or 1) indicating if the indicator is currently active         |

## Data Analysis Scripts

Beyond fetching data, this project also includes scripts for analyzing the collected commodity data.

### `full_correlation_matrix.py`

*   **Purpose**: Calculates and ranks the Pearson correlation coefficients for every possible pair of commodity indicators found in `commodity_indicators_list.txt` based on their monthly data from `imf_commodities.sqlite`. The analysis can be performed over a user-defined number of months or the maximum available common history for each pair.
*   **How it Works**:
    1.  Reads indicator codes and their descriptions from `commodity_indicators_list.txt`.
    2.  Prompts the user to enter the number of months for the analysis window (e.g., 12, 24, 60) or to type 'max' to use the full available common history for each pair.
    3.  Fetches all available monthly data for every indicator from the `imf_commodities.sqlite` database and caches it.
    4.  Generates all unique pairs of indicators.
    5.  For each pair:
        *   If a specific number of months is chosen: It identifies the latest common data point for the pair and defines an analysis window by looking back the specified number of months.
        *   If 'max' is chosen: It uses the entire overlapping historical data for the pair.
        *   Filters the data for both indicators to the determined window/history.
        *   Merges the data and calculates the Pearson correlation if there are enough common data points (at least 12 months by default).
    6.  Collects all valid correlation results.
    7.  Sorts the results by correlation value in descending order.
    8.  Saves the ranked results to a CSV file named `full_correlation_results.csv`.
    9.  Prints a confirmation message to the console, including the path to the output CSV.
*   **Prerequisites**:
    *   `imf_commodities.sqlite` database file (generated by `main.py`) must be present in the same directory.
    *   `commodity_indicators_list.txt` file (generated by `main.py`) must be present.
    *   Python libraries: `pandas`.
*   **Usage**:
    ```bash
    python full_correlation_matrix.py
    ```
    The script will then prompt for the analysis timeframe (number of months or 'max').
*   **Output**: 
    *   A CSV file named `full_correlation_results.csv` containing the ranked correlations. Columns include indicator codes, their (cleaned) descriptions, the correlation coefficient, the number of common months in the window, and the start/end dates of the analysis window used for that pair.
    *   A confirmation message printed to the console.