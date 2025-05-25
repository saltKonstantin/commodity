import requests
import pandas as pd
import json
import sqlite3 # Added for database operations
import time    # Added for delays between API calls
import logging # Added for logging
import os      # Added for path joining

# Base URL for the IMF API
BASE_URL = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'
DB_FILE = os.path.join('database', 'imf_commodities.sqlite')
LOG_FILE = os.path.join('database', 'imf_data_fetch.log')
INDICATORS_LIST_FILE = os.path.join('database', 'commodity_indicators_list.txt') # File for the list of indicators

# --- Logger Setup (done globally or in main) ---
# Logger will be configured in __main__

# --- Database Operations ---
def init_db(db_path, logger):
    """Initializes the database and creates tables if they don't exist."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS commodity_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            frequency_code TEXT NOT NULL,
            area_code TEXT NOT NULL,
            indicator_code TEXT NOT NULL,
            unit_code TEXT NOT NULL,
            observation_date TEXT NOT NULL, -- Store as TEXT in YYYY-MM-DD or YYYY-MM format
            observation_value REAL,
            data_series_key TEXT NOT NULL, -- e.g., M.W00.PGOLD.USD
            last_fetched_script TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (data_series_key, observation_date)
        )
        ''')
        
        # NEW: indicators_metadata table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS indicators_metadata (
            indicator_code TEXT PRIMARY KEY,
            description TEXT,
            first_seen_script_run TIMESTAMP,
            last_seen_active_script_run TIMESTAMP,
            is_currently_active INTEGER DEFAULT 0 CHECK(is_currently_active IN (0,1))
        )
        ''')
        conn.commit()
        logger.info(f"Database initialized/checked successfully at {db_path}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"SQLite error during DB initialization: {e}")
        raise # Re-raise the exception to halt if DB can't be set up

def save_series_to_db(conn, freq, area, indicator, unit, df, logger):
    """Saves the fetched and processed commodity data DataFrame to the database."""
    if df is None or df.empty:
        logger.info(f"No data to save for series {freq}.{area}.{indicator}.{unit}")
        return 0

    data_series_key = f"{freq}.{area}.{indicator}.{unit}"
    cursor = conn.cursor()
    insert_count = 0
    update_count = 0

    for date, row in df.iterrows():
        obs_date_str = date.strftime('%Y-%m-%d') if freq != 'A' else date.strftime('%Y') # Adjust date format as needed
        if freq == 'M':
            obs_date_str = date.strftime('%Y-%m')
        elif freq == 'A':
            obs_date_str = date.strftime('%Y')
        elif freq == 'Q': # Pandas to_datetime might parse YYYY-Q# to first day of quarter.
                         # Or API returns YYYY-Q[1-4]. Store as is or convert.
                         # For simplicity, let's assume API returns YYYY-MM for M, YYYY for A.
                         # The @TIME_PERIOD from API gives the original string.
                         # The current df['date'] is already a datetime object.
            obs_date_str = date.strftime('%Y-%m-%d') # Defaulting quarterly to first day of month

        obs_value = row['value']

        try:
            cursor.execute('''
            INSERT INTO commodity_prices 
            (frequency_code, area_code, indicator_code, unit_code, observation_date, observation_value, data_series_key)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(data_series_key, observation_date) DO UPDATE SET
            observation_value = excluded.observation_value,
            last_fetched_script = CURRENT_TIMESTAMP
            WHERE observation_value IS NOT excluded.observation_value; 
            ''', (freq, area, indicator, unit, obs_date_str, obs_value, data_series_key))
            
            if cursor.rowcount > 0:
                 # Check if it was an insert or an update that changed data
                 # This is a bit tricky with ON CONFLICT. A simpler way is separate INSERT/UPDATE
                 # For now, let's assume rowcount > 0 means something was written or updated.
                 # A more accurate count requires checking if value actually changed.
                 # The WHERE clause in DO UPDATE helps: only updates if value changed.
                 # So, cursor.rowcount effectively counts inserts + meaningful updates.
                 insert_count += 1 # Simplified: counting any successful write/update as one "written" record
                                   # More precisely: an insert or an update that changed the value.

        except sqlite3.Error as e:
            logger.error(f"SQLite error when saving {data_series_key} for date {obs_date_str}: {e}")

    conn.commit()
    if insert_count > 0:
        logger.info(f"Saved/Updated {insert_count} records for series {data_series_key} in the database.")
    else:
        logger.info(f"No new or changed records to save for series {data_series_key}.")
    return insert_count

# --- API Request ---
def get_imf_data(series_key_or_datastructure_path, logger):
    """
    Fetches data or data structure from the IMF API.
    Argument can be a series key like 'CompactData/PCPS/M.W00.PCOALAU_USD'
    or a data structure path like 'DataStructure/PCPS'.
    """
    try:
        url = f"{BASE_URL}{series_key_or_datastructure_path}"
        logger.info(f"Requesting URL: {url}")
        response = requests.get(url, timeout=30) # Added timeout
        response.raise_for_status()  # Raises an exception for bad status codes (4xx or 5xx)
        data = response.json()
        return data
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred for {url}: {http_err}")
        if hasattr(response, 'text'): logger.error(f"Response content: {response.text[:500]}...") # Log snippet
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred for {url}: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error occurred for {url}: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"An unexpected error occurred during request for {url}: {req_err}")
    except ValueError as json_err:  # Includes JSONDecodeError
        logger.error(f"JSON decoding error for {url}: {json_err}")
        if hasattr(response, 'text'): logger.error(f"Response content for JSON error: {response.text[:500]}...")
    except Exception as e:  # Generic catch-all for any other exceptions
        logger.exception(f"An unexpected general error occurred in get_imf_data for {url}: {e}")
    return None

# --- Data Structure Parsing ---
def parse_and_print_codelists(structure_data, logger): # Added logger
    """
    Parses the structure_data JSON and prints available codelists for dimensions.
    Now logs instead of printing directly for console, unless for explicit user view.
    """
    if not structure_data or 'Structure' not in structure_data or 'CodeLists' not in structure_data['Structure']:
        logger.warning("Could not find CodeLists in the provided structure data.")
        return

    codelists_data = structure_data['Structure']['CodeLists']['CodeList']
    
    dimensions_to_log = {
        "CL_FREQ": "Available Frequencies",
        "CL_AREA_PCPS": "Available Reference Areas",
        "CL_INDICATOR_PCPS": "Available Commodities (Indicators)",
        "CL_UNIT_PCPS": "Available Units of Measure"
    }

    key_families = structure_data.get('Structure', {}).get('KeyFamilies', {}).get('KeyFamily', {})
    if isinstance(key_families, list): # If multiple key families, take the first one (assuming PCPS is primary)
        key_families = next((kf for kf in key_families if kf.get('@id') == 'PCPS'), key_families[0] if key_families else {})
    
    dimension_concept_map = {}
    if key_families and 'Components' in key_families and 'Dimension' in key_families['Components']:
        for dim in key_families['Components']['Dimension']:
            dimension_concept_map[dim.get('@codelist')] = dim.get('@conceptRef')

    logger.info("--- Available Codes for PCPS Dataset (from DataStructure) ---")
    for cl in codelists_data:
        codelist_id = cl.get('@id')
        if codelist_id in dimensions_to_log:
            concept_ref = dimension_concept_map.get(codelist_id, "N/A")
            logger.info(f"--- {dimensions_to_log[codelist_id]} (Dimension: {codelist_id}, Concept: {concept_ref}) ---")
            codes = cl.get('Code', [])
            if not isinstance(codes, list): # If single code, make it a list
                codes = [codes]
            for code_entry in codes:
                value = code_entry.get('@value')
                description = "N/A"
                desc_obj = code_entry.get('Description')
                if isinstance(desc_obj, list):
                    description = desc_obj[0].get('#text', "N/A")
                elif isinstance(desc_obj, dict):
                    description = desc_obj.get('#text', "N/A")
                logger.info(f"  Code: {value:<15} | Description: {description}")
    logger.info("----------------------------------------")

# --- Data Processing ---
def process_data(json_data, series_key_identifier, logger): # Added logger
    """
    Processes the JSON data from IMF API into a pandas DataFrame.
    """
    if (not json_data or
            'CompactData' not in json_data or
            'DataSet' not in json_data['CompactData']):
        logger.warning(f"Unexpected JSON structure for {series_key_identifier}: 'CompactData' or 'DataSet' not found.")
        if json_data: logger.debug(f"Received JSON (first 500 chars): {str(json_data)[:500]}")
        return None

    dataset = json_data['CompactData']['DataSet']
    
    # The 'Series' data can be a dict (single series) or a list of dicts (multiple series)
    series_data_list = dataset.get('Series')
    if not series_data_list:
        logger.warning(f"No 'Series' data found in DataSet for {series_key_identifier}.")
        logger.debug(f"DataSet content for {series_key_identifier}: {str(dataset)[:500]}")
        return None

    if not isinstance(series_data_list, list):
        series_data_list = [series_data_list]

    all_series_df_list = [] # Changed name to avoid confusion with pandas DataFrame

    for series_data in series_data_list:
        if not isinstance(series_data, dict) or 'Obs' not in series_data:
            logger.warning(f"No 'Obs' (observations) found in a series entry for {series_key_identifier}.")
            logger.debug(f"Problematic series data: {str(series_data)[:500]}")
            continue  # Skip this series and try the next
        
        observations = series_data.get('Obs')
        if not observations: # Could be an empty list or None
            logger.info(f"Observations list is empty or missing for a series in {series_key_identifier}.")
            continue

        if not isinstance(observations, list): # If single observation, make it a list
            observations = [observations]
            
        # Extract series-specific attributes for more informative column naming or metadata
        series_attributes = {k: v for k, v in series_data.items() if k.startswith('@') and k != '@Obs'}
        series_desc_parts = [f"{k.replace('@','')}={v}" for k,v in series_attributes.items()]
        
        # Add explicitly parsed dimensions if available from series_attributes for better description
        explicit_freq = series_attributes.get('FREQ', series_key_identifier.split('.')[0])
        explicit_area = series_attributes.get('REF_AREA', series_key_identifier.split('.')[1])
        explicit_indicator = series_attributes.get('COMMODITY', series_key_identifier.split('.')[2])
        explicit_unit = series_attributes.get('UNIT_MEASURE', series_key_identifier.split('.')[3] if len(series_key_identifier.split('.')) > 3 else 'N/A')

        series_desc = f"FREQ={explicit_freq}, REF_AREA={explicit_area}, COMMODITY={explicit_indicator}, UNIT_MEASURE={explicit_unit}"
        
        base_year_info = f" (Base Year: {series_data['@BASE_YEAR']})" if series_data.get('@BASE_YEAR') else ""

        data_list = []
        for obs in observations:
            time_period_str = obs.get('@TIME_PERIOD') # This is the original string like YYYY, YYYY-MM, YYYY-Q#
            obs_value = obs.get('@OBS_VALUE')
            if time_period_str and obs_value is not None:  # Ensure both values exist
                data_list.append([time_period_str, obs_value])

        if not data_list:
            logger.info(f"No valid observations could be extracted for series {series_desc} within {series_key_identifier}.")
            continue

        try:
            df = pd.DataFrame(data_list, columns=['original_time_period', 'value'])
            # Attempt to convert to datetime; store original if fails or for specific handling in DB
            df['date'] = pd.to_datetime(df['original_time_period'], errors='coerce')
            
            # Handle cases where conversion to datetime might be None (e.g. for YYYY or YYYY-Q# if not directly parsable to day)
            # For database storage, we will use the 'original_time_period' for date key if 'date' is NaT
            # The save_series_to_db function will handle formatting based on frequency.
            
            df['value'] = df['value'].astype(float)
            # We need a proper date index for operations like iloc[-1] if used later,
            # but for DB saving, the original string or specific format is better.
            # Let's ensure 'date' column is used for processing and pass it to save_series_to_db
            df = df.set_index('date') # Use the new 'date' column as index
            df = df.sort_index()

        except Exception as e:
            logger.error(f"Error processing data into DataFrame for series {series_desc}: {e}")
            logger.debug(f"Data list that caused error: {data_list[:5]}")
            continue
        
        logger.info(f"Successfully processed data for series: {series_desc}{base_year_info}")
        all_series_df_list.append(df)

    if not all_series_df_list:
        logger.warning(f"No dataframes were created from the processed series for {series_key_identifier}.")
        return None
    
    # For now, if multiple series are returned, we'll return the first one.
    # Later, we can decide how to handle multiple series (e.g., merge or return a list).
    # If you expect multiple series from a single query (e.g. PCOAL*.W00..USD)
    # you might want to concatenate them or return them as a list/dict of DataFrames.
    if len(all_series_df_list) > 1:
        logger.warning(f"Multiple series processed ({len(all_series_df_list)}). Returning the first one for now.")
        # Example: Concatenate if they have the same index and different value columns
        # return pd.concat(all_series_df_list, axis=1) # This requires careful handling of column names
    
    return all_series_df_list[0]


# --- Main Execution ---
if __name__ == "__main__":
    # Setup logging
    logger = logging.getLogger("imf_data_fetcher")
    logger.setLevel(logging.INFO)
    # File handler
    fh = logging.FileHandler(LOG_FILE, mode='w') # Overwrite log each run
    fh.setLevel(logging.INFO)
    # Console handler (NEW)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter) # Use the same formatter for console
    # Add handlers to logger
    logger.addHandler(fh)
    logger.addHandler(ch) # Add console handler

    logger.info("Script starting. Logging to console and file.")
    
    try:
        db_connection = init_db(DB_FILE, logger)
    except Exception as e: # Catch error from init_db if it fails
        logger.critical(f"Failed to initialize database. Script cannot continue. Error: {e}")
        exit()

    # 1. Fetch Data Structure for PCPS to know available codes
    data_structure_key = 'DataStructure/PCPS'
    logger.info(f"Fetching Data Structure for: {data_structure_key}")
    structure_data = get_imf_data(data_structure_key, logger)
    time.sleep(1) # Delay after API call

    available_codes = {}
    current_api_indicators_info = [] # Store list of {'code': ..., 'description': ...} from API

    if structure_data and 'Structure' in structure_data and 'CodeLists' in structure_data['Structure']:
        codelists_data = structure_data['Structure']['CodeLists']['CodeList']
        dimensions_map = {
            "CL_FREQ": "frequencies",
            "CL_AREA_PCPS": "areas",
            "CL_INDICATOR_PCPS": "indicators", # This is what we need for current_api_indicators_info
            "CL_UNIT_PCPS": "units"
        }
        for cl in codelists_data:
            codelist_id = cl.get('@id')
            if codelist_id in dimensions_map:
                key_name = dimensions_map[codelist_id]
                available_codes[key_name] = []
                codes = cl.get('Code', [])
                if not isinstance(codes, list): codes = [codes]
                for code_entry in codes:
                    indicator_val = code_entry.get('@value')
                    indicator_desc_text = code_entry.get('Description', {}).get('#text', 'N/A')
                    if indicator_val != f"All_{key_name.capitalize()}":
                        item_info = {
                            'code': indicator_val,
                            'description': indicator_desc_text
                        }
                        available_codes[key_name].append(item_info)
                        if key_name == 'indicators':
                            current_api_indicators_info.append(item_info)
        
        logger.info("Successfully parsed available codes from DataStructure.")

        # --- Update indicators_metadata table ---
        if current_api_indicators_info:
            logger.info(f"Updating indicators_metadata table with {len(current_api_indicators_info)} indicators from API...")
            cursor = db_connection.cursor()
            # Step 1: Mark all existing indicators as inactive
            try:
                cursor.execute("UPDATE indicators_metadata SET is_currently_active = 0")
                logger.info(f"Marked {cursor.rowcount} existing indicators as inactive initially.")
            except sqlite3.Error as e:
                logger.error(f"Error marking existing indicators as inactive: {e}")

            # Step 2: Process current indicators from API (upsert logic)
            for ind_info in current_api_indicators_info:
                code = ind_info['code']
                desc = ind_info['description']
                try:
                    cursor.execute("SELECT 1 FROM indicators_metadata WHERE indicator_code = ?", (code,))
                    exists = cursor.fetchone()
                    current_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

                    if exists:
                        cursor.execute("""
                        UPDATE indicators_metadata 
                        SET description = ?, last_seen_active_script_run = ?, is_currently_active = 1
                        WHERE indicator_code = ?
                        """, (desc, current_timestamp, code))
                    else:
                        cursor.execute("""
                        INSERT INTO indicators_metadata 
                        (indicator_code, description, first_seen_script_run, last_seen_active_script_run, is_currently_active)
                        VALUES (?, ?, ?, ?, 1)
                        """, (code, desc, current_timestamp, current_timestamp))
                except sqlite3.Error as e:
                    logger.error(f"Error upserting indicator {code} into indicators_metadata: {e}")
            db_connection.commit()
            logger.info("Finished updating indicators_metadata table.")
        else:
            logger.warning("No current indicators found in API response to update metadata.")

        # --- WRITING INDICATORS TO A SEPARATE FILE (based on current_api_indicators_info) ---
        if current_api_indicators_info:
            try:
                with open(INDICATORS_LIST_FILE, 'w', encoding='utf-8') as f_indicators:
                    f_indicators.write("---- List of Currently Active Commodity Indicators (from IMF PCPS DataStructure) ----\n")
                    f_indicators.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n")
                    f_indicators.write(f"Source: {BASE_URL}DataStructure/PCPS\n")
                    f_indicators.write("----------------------------------------------------------------------\n")
                    f_indicators.write(f"{'Indicator Code':<18} | Description\n")
                    f_indicators.write("----------------------------------------------------------------------\n")
                    for ind in current_api_indicators_info: # Use the live list from API
                        f_indicators.write(f"{ind['code']:<18} | {ind['description']}\n")
                logger.info(f"Successfully wrote/updated currently active commodity indicators list to {INDICATORS_LIST_FILE}")
            except IOError as e:
                logger.error(f"Failed to write commodity indicators list to {INDICATORS_LIST_FILE}: {e}")
        else:
            logger.warning("No currently active indicators found to write to list file.")
        
    else:
        logger.error("Failed to fetch or parse data structure. Cannot define series or update metadata. Exiting.")
        if db_connection: db_connection.close()
        exit()

    series_to_process = []
    target_frequencies = [f_info['code'] for f_info in available_codes.get('frequencies', []) if f_info['code'] in ['M', 'A']]
    target_area = available_codes.get('areas', [{'code': 'W00'}])[0]['code']
    all_indicators_for_processing = current_api_indicators_info 
    default_units_for_price = [u_info['code'] for u_info in available_codes.get('units', []) if u_info['code'] == 'USD']
    default_units_for_index = [u_info['code'] for u_info in available_codes.get('units', []) if u_info['code'] == 'IX']

    if not all_indicators_for_processing:
        logger.error("No indicators identified from API to process for data fetching. Exiting.")
        if db_connection: db_connection.close()
        exit()
    if not target_frequencies:
        logger.warning("Target frequencies (M, A) not found in parsed data structure. Using defaults: ['M', 'A']")
        target_frequencies = ['M', 'A']
    if not default_units_for_price:
        logger.warning("USD unit not found in parsed units. Defaulting to ['USD'] for prices.")
        default_units_for_price = ['USD']
    if not default_units_for_index:
        logger.warning("IX unit not found in parsed units. Defaulting to ['IX'] for indices.")
        default_units_for_index = ['IX']

    for indicator_info in all_indicators_for_processing:
        indicator_code = indicator_info['code']
        indicator_desc = indicator_info['description']
        is_index_heuristic = (
            'index' in indicator_desc.lower() or \
            indicator_code.endswith('IX') or \
            indicator_code in ['PALLFNF', 'PALLMETA', 'PRAWM', 'PBEVE', 'PCERE', 'PFOOD', 'PNFUEL', 'PPMETA']
        )
        units_to_try = default_units_for_index if is_index_heuristic else default_units_for_price
        for freq_code in target_frequencies:
            for unit_code in units_to_try:
                series_to_process.append({
                    'freq': freq_code,
                    'area': target_area,
                    'indicator': indicator_code,
                    'unit': unit_code
                })

    logger.info(f"Targeting {len(all_indicators_for_processing)} distinct currently active indicators from the data structure.")
    logger.info(f"Will attempt to fetch and store {len(series_to_process)} series configurations (active indicators, M/A frequencies)...")
    
    total_records_saved_session = 0

    for i, series_config in enumerate(series_to_process):
        freq = series_config['freq']
        area = series_config['area']
        indicator = series_config['indicator']
        unit = series_config['unit']

        series_key_parts = f"{freq}.{area}.{indicator}.{unit}"
        series_key_to_fetch = f"CompactData/PCPS/{series_key_parts}"

        logger.info(f"({i+1}/{len(series_to_process)}) Attempting to fetch: {series_key_to_fetch}")
        raw_data = get_imf_data(series_key_to_fetch, logger)
        time.sleep(1) # Delay after each API call

        if raw_data:
            commodity_df = process_data(raw_data, series_key_to_fetch, logger)
            if commodity_df is not None and not commodity_df.empty:
                num_saved = save_series_to_db(db_connection, freq, area, indicator, unit, commodity_df, logger)
                total_records_saved_session += num_saved
            # else: # process_data already prints errors/issues
            #     logger.info(f"Could not process data for {series_key_to_fetch} or no data returned.")
        # else: # get_imf_data already prints errors/issues
        #    logger.info(f"Failed to fetch data for {series_key_to_fetch}.")
            
    logger.info("--- Session Summary ---")
    logger.info(f"Total new/updated observation records written to database in this session: {total_records_saved_session}")

    if db_connection:
        db_connection.close()
        logger.info("Database connection closed.")

    logger.info("Script finished.")
