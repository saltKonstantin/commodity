import sqlite3
import pandas as pd
from itertools import combinations
import os

DB_FILE = 'imf_commodities.sqlite'
INDICATORS_LIST_FILE = 'commodity_indicators_list.txt'
CSV_OUTPUT_FILE = 'full_correlation_results.csv'
DEFAULT_FREQUENCY = 'M'  # Using Monthly data for all calculations
MIN_MONTHS_FOR_CORRELATION = 12 # Minimum data points needed within the window for a valid correlation

def read_indicator_info(filepath):
    """Reads indicator codes and their descriptions from the specified file."""
    if not os.path.exists(filepath):
        print(f"Error: Indicator list file not found at {filepath}")
        return {}
    
    indicator_info_map = {}
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i < 5:  # Skip header lines (adjust if your header is different)
                    continue
                parts = line.split('|')
                if len(parts) > 1:
                    code = parts[0].strip()
                    description = parts[1].strip()
                    if code and description: # Ensure both are non-empty
                        indicator_info_map[code] = description
    except IOError as e:
        print(f"Error reading indicator list file {filepath}: {e}")
        return {}
    if not indicator_info_map:
        print(f"No indicator codes or descriptions found in {filepath}. Please check the file format and content.")
    return indicator_info_map

def get_all_monthly_data_for_indicator(indicator_code, conn):
    """
    Fetches all monthly observation_date and observation_value for a given indicator.
    Returns a DataFrame with the date as PeriodIndex and a single column named after the indicator_code.
    """
    cursor = conn.cursor()
    data = []
    try:
        cursor.execute("""
            SELECT observation_date, observation_value
            FROM commodity_prices
            WHERE indicator_code = ? 
              AND frequency_code = ? 
            ORDER BY observation_date
        """, (indicator_code, DEFAULT_FREQUENCY)) # Explicitly uses DEFAULT_FREQUENCY ('M')
        data = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error while fetching data for {indicator_code}: {e}")
        return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=['date', indicator_code])
    try:
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m').dt.to_period('M')
    except ValueError:
        # This general parsing might be slower or less precise if formats vary wildly.
        # print(f"Warning: Non-standard date format encountered for {indicator_code}. Attempting general parsing.")
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.to_period('M')
        
    df = df.set_index('date')
    df[indicator_code] = pd.to_numeric(df[indicator_code], errors='coerce')
    df = df.dropna() 
    return df

def main():
    print("Full Commodity Correlation Matrix Analyzer")
    print("This script calculates correlations based on MONTHLY data.")
    
    analysis_months = 0
    use_max_timeframe_for_pairs = False

    while True:
        try:
            user_input = input("Enter the number of months for the analysis window (e.g., 12, 24, 60, or 'max' for full history per pair): ").strip().lower()
            if user_input == 'max':
                use_max_timeframe_for_pairs = True
                print("Selected: Maximum available common history for each pair.")
                break
            else:
                analysis_months = int(user_input)
                if analysis_months <= 0:
                    print("Number of months must be positive.")
                elif analysis_months < MIN_MONTHS_FOR_CORRELATION:
                    print(f"Warning: Requested window is {analysis_months} months, which is less than the minimum {MIN_MONTHS_FOR_CORRELATION} months required for a correlation calculation for any given pair.")
                    print(f"Pairs with fewer than {MIN_MONTHS_FOR_CORRELATION} overlapping data points within their specific window will be skipped.")
                    break 
                else:
                    break
        except ValueError:
            print("Invalid input. Please enter a whole number for months or 'max'.")

    print(f"\nUsing database: {DB_FILE}")
    print(f"Reading indicators from: {INDICATORS_LIST_FILE}")
    if use_max_timeframe_for_pairs:
        print(f"Analysis timeframe: Maximum available common history for each pair.")
    else:
        print(f"Requested analysis window length: {analysis_months} month(s).")
    print(f"Correlations will be calculated for pairs if they have at least {MIN_MONTHS_FOR_CORRELATION} common monthly data points within the chosen timeframe.")

    indicator_info_map = read_indicator_info(INDICATORS_LIST_FILE)
    if not indicator_info_map:
        print("Exiting due to issues with indicator list.")
        return
    
    indicator_codes = list(indicator_info_map.keys())
    print(f"Found {len(indicator_codes)} potential indicators with descriptions.")

    conn = None
    data_cache = {}
    
    try:
        if not os.path.exists(DB_FILE):
            print(f"Error: Database file not found at {DB_FILE}")
            return
        conn = sqlite3.connect(DB_FILE)

        print("Fetching all monthly data for indicators...")
        # Use indicator_codes which are the keys from indicator_info_map
        for i, code in enumerate(indicator_codes):
            df = get_all_monthly_data_for_indicator(code, conn)
            if not df.empty:
                data_cache[code] = df
            # Progress update
            if (i + 1) % 20 == 0 or (i + 1) == len(indicator_codes):
                print(f"  Fetched data for {i + 1}/{len(indicator_codes)} indicators...")
        
        print(f"\nSuccessfully fetched data for {len(data_cache)} indicators with available monthly series.")
        if len(data_cache) < 2:
            print("Not enough indicators with data to perform correlation analysis. Exiting.")
            return

        # data_cache keys are the codes we want to pair up
        all_pairs = list(combinations(data_cache.keys(), 2))
        correlation_results = []
        
        print(f"\nCalculating correlations for {len(all_pairs)} pairs...")
        if use_max_timeframe_for_pairs:
            print(f"Using the maximum available common history for each pair.")
        else:
            print(f"Using a {analysis_months}-month rolling window for each pair.")
            print(f"(The exact start/end dates for each pair's {analysis_months}-month window will be in the CSV)")

        for i, (code1, code2) in enumerate(all_pairs):
            df1_full = data_cache.get(code1)
            df2_full = data_cache.get(code2)

            if df1_full is None or df1_full.empty or df1_full.index.empty or \
               df2_full is None or df2_full.empty or df2_full.index.empty:
                # print(f"Skipping pair ({code1}, {code2}) due to missing or empty data.")
                continue

            pair_analysis_start_period = None
            pair_analysis_end_period = None
            merged_df = pd.DataFrame() # Initialize an empty DataFrame

            if use_max_timeframe_for_pairs:
                # Use the full history of both dataframes
                merged_df = pd.merge(df1_full, df2_full, left_index=True, right_index=True, how='inner')
                if not merged_df.empty:
                    pair_analysis_start_period = merged_df.index.min()
                    pair_analysis_end_period = merged_df.index.max()
            else: # Specific number of months window
                latest_period1 = df1_full.index.max()
                latest_period2 = df2_full.index.max()

                if pd.isna(latest_period1) or pd.isna(latest_period2):
                    # print(f"Skipping pair ({code1}, {code2}) due to NA latest period.")
                    continue
                    
                pair_analysis_end_period = min(latest_period1, latest_period2)
                # Calculate start period for the N-month window
                pair_analysis_start_period = pair_analysis_end_period - (analysis_months - 1)
                
                # Slice data to this specific window for both dataframes
                df1_window = df1_full[(df1_full.index >= pair_analysis_start_period) & (df1_full.index <= pair_analysis_end_period)]
                df2_window = df2_full[(df2_full.index >= pair_analysis_start_period) & (df2_full.index <= pair_analysis_end_period)]
                
                merged_df = pd.merge(df1_window, df2_window, left_index=True, right_index=True, how='inner')

            # Now, the crucial check: ensure enough data points *within this specific window*
            if not merged_df.empty and len(merged_df) >= MIN_MONTHS_FOR_CORRELATION:
                if code1 in merged_df.columns and code2 in merged_df.columns:
                    correlation = merged_df[code1].corr(merged_df[code2])
                    if not pd.isna(correlation):
                        desc1 = indicator_info_map.get(code1, 'N/A')
                        desc2 = indicator_info_map.get(code2, 'N/A')
                        
                        # Remove prefix from descriptions for CSV output
                        prefix_to_remove = "Primary Commodity Prices, "
                        if desc1.startswith(prefix_to_remove):
                            desc1 = desc1[len(prefix_to_remove):]
                        if desc2.startswith(prefix_to_remove):
                            desc2 = desc2[len(prefix_to_remove):]

                        correlation_results.append({
                            'indicator1_code': code1,
                            'indicator1_description': desc1,
                            'indicator2_code': code2,
                            'indicator2_description': desc2,
                            'correlation': correlation,
                            'common_months_in_window': len(merged_df),
                            'analysis_window_start': pair_analysis_start_period.strftime('%Y-%m'),
                            'analysis_window_end': pair_analysis_end_period.strftime('%Y-%m')
                        })
            # else:
                # if merged_df.empty:
                    # print(f"Pair ({code1}, {code2}) had no common data in the window.")
                # else:
                    # print(f"Pair ({code1}, {code2}) had {len(merged_df)} common months, less than {MIN_MONTHS_FOR_CORRELATION} required.")

            # Progress update
            if (i + 1) % 500 == 0 or (i + 1) == len(all_pairs):
                print(f"  Processed {i + 1}/{len(all_pairs)} pairs...")

        if not correlation_results:
            print("\nNo indicator pairs met the criteria for correlation analysis within the specified timeframe.")
            # No return here, proceed to print completion message
        
        results_df = pd.DataFrame(correlation_results)
        if not results_df.empty:
            results_df.sort_values(by='correlation', ascending=False, inplace=True)
            results_df.index = range(1, len(results_df) + 1) # Reset index to start from 1 after sorting
            results_df.to_csv(CSV_OUTPUT_FILE, index_label='rank')
            print(f"\n--- Analysis Complete ---")
            print(f"Ranked correlation results saved to: {CSV_OUTPUT_FILE}")
            print(f"{len(correlation_results)} pairs met the criteria and were included in the CSV.")
        else:
            print("\n--- Analysis Complete ---")
            print("No pairs met the criteria to be included in the CSV.")

    except sqlite3.Error as e:
        print(f"A database error occurred: {e}")
    except IOError as e: # Catches file I/O errors, e.g. writing to CSV_OUTPUT_FILE
        print(f"An error occurred writing to {CSV_OUTPUT_FILE}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for unexpected errors
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main() 