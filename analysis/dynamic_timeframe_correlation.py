import sqlite3
import pandas as pd
import os
import matplotlib.pyplot as plt # Added for plotting

# Adjusted paths for new directory structure
DB_FILE = os.path.join('..', 'database', 'imf_commodities.sqlite')
INDICATORS_LIST_FILE = os.path.join('..', 'database', 'commodity_indicators_list.txt')
DEFAULT_FREQUENCY = 'M'  # Using Monthly data
FIXED_WINDOW_SIZE = 12 # Each correlation window will be this many months

def read_indicator_info(filepath):
    """Reads indicator codes and their descriptions from the specified file."""
    if not os.path.exists(filepath):
        print(f"Error: Indicator list file not found at {filepath}")
        return {}
    
    indicator_info_map = {}
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i < 5:  # Skip header lines
                    continue
                parts = line.split('|')
                if len(parts) > 1:
                    code = parts[0].strip()
                    description = parts[1].strip()
                    if code and description:
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
        """, (indicator_code, DEFAULT_FREQUENCY))
        data = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error while fetching data for {indicator_code}: {e}")
        return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=['date', indicator_code])
    try:
        # Convert to PeriodIndex first for consistent date handling
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m').dt.to_period('M')
    except ValueError:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.to_period('M')
        
    df = df.set_index('date')
    df[indicator_code] = pd.to_numeric(df[indicator_code], errors='coerce')
    df = df.dropna() 
    return df

def plot_correlation_data(df, indicator1_code, indicator2_code, window_size, output_plot_filename):
    """Generates and saves a plot from the correlation DataFrame."""
    try:
        df_plot = df.copy()
        df_plot['analysis_window_end_dt'] = pd.to_datetime(df_plot['analysis_window_end'])
        df_plot = df_plot.sort_values(by='analysis_window_end_dt')

        plt.figure(figsize=(14, 7))
        plt.plot(df_plot['analysis_window_end_dt'], df_plot['correlation'], marker='.', linestyle='-', markersize=5)
        
        title = f'{window_size}-Month Rolling Correlation: {indicator1_code} vs {indicator2_code}'
        plt.title(title, fontsize=16)
        plt.xlabel("End Date of Rolling Window", fontsize=12)
        plt.ylabel("Pearson Correlation Coefficient", fontsize=12)
        plt.ylim(-1.1, 1.1)
        plt.grid(True, which='major', linestyle='--', linewidth=0.5)
        plt.axhline(0, color='black', linestyle='--', linewidth=0.7)
        
        plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m'))
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        plt.savefig(output_plot_filename, format='jpg')
        print(f"Chart saved as: {output_plot_filename}")

    except Exception as e:
        print(f"Error generating plot: {e}")
        import traceback
        traceback.print_exc()

def main():
    print(f"True Rolling {FIXED_WINDOW_SIZE}-Month Correlation Analyzer & Plotter")
    print(f"This script calculates correlations for a {FIXED_WINDOW_SIZE}-month window, rolling forward one month at a time.")
    print("It saves results to a CSV and generates a JPG plot.")
    print("Ensure you have matplotlib installed: pip install matplotlib")

    indicator_info_map = read_indicator_info(INDICATORS_LIST_FILE)
    if not indicator_info_map:
        print("Exiting due to issues with indicator list.")
        return

    while True:
        indicator_code1 = input(f"Enter the first indicator code (e.g., PGOLD): ").strip().upper()
        if indicator_code1 in indicator_info_map:
            break
        else:
            print(f"Error: Indicator code '{indicator_code1}' not found in {INDICATORS_LIST_FILE}. Please try again.")
            print(f"Available codes start with 'P...' - examples: PCOALAU, POILAPSP, PWHEAUS...")

    while True:
        indicator_code2 = input(f"Enter the second indicator code (e.g., POILWTI): ").strip().upper()
        if indicator_code2 == indicator_code1:
            print("Error: Please enter two different indicator codes.")
        elif indicator_code2 in indicator_info_map:
            break
        else:
            print(f"Error: Indicator code '{indicator_code2}' not found in {INDICATORS_LIST_FILE}. Please try again.")
            print(f"Available codes start with 'P...' - examples: PCOALAU, POILAPSP, PWHEAUS...")

    print(f"\nAnalyzing true rolling {FIXED_WINDOW_SIZE}-month correlations between {indicator_code1} and {indicator_code2}.")
    print(f"Using database: {DB_FILE}")

    conn = None
    
    # Define output directory and ensure it exists
    output_dir = 'output'
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            print(f"Created output directory: {output_dir}")
        except OSError as e:
            print(f"Error creating output directory {output_dir}: {e}")
            return
            
    csv_output_filename = os.path.join(output_dir, f"true_rolling_{FIXED_WINDOW_SIZE}m_correlation_{indicator_code1}_{indicator_code2}.csv")
    plot_output_filename = os.path.join(output_dir, f"true_rolling_{FIXED_WINDOW_SIZE}m_correlation_{indicator_code1}_{indicator_code2}.jpg")

    try:
        if not os.path.exists(DB_FILE):
            print(f"Error: Database file not found at {DB_FILE}")
            return
        conn = sqlite3.connect(DB_FILE)

        df1_full = get_all_monthly_data_for_indicator(indicator_code1, conn)
        df2_full = get_all_monthly_data_for_indicator(indicator_code2, conn)

        if df1_full.empty:
            print(f"No monthly data found for {indicator_code1}. Exiting.")
            return
        if df2_full.empty:
            print(f"No monthly data found for {indicator_code2}. Exiting.")
            return
            
        merged_data_all_history = pd.merge(df1_full, df2_full, left_index=True, right_index=True, how='inner')
        max_common_months = len(merged_data_all_history)

        if max_common_months < FIXED_WINDOW_SIZE:
            print(f"Not enough common historical data for {indicator_code1} and {indicator_code2} (found {max_common_months} common months). Need at least {FIXED_WINDOW_SIZE} months. Exiting.")
            return

        num_possible_windows = max_common_months - FIXED_WINDOW_SIZE + 1
        print(f"Found {max_common_months} common monthly data points for the pair.")
        print(f"Calculating correlations for {num_possible_windows} rolling {FIXED_WINDOW_SIZE}-month windows (1-month step)...")

        correlation_results = []
        
        desc1_full = indicator_info_map.get(indicator_code1, 'N/A')
        desc2_full = indicator_info_map.get(indicator_code2, 'N/A')
        prefix_to_remove = "Primary Commodity Prices, "
        desc1_cleaned = desc1_full[len(prefix_to_remove):] if desc1_full.startswith(prefix_to_remove) else desc1_full
        desc2_cleaned = desc2_full[len(prefix_to_remove):] if desc2_full.startswith(prefix_to_remove) else desc2_full

        for i in range(num_possible_windows):
            start_idx = i
            end_idx = i + FIXED_WINDOW_SIZE
            current_window_df = merged_data_all_history.iloc[start_idx:end_idx]
            
            if len(current_window_df) < FIXED_WINDOW_SIZE: 
                print(f"Warning: Skipped a window due to unexpected insufficient data ({len(current_window_df)} months) at step {i}.")
                continue

            correlation = current_window_df[indicator_code1].corr(current_window_df[indicator_code2])
            
            window_start_date_period = current_window_df.index.min()
            window_end_date_period = current_window_df.index.max()
            
            correlation_results.append({
                'indicator1_code': indicator_code1,
                'indicator1_description': desc1_cleaned,
                'indicator2_code': indicator_code2,
                'indicator2_description': desc2_cleaned,
                'timeframe_length_months': FIXED_WINDOW_SIZE,
                'correlation': correlation if not pd.isna(correlation) else None,
                'actual_common_months_in_window': len(current_window_df),
                'analysis_window_start': window_start_date_period.strftime('%Y-%m'),
                'analysis_window_end': window_end_date_period.strftime('%Y-%m')
            })
            
            if (i + 1) % 50 == 0 or (i + 1) == num_possible_windows:
                 print(f"  Calculated for rolling window {i + 1}/{num_possible_windows} (Ending: {window_end_date_period.strftime('%Y-%m')})...")

        if not correlation_results:
            print(f"\nNo correlation results generated. Please check data.")
            return

        results_df = pd.DataFrame(correlation_results)
        results_df.to_csv(csv_output_filename, index=False)
        print(f"\n--- CSV Generation Complete ---")
        print(f"True rolling {FIXED_WINDOW_SIZE}-month correlation analysis saved to: {csv_output_filename}")
        print(f"CSV contains {len(results_df)} rows.")

        # Generate and save the plot
        print(f"\nGenerating plot...")
        plot_correlation_data(results_df, indicator_code1, indicator_code2, FIXED_WINDOW_SIZE, plot_output_filename)

        print(f"\n--- Analysis and Plotting Complete ---")

    except sqlite3.Error as e:
        print(f"A database error occurred: {e}")
    except IOError as e: # Covers issues with CSV or plot file writing
        print(f"An I/O error occurred (e.g., writing CSV or plot file): {e}")
    except ImportError:
        print("Error: matplotlib library not found. Please install it by running: pip install matplotlib")     
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main() 