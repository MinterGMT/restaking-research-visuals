
# This script performs a concentration analysis on the EigenLayer operator market,
# focusing on delegations from major Liquid Restaking Token (LRT) protocols.
# It fetches data from a Dune Analytics query, calculates HHI and Gini coefficients,
# and generates visualizations including bar charts and Lorenz curves.

import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from dune_client.client import DuneClient
import matplotlib.pyplot as plt
import seaborn as sns

# --- 1. Setup ---
# Load environment variables from a .env file in the same directory
load_dotenv()
dune_api_key = os.getenv("DUNE_API_KEY")

# Check if the API key was loaded.
if not dune_api_key:
    raise ValueError("DUNE_API_KEY not found. Please create a .env file with your key.")

# Initialize the Dune client with the API key.
dune = DuneClient(dune_api_key)
print("✅ Dune client initialized.")

# --- 2. Data Acquisition ---
# The script uses a pre-defined Dune query that provides a complete breakdown 
# of all active operators, labeled by their affiliated LRT protocol.
FINAL_OPERATOR_QUERY_ID = 5292464 

try:
    print(f"Fetching latest results for query ID: {FINAL_OPERATOR_QUERY_ID}")
    # Retrieve the latest results from the specified Dune query.
    query_result = dune.get_latest_result(FINAL_OPERATOR_QUERY_ID)
    print("✅ Successfully fetched latest results from Dune.")

    # Convert the raw query result into a pandas DataFrame
    results_df = pd.DataFrame(query_result.result.rows)
    
    # Ensure the 'USD value Delegated' column is treated as a numeric type for calculations
    results_df['USD value Delegated'] = pd.to_numeric(results_df['USD value Delegated'])
    print(f"✅ Processed {len(results_df)} rows into a pandas DataFrame.")

except Exception as e:
    print(f"❌ Error fetching data from Dune: {e}")
    exit()


# --- 3. Analysis Functions ---
def calculate_hhi(shares):
    """Calculates the Herfindahl-Hirschman Index from a pandas Series of percentages."""
    # If there are no shares, concentration is zero.
    if shares.sum() == 0:
        return 0
    # The formula requires shares to be expressed as percentages (e.g., 50 for 50%)
    percentages = (shares / shares.sum()) * 100
    # The HHI is the sum of the squares of the market share percentages.
    return (percentages**2).sum()

def calculate_gini(arr):
    """
    Calculates the Gini coefficient of a numpy array using a stable formula.
    """
    # Ensure the input is a numpy array and filter out negative values (which are invalid for this context).
    arr = np.asarray(arr, dtype=np.float64)
    arr = arr[arr >= 0]

    # The Gini coefficient is undefined for empty or all-zero arrays.
    # The second condition handles cases where the sum is zero.
    if arr.size == 0 or np.sum(arr) == 0:
        return np.nan # Using NaN is more explicit than 0 for an undefined state

    # The formula requires the array be sorted in ascending order
    sorted_arr = np.sort(arr)
    n = len(sorted_arr)
    # The formula uses 1-based indexing for i, so we create an index from 1 to n.
    index = np.arange(1, n + 1)
    
    # Gini coefficient formula: sum((2 * i - n - 1) * x_i) / (n * sum(x_i))
    numerator = np.sum((2 * index - n - 1) * sorted_arr)
    denominator = n * np.sum(sorted_arr)
    
    # This check is a safeguard, although already handled by the initial check for a zero sum.
    if denominator == 0:
        return np.nan
        
    return numerator / denominator

def plot_lorenz_curve(df, group_name, output_dir):
    """Plots the Lorenz curve for a given DataFrame."""
    # Skip plotting if the Dataframe is empty or has no stake.
    if df.empty or df['USD value Delegated'].sum() == 0:
        print(f"Skipping Lorenz curve for {group_name}: No data to plot.")
        return
    
    # Prepare the data for the Lorenz curve.
    values = df['USD value Delegated'].fillna(0).values
    sorted_values = np.sort(values)
    cum_values = np.cumsum(sorted_values)
    n = len(sorted_values)

    # The Lorenz curve is undefined for an empty set of operators
    if n == 0:
        return
    
    # Create cumulative percentages
    # x-axis: cumulative fraction of the population (operators) [1/n, 2/n, ..., n/n]
    # y-axis: cumulative fraction of the total wealth (delegated stake)
    cum_op_perc = np.arange(1, n+1) / n
    cum_stake_perc = cum_values / cum_values[-1]

    # Insert a (0,0) point to start the curve from the origin
    cum_op_perc = np.insert(cum_op_perc, 0, 0)
    cum_stake_perc = np.insert(cum_stake_perc, 0, 0)

    # Create and style the plot
    fig_lorenz, ax_lorenz = plt.subplots(figsize = (10, 10))
    # Plot the line of perfect equality for reference.
    ax_lorenz.plot([0,1], [0,1], label='Line of Perfect Equality', color='red', linestyle='--')
    # Plot the actual Lorenz curve for the data.
    ax_lorenz.plot(cum_op_perc, cum_stake_perc, label=f'{group_name} Lorenz Curve', color='navy')
    
    ax_lorenz.set_title(f'Lorenz Curve for: {group_name}', fontsize=16, weight='bold')
    ax_lorenz.set_xlabel('Cumulative % of Operators', fontsize=12)
    ax_lorenz.set_ylabel('Cumulative % of Delegated Stake', fontsize=12)
    ax_lorenz.tick_params(axis='both', which='major', labelsize=10)
    ax_lorenz.legend()
    ax_lorenz.grid(True)
    ax_lorenz.set_aspect('equal', adjustable='box') 

    # Save the plot to a file.
    output_path = os.path.join(output_dir, f'{group_name}_lorenz_curve.png')
    plt.savefig(output_path, dpi=300)
    plt.close(fig_lorenz)    

def run_analysis(df, group_name, output_dir, analysis_type='Protocol'):
    """Performs a full concentration analysis on a given DataFrame."""
    if analysis_type == 'Protocol':
        # Filter the Dataframe for a specific LRT protocol or the "Other" category
        protocol_df = df[df['Protocol'] == group_name].copy()
    else:
        # Use the entire DataFrame for the overall market analysis
        protocol_df = df.copy()

    # Skip analysis if there is no data for the group.
    if protocol_df.empty:
        return None

    # --- Calculations ---
    num_operators = len(protocol_df)
    total_stake_usd = protocol_df['USD value Delegated'].sum()
    hhi = calculate_hhi(protocol_df['USD value Delegated'])
    gini = calculate_gini(protocol_df['USD value Delegated'].values)

    # --- Visualization: Bar Chart ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12,10))
    
    sorted_protocol_df = protocol_df.sort_values(by='USD value Delegated', ascending=False)
    
    # If there are more than 15 operators, show a "Top 15" subset instead of the full list
    if num_operators > 15:
        chart_title = f'Top 15 Operator Stake Concentration for: {group_name}'
        data_to_plot = sorted_protocol_df.head(15)
    else:
        chart_title = f'Operator Stake Concentration for: {group_name}'
        data_to_plot = sorted_protocol_df

    # Sort by stake to identify the top operators for clear visualization.
    sns.barplot(
        data=data_to_plot, 
        y='Operator Name',
        x='USD value Delegated',
        ax=ax, 
        palette='Blues_r',
        orient='h'
    )

    ax.set_title(chart_title, fontsize=18, weight='bold')
    ax.set_ylabel('Operator Name', fontsize=14)
    ax.set_xlabel('USD Value Delegated', fontsize=14)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e6:.1f}M')) # Format x-axis in millions
    ax.tick_params(axis='both', which='major', labelsize=12)
    plt.tight_layout(pad=1.5)

    # Save the bar chart to a file.
    output_path = os.path.join(output_dir, f'{group_name}_concentration.png')
    plt.savefig(output_path, dpi=300),
    plt.close(fig)

    # --- Visualization: Lorenz Curve ---
    plot_lorenz_curve(protocol_df, group_name, output_dir)

    # Return a dictionary of the calculated metrics for summary reporting.
    return {
        "Group": group_name,
        "Number of Operators": num_operators,
        "Total Delegated USD": f"${total_stake_usd:,.0f}",
        "HHI": f"{hhi:,.2f}",
        "Gini Coefficient": f"{gini:.4f}"
    }

# --- 4. Main Execution ---
output_dir = "outputs"
module_output_dir = os.path.join(output_dir, "module1_LRT_concentration")
if not os.path.exists(module_output_dir):
    os.makedirs(module_output_dir)
    print(f"📁 Created output directory: {module_output_dir}")

all_results = []

# First, run the Macro Analysis on the entire operator market to get a baseline.
print("\n--- Running Macro Analysis on Overall Operator Market ---")
overall_market_results = run_analysis(results_df, 'Overall Market', module_output_dir, analysis_type='Overall')
if overall_market_results:
    all_results.append(overall_market_results)

# Second, run the Micro Analysis for each individual LRT protocol
print("\n--- Running Micro Analysis for Each LRT Protocol ---")
protocols_to_analyze = results_df['Protocol'].unique()
for protocol in sorted(protocols_to_analyze):
    if protocol == 'Other': # Skip the "Other" category if it's not a single protocol.
        continue
    result = run_analysis(results_df, protocol, module_output_dir, analysis_type='Protocol')
    if result:
        all_results.append(result)

# --- 5. Final Summary ---
if all_results:
    summary_df = pd.DataFrame(all_results)
    print("\n\n==================================================")
    print("   Thesis Centralization Analysis Summary")
    print("==================================================")
    print(summary_df.to_string(index=False))
    
    # Save the final summary table to a CSV file.
    summary_path = os.path.join(module_output_dir, 'centralization_summary.csv')
    summary_df.to_csv(summary_path, index=False)
    print(f"\n✅ Summary and all charts saved to {module_output_dir}")