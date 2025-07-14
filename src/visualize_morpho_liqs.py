
# This script fetches pre-aggregated daily liquidation data for ezETH on Morpho Blue
# during the April 2024 crisis week. It generates a static bar chart to be
# used as a key piece of evidence (Figure 4.9) in the case study analysis.

import os
import pandas as pd
from dotenv import load_dotenv
from dune_client.client import DuneClient
import matplotlib.pyplot as plt
import seaborn as sns

# --- 1. SETUP ---
# Initialize the script and establish the connection to the Dune API.
print("📊 Initializing Morpho Liquidations Visualization Script...")
load_dotenv()
dune = DuneClient(os.getenv("DUNE_API_KEY"))
# The ID for the Dune query that aggregates liquidations specifically for the crisis week.
QUERY_ID_MORPHO_LIQS_FOCUSED = 5323305

# --- 2. DATA ACQUISITION ---
# Get the query results from Dune's API and load them into a pandas DataFrame.
print("Fetching liquidation data from Dune...")
try:
    liquidations_df = dune.get_latest_result_dataframe(QUERY_ID_MORPHO_LIQS_FOCUSED)
    print("✅ Successfully fetched data.")
except Exception as e:
    print(f"❌ Error fetching data: {e}")
    exit()

# --- 3. DATA PROCESSING ---
# This section ensures the data is clean and correctly formatted for plotting.
# Convert the 'day' column from text to proper datetime objects.
liquidations_df['day'] = pd.to_datetime(liquidations_df['day'])

# Create a "date scaffold" to provide context for days with zero liquidations.
# This prevents the plotting library from misinterpreting a single data point.
date_range = pd.date_range(start='2024-04-23', end='2024-04-26', freq='D')
scaffold_df = pd.DataFrame(date_range, columns=['day'])
# Ensure the scaffold's timezone matches the Dune data (UTC) to allow for a successful merge.
scaffold_df['day'] = pd.to_datetime(scaffold_df['day']).dt.tz_localize('UTC')

# Merge the liquidation data onto the scaffold. Days without liquidations will have zero values.
aligned_df = pd.merge(scaffold_df, liquidations_df, on='day', how='left').fillna(0)
# Create a clean, text-based label for the x-axis to ensure correct categorical plotting.
aligned_df['date_label'] = aligned_df['day'].dt.strftime('%b %d')

# --- 4. VISUALIZATION ---
# This section generates the final, publication-quality static chart.
print("🎨 Generating chart...")
plt.style.use('seaborn-v0_8-whitegrid')
fig, ax = plt.subplots(figsize=(12, 7))

# Create the bar plot using the prepared string labels for the x-axis.
bar_plot = sns.barplot(
    data=aligned_df,
    x='date_label', 
    y='total_usd_liquidated',
    ax=ax,
    color='#E55807'
)

# Add data labels on top of the bars
for p in bar_plot.patches:
    if p.get_height() > 0:
        ax.annotate(f'${p.get_height()/1e6:.1f}M', 
                       (p.get_x() + p.get_width() / 2., p.get_height()), 
                       ha = 'center', va = 'center', xytext = (0, 9), textcoords = 'offset points',
                       fontsize=12, weight='bold')

# Formatting
ax.set_title('Daily ezETH Liquidations on Morpho Blue', fontsize=18, weight='bold')
ax.set_xlabel('Date', fontsize=14)
ax.set_ylabel('Value Liquidated (USD)', fontsize=14)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, p: f'${y/1e6:,.0f}M'))
plt.xticks(rotation=0, fontsize=12) 
plt.tight_layout(pad=1.5)

# --- 5. OUTPUT ---
output_dir = "outputs"
module_output_dir = os.path.join(output_dir, "module2_depeg_analysis")
if not os.path.exists(module_output_dir):
    os.makedirs(module_output_dir)
output_path = os.path.join(module_output_dir, "figure_market_wide_dex_volume.png")
plt.savefig(output_path, dpi=300)
print(f"✅ Chart saved to '{output_path}'")

plt.show()