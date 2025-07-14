
# This script fetches daily trading volume for ezETH across all major decentralized
# exchanges (DEXs) during the crisis week. It processes the data and generates a
# stacked bar chart to provide context for the case study's focus on Balancer.

import os
import pandas as pd
from dotenv import load_dotenv
from dune_client.client import DuneClient
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates

# --- 1. SETUP ---
# Initialize script and connect to Dune API.
print("📊 Initializing Market-Wide DEX Volume Visualization Script...")
load_dotenv()
dune = DuneClient(os.getenv("DUNE_API_KEY"))

# The ID for the Dune query that aggregates daily volume by DEX project.
QUERY_ID_DEX_VOLUME = 5301668

# --- 2. DATA ACQUISITION ---
# Get the query results from Dune's API.
print("Fetching DEX volume data from Dune...")
try:
    dex_volume_df = dune.get_latest_result_dataframe(QUERY_ID_DEX_VOLUME)
    print("✅ Successfully fetched data.")
except Exception as e:
    print(f"❌ Error fetching data: {e}")
    exit()

# --- 3. DATA PROCESSING ---
# This section transforms the data into the correct format for a stacked bar chart.
dex_volume_df['day'] = pd.to_datetime(dex_volume_df['day'])
# Pivot the data: transform the 'project' column into separate columns for each DEX,
# with the values being the total trading volume. This is the standard format for stacking.
pivot_df = dex_volume_df.pivot(index='day', columns='project', values='total_volume_usd').fillna(0)
# Ensure the columns are in a logical order for stacking (biggest at bottom)
pivot_df = pivot_df[['balancer', 'uniswap', 'curve', '0x-API', '1inch-LOP']]

# --- 4. VISUALIZATION ---
print("🎨 Generating chart...")
plt.style.use('seaborn-v0_8-whitegrid')
fig, ax = plt.subplots(figsize=(12, 8))

# Create the stacked bar chart
pivot_df.plot(
    kind='bar',
    stacked=True,
    ax=ax,
    colormap='viridis' 
)

# Formatting
ax.set_title('Daily ezETH Trading Volume by DEX Surrounding the Crisis Period', fontsize=18, weight='bold')
ax.set_xlabel('Date', fontsize=14)
ax.set_ylabel('Trading Volume (USD)', fontsize=14)
# Format y-axis in millions of dollars.
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, p: f'${y/1e6:,.0f}M'))
ax.legend(title='DEX Project', fontsize=11, title_fontsize=12)
plt.xticks(rotation=0, ha='center', fontsize=12)
plt.yticks(fontsize=12)
# Use the dates from the index for x-tick labels
ax.set_xticklabels(pivot_df.index.strftime('%b %d'))
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