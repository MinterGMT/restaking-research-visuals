
# This script reconstructs the April 2024 ezETH de-peg event by fetching
# on-chain data from multiple Dune queries. It processes and visualizes
# the data to create a multi-panel narrative chart of the crisis.

import os
import pandas as pd
from dotenv import load_dotenv
from dune_client.client import DuneClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# --- 1. SETUP ---
print("🚀 Initializing script...")
load_dotenv()
dune = DuneClient(os.getenv("DUNE_API_KEY"))
print("✅ Dune client initialized.")

# --- 2. DATA ACQUISITION ---
# Define the Dune Query IDs for the various datasets needed for the analysis.
QUERY_ID_PRICE_VOLUME = 5299669 # Minute-by-minute price and volume from Balancer
QUERY_ID_WETH_DRAIN = 5299808 # Cumulative WETH drained from the Balancer pool.
QUERY_ID_MORPHO_LIQS = 5323305 # Daily liquidations on Morpho Blue.
QUERY_ID_BLAST_FLOWS = 5306342 # Daily deposits/withdrawals from Blast L2 vault.

print("Fetching data from Dune...")
# Fetch all datasets and load them into pandas DataFrames.
price_vol_df = dune.get_latest_result_dataframe(QUERY_ID_PRICE_VOLUME)
weth_drain_df = dune.get_latest_result_dataframe(QUERY_ID_WETH_DRAIN)
morpho_liqs_df = dune.get_latest_result_dataframe(QUERY_ID_MORPHO_LIQS)
blast_flows_df = dune.get_latest_result_dataframe(QUERY_ID_BLAST_FLOWS)
print("✅ Successfully fetched all dataframes from Dune.")

# --- 3. DATA PROCESSING ---
print("Processing and cleaning data...")
# Convert all time/date columns to datetime objects for proper plotting.
price_vol_df['minute'] = pd.to_datetime(price_vol_df['minute'])
weth_drain_df['minute'] = pd.to_datetime(weth_drain_df['minute'])
morpho_liqs_df['day'] = pd.to_datetime(morpho_liqs_df['day'])
blast_flows_df['day'] = pd.to_datetime(blast_flows_df['day'])

# Resample minute-by-minute volume to hourly sums to reduce clutter in the volume chart.
hourly_volume_df = price_vol_df.set_index('minute')['volume_usd'].resample('1H').sum().reset_index()

# Prepare Blast data for plotting (use absolute values for withdrawals).
blast_flows_df['gross_withdrawals_abs'] = blast_flows_df['gross_withdrawals'].abs()

# Align the daily liquidation data with the main plot's time axis.
# This ensures that days with zero liquidations are still plotted as zero.
# 1. Create a complete, daily date range that matches the main plot's visible range
date_range = pd.date_range(start=price_vol_df['minute'].min(), end=price_vol_df['minute'].max(), freq='D')
scaffold_df = pd.DataFrame(date_range, columns=['day'])
# 2. Merge liquidation data onto this complete daily "scaffold"
morpho_aligned_df = pd.merge(scaffold_df, morpho_liqs_df, on='day', how='left')
# 3. Fill the days with no liquidations with 0
morpho_aligned_df['total_usd_liquidated'] = morpho_aligned_df['total_usd_liquidated'].fillna(0)


# --- 4. VISUALIZATION ---
print("🎨 Creating visualizations...")

# --- CHART 1: The Mainnet Crisis (Multi-panel plot) ---
# A figure with 3 vertically stacked subplots will be created.
fig_mainnet = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.08,
    row_heights=[0.5, 0.25, 0.25],
    specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]],
    subplot_titles=(
        "<b>Price De-peg vs. WETH Liquidity Drain (Balancer Pool)</b>",
        "<b>DEX Trading Volume (Hourly)</b>",
        "<b>On-Chain Liquidations (Morpho Blue)</b>"
    )
)

# PLOT 1A: Price Ratio
fig_mainnet.add_trace(go.Scatter(x=price_vol_df['minute'], y=price_vol_df['price_ratio_weth'], name='ezETH/WETH Price Ratio', line=dict(color='#4A55A2', width=2)), row=1, col=1)
fig_mainnet.add_hline(y=1.0, line_dash="dash", line_color="grey", row=1, col=1, annotation_text="Peg", annotation_position="bottom right")

# PLOT 1B: WETH Drain
fig_mainnet.add_trace(go.Scatter(x=weth_drain_df['minute'], y=weth_drain_df['cumulative_weth_drained'], name='Cumulative WETH Drained', line=dict(color='#C51605', width=2, dash='dot')), row=1, col=1, secondary_y=True)

# PLOT 2: Trading Volume
fig_mainnet.add_trace(go.Bar(
    x=hourly_volume_df['minute'],
    y=hourly_volume_df['volume_usd'],
    name='Hourly Trading Volume',
    marker_color='#A4D0A4',
    hovertemplate='<b>Date</b>: %{x}<br><b>Volume</b>: $%{y:,.0f}<extra></extra>'
), row=2, col=1)

# PLOT 3: Morpho Liquidations
fig_mainnet.add_trace(go.Bar(
    x=morpho_aligned_df['day'],   
    y=morpho_aligned_df['total_usd_liquidated'], 
    name='Daily Liquidation Value',
    marker_color='#E55807',
    hovertemplate='<b>Date</b>: %{x|%Y-%m-%d}<br><b>Liquidated</b>: $%{y:,.0f}<extra></extra>'
), row=3, col=1)

# --- Layout and Formatting for Mainnet Chart ---
fig_mainnet.update_layout(
    title=dict(
        text="<b>Anatomy of the ezETH De-Peg Crisis: Ethereum Mainnet (April 24, 2024)</b>",
        y=0.97, 
        x=0.5,
        xanchor='center',
        yanchor='top',
        font=dict(
            size=23,
            color="#2c3e50"
        )
    ),
    legend=dict(
        orientation="v", yanchor="top", y=0.9, xanchor="left", x=1.02,          
        bgcolor="rgba(255,255,255,0.6)", bordercolor="lightgrey", borderwidth=1
    ),
    height=900, template='plotly_white', margin=dict(t=125, b=60, l=60, r=60) 
)

# --- Subplot Title and Axis Label Enhancement ---
fig_mainnet.update_annotations(font_size=17)
fig_mainnet.update_yaxes(title_font=dict(size=14), tickfont=dict(size=12))
fig_mainnet.update_xaxes(title_font=dict(size=14), tickfont=dict(size=12))
fig_mainnet.update_yaxes(title_text="<b>Price Ratio</b>", row=1, col=1, secondary_y=False, range=[0.8, 1.05])
fig_mainnet.update_yaxes(title_text="<b>WETH Drained</b>", row=1, col=1, secondary_y=True)
fig_mainnet.update_yaxes(title_text="<b>Volume (USD)</b>", row=2, col=1)
fig_mainnet.update_yaxes(title_text="<b>Liquidated (USD)</b>", row=3, col=1)
fig_mainnet.update_xaxes(title_text="<b>Date</b>", row=3, col=1)

# --- CHART 2: The Cross-Chain Contagion (Blast L2) ---
fig_blast = make_subplots(rows=1, cols=1)
fig_blast.add_trace(go.Scatter(x=blast_flows_df['day'], y=blast_flows_df['gross_deposits'], name='Daily Deposits', line=dict(color='#4A55A2')))
fig_blast.add_trace(go.Scatter(x=blast_flows_df['day'], y=blast_flows_df['gross_withdrawals_abs'], name='Daily Withdrawals', line=dict(color='#C51605')))
fig_blast.add_trace(go.Scatter(x=blast_flows_df['day'], y=blast_flows_df['net_flow_ezeth'], name='Net Daily Flow', line=dict(color='grey', dash='dash')))
fig_blast.add_hline(y=0, line_dash="dot", line_color="black")

fig_blast.update_layout(
    title=dict(text="<b>Cross-Chain Contagion: Bank Run on the Blast L2 ezETH Vault</b>", y=0.95, x=0.5, xanchor='center', yanchor='top', font=dict(size=23, color="#2c3e50")),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    height=600, template='plotly_white', margin=dict(t=100, b=60, l=70, r=50)
)
fig_blast.update_yaxes(title_text="<b>ezETH Token Amount</b>", title_font=dict(size=14), tickfont=dict(size=12))
fig_blast.update_xaxes(title_text="<b>Date</b>", title_font=dict(size=14), tickfont=dict(size=12))

# --- 5. OUTPUT ---
output_dir = "outputs"
module_output_dir = os.path.join(output_dir, "module2_depeg_analysis")
if not os.path.exists(module_output_dir):
    os.makedirs(module_output_dir)
    print(f"📁 Created output directory: {module_output_dir}")

print("Displaying charts and saving to HTML...")
fig_mainnet.show()
fig_blast.show()

mainnet_html_path = os.path.join(module_output_dir, "ezETH_depeg_mainnet_analysis.html")
blast_html_path = os.path.join(module_output_dir, "ezETH_depeg_blast_contagion.html")
fig_mainnet.write_html(mainnet_html_path)
fig_blast.write_html(blast_html_path)

print(f"✅ Interactive HTML charts saved to '{module_output_dir}'.")