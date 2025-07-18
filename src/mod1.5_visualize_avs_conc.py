
# This script loads the AVS concentration data from the CSV generated by
# module1_B_analysis.py and creates a comprehensive bar chart to visualize
# the HHI scores of different AVS security markets.

import pandas as pd
import plotly.graph_objects as go
import os

# --- 1. SETUP ---
print("📊 Initializing AVS Concentration Visualization Script...")

# Define the input CSV file and the output directory
input_csv = "avs_concentration_summary.csv"
output_dir = "module1.5_outputs"

# --- 2. DATA LOADING AND PREPARATION ---
try:
    # Read the data from the CSV generated by the previous script.
    avs_summary_df = pd.read_csv(input_csv, thousands=',')
    print(f"✅ Successfully loaded data from '{input_csv}'.")
except FileNotFoundError:
    print(f"❌ Error: The file '{input_csv}' was not found.")
    print("Please run the 'module1.5_avs_analysis.py' script first to generate the data.")
    exit()

# Manually add the "Overall Market" data from Module 1A for comparison.
overall_market_data = pd.DataFrame([{
    "Market": "Overall Market",
    "HHI (Proxy)": 503.92  # Value from Module 1A 
}])

# Combine theAVS data with the overall market data.
plot_df = pd.concat([avs_summary_df, overall_market_data], ignore_index=True)
# Ensure HHI is numeric and sort by HHI for clean visual presentation.
plot_df['HHI (Proxy)'] = pd.to_numeric(plot_df['HHI (Proxy)'], errors='coerce')
plot_df = plot_df.sort_values(by='HHI (Proxy)', ascending=True).dropna(subset=['HHI (Proxy)'])

# --- 3. VISUALIZATION ---
print("🎨 Creating AVS concentration chart...")
fig_avs = go.Figure()

# Add the horizontal bars for each AVS market
fig_avs.add_trace(go.Bar(
    y=plot_df['Market'],
    x=plot_df['HHI (Proxy)'],
    orientation='h',
    text=plot_df['HHI (Proxy)'].round(0), # Display HHI value on bars.
    textposition='auto',
    marker=dict(
        color=plot_df['HHI (Proxy)'], # Color bars by HHI value for emphasis.
        colorscale='Viridis', 
        showscale=True,
        colorbar=dict(title='HHI Score')
    )
))

# Add the vertical threshold lines for concentration levels
fig_avs.add_vline(x=1500, line_dash="dash", line_color="grey", annotation_text="Moderately Concentrated",
    annotation_font_size=14)
fig_avs.add_vline(x=2500, line_dash="dash", line_color="darkgrey", annotation_text="Highly Concentrated",
    annotation_font_size=14, annotation_y=0.95)

# --- Polished Layout ---
fig_avs.update_layout(
    title=dict(text="<b>Operator Concentration: Overall Market vs. Individual AVS Security Markets</b>", y=0.97, x=0.5, font=dict(size=22)),
    xaxis_title="Herfindahl-Hirschman Index (HHI) - Proxy Value",
    yaxis_title="Security Market",
    template='plotly_white',
    width=1200,
    height=1000,
    margin=dict(l=250, r=50, t=100, b=50), # Adjust margins for long labels

    font=dict(family="Arial", size=14),
    
    # Explicitly define title fonts to be larger than labels
    xaxis=dict(
        title_font=dict(size=16), # Axis title font
        tickfont=dict(size=12)    # Tick label font
    ),
    yaxis=dict(
        autorange="reversed",
        title_font=dict(size=16), # Axis title font
        tickfont=dict(size=14)    # Keep AVS names large for readability
    )
)
fig_avs.update_traces(
    textposition="inside",
    insidetextanchor="end", # Align text to the end of the bar
    textfont_size=12,
    textangle=0
)

# --- 4. OUTPUT ---
# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"📁 Created output directory: {output_dir}")

# Save the interactive chart to an HTML file.
avs_html_path = os.path.join(output_dir, "avs_hhi_concentration_chart.html")
fig_avs.write_html(avs_html_path)
fig_avs.show()

print(f"✅ AVS concentration chart saved to '{avs_html_path}'")