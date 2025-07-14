
# This script analyzes the "nested" concentration of stake within the security markets
# of individual Actively Validated Services (AVSs). It uses a parameterized Dune query
# to fetch the operator set for each AVS and calculates concentration metrics.
# To respect the free tier of Dune, the script uses retry logic 
# with a cool-down period to handle standard API rate limits and errors. 

import os
import pandas as pd
from dotenv import load_dotenv
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import numpy as np
import time

# --- 1. SETUP ---
print("🚀 Initializing AVS Concentration Analysis...")
load_dotenv()
dune = DuneClient(os.getenv("DUNE_API_KEY"))
print("✅ Dune client initialized.")

# --- HHI & Gini Functions ---
def calculate_hhi(shares):
    if shares.sum() == 0: return 0
    percentages = (shares / shares.sum()) * 100
    return (percentages**2).sum()

def calculate_gini(arr):
    arr = np.asarray(arr, dtype=np.float64)
    arr = arr[arr >= 0]
    if arr.size == 0 or np.sum(arr) == 0: return np.nan
    sorted_arr = np.sort(arr)
    n = len(sorted_arr)
    index = np.arange(1, n + 1)
    numerator = np.sum((2 * index - n - 1) * sorted_arr)
    denominator = n * np.sum(sorted_arr)
    if denominator == 0: return np.nan
    return numerator / denominator

# --- 2. CONFIGURATION ---
# The Query ID of the MASTER parameterized query on Dune
# This query accepts an AVS address as a parameter.
MASTER_AVS_QUERY_ID = 5391472

# Dictionary of Top 30 AVSs to analyze, mapping their name to their contract address.
AVS_TO_ANALYZE = {
    "EigenDA": "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0",
    "Eoracle": "0x23221c5bb90c7c57ecc1e75513e2e4257673f0ef",
    "Witness Chain": "0xd25c2c5802198cb8541987b73a8db4c9bcae5cc7",
    "Lagrange ZK Prover Network": "0x22cac0e6a1465f043428e8aef737b3cb09d0eeda",
    "OpenLayer": "0xf7fcff55d5fdaf2c3bbeb140be5e62a2c7d26db3",
    "Cyber": "0x1f2c296448f692af840843d993ffc0546619dcdb",
    "Hyperlane": "0xe8e59c6c8b56f2c178f63bcfc4ce5e5e2359c8fc",
    "UniFi": "0x2d86e90ed40a034c753931ee31b1bd5e1970113d",
    "Lagrange Committees": "0x35f4f28a8d3ff20eed10e087e8f96ea2641e6aa2",
    "Predicate": "0xf6f4a30eef7cf51ed4ee1415fb3bfdaf3694b0d2",
    "Aligned": "0xef2a435e5ee44b2041100ef8cbc8ae035166606c",
    "Vision: Validator AVS by ETHGas": "0x6201bc0a699e3b10f324204e6f8ecdd0983de227",
    "Ungate InfiniRoute": "0xb3e069fd6dda251acbde09eda547e0ab207016ee",
    "Brevis": "0x0328635ba5ff28476118595234b5b7236b906c0b",
    "ARPA Network": "0x1de75eaab2df55d467494a172652579e6fa4540e",
    "Automata": "0xe5445838c475a2980e6a88054ff1514230b83aeb",
    "Mev-commit": "0xbc77233855e3274e1903771675eb71e602d9dc2e",
    "RedStone": "0x6f943318b05ad7c6ee596a220510a6d64b518dd8",
    "Aethos": "0x07e26bf8060e33fa3771d88128b75493750515c1",
    "Skate": "0xfc569b3b74e15cf48aa684144e072e839fd89380",
    "Ava Protocol": "0x18343aa10e3d2f3a861e5649627324aead987adf",
    "AltLayer": "0x71a77037870169d47aad6c2c9360861a4c0df2bf",
    "Xterio Mach": "0x6026b61bdd2252160691cb3f6005b6b72e0ec044",
    "GoPlus": "0xa3f64d3102a035db35c42a9001bbc83e08c7a366",
    "zScore": "0xc9e94bf890c9b4f11685d576bc65b08e0e87556f",
    "K3-Labs": "0x83742c346e9f305dca94e20915ab49a483d33f3e",
    "Opacity": "0xce06c5fe42d22ff827a519396583fd9f5176e3d3",
    "Mishti Network": "0x42f15f9e4df4994317453477e80e24797cc1a929",
    "Chainbase Network": "0xb73a87e8f7f9129816d40940ca19dfa396944c71",
    "Omni": "0xed2f4d90b073128ae6769a9a8d51547b1df766c8"
    
}

# --- 3. ANALYSIS LOOP ---
all_avs_results = []

# Iterate through each AVS defined in the configuration dictionary.
for avs_name, avs_address in AVS_TO_ANALYZE.items():
    print(f"\nAnalyzing AVS: {avs_name}...")
    
    # Define the query with its parameter. The parameter name "avs_address"
    # must match the one defined in the Dune query editor.
    query = QueryBase(
        query_id=MASTER_AVS_QUERY_ID,
        params=[
                QueryParameter.text_type(name="avs_address", value=avs_address)
            ]
        ) 
    try:
        # Execute the parameterized query.
        avs_df = dune.run_query_dataframe(query=query)

    except Exception as e:
        # Implement a retry mechanism for API rate limiting (HTTP 429)
        if "429" in str(e):
            print(" Rate limit hit. Waiting for a 10-second cool-down...")
            time.sleep(10)
            try:
                # Second attempt after waiting
                print(" Retrying...")
                avs_df = dune.run_query_dataframe(query=query, performance="large")
            except Exception as e2:
                print(f"❌ Error on retry for {avs_name}: {e2}")
                continue # Skip to the next AVS if the retry also fails
        else:
            # If it's a different kind of error, report it and skip to the next AVS
            print(f"❌ A non-rate-limit error occurred for {avs_name}: {e}")
            continue 
        
    print(f"✅ Successfully fetched data for {avs_name}. Found {len(avs_df)} operators.")
        
    try:
        # Perform the concentration calculations.
        delegated_col = 'USD value Delegated'

        avs_df[delegated_col] = pd.to_numeric(avs_df[delegated_col], errors='coerce')
        num_operators = len(avs_df)
        total_stake_usd = avs_df[delegated_col].sum()
        hhi = calculate_hhi(avs_df[delegated_col])
        gini = calculate_gini(avs_df[delegated_col].values)
        
        # Store the results in a list of dictionaries.
        all_avs_results.append({
            "Market": avs_name,
            "Number of Operators": num_operators,
            "Total Delegated USD (Proxy)": f"${total_stake_usd:,.0f}",
            "HHI (Proxy)": f"{hhi:,.2f}",
            "Gini (Proxy)": f"{gini:.4f}"
        })
        
    except Exception as e:
        print(f"❌ Error analyzing {avs_name}: {e}")
    
    # Pause between API calls to respect rate limits.
    print("Pausing for 3 seconds to respect API rate limits...")
    time.sleep(3)

# --- 4. FINAL OUTPUT ---
output_dir = "outputs"
module_output_dir = os.path.join(output_dir, "module1.5_AVS_concentration")
if not os.path.exists(module_output_dir):
    os.makedirs(module_output_dir)
    print(f"\n📁 Created output directory: {module_output_dir}")

if all_avs_results:
    summary_df = pd.DataFrame(all_avs_results)
    print("\n\n==================================================")
    print("   AVS Operator Concentration Analysis Summary")
    print("==================================================")
    print(summary_df.to_string(index=False))
    
    # Save the final results to a CSV file for later use in visualization.
    summary_path = os.path.join(module_output_dir, 'avs_concentration_summary.csv')
    summary_df.to_csv(summary_path, index=False)
    print(f"\n✅ AVS summary saved to {summary_path}")