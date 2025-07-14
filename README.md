# The Re-Staked Economy: An Empirical Analysis of Systemic Risk

This repository contains the source code, queries, and key interactive data visualizations for the Master's thesis of the same name.

The project aims to empirically measure key risk indicators within the Liquid Restaking Token (LRT) market, including operator centralization and financial instability.

---

### Repository Structure

*   **/src**: Contains all Python scripts used for analysis and visualization.
*   **/queries**: Contains all of the core SQL queries executed on Dune Analytics.
*   The `.html` files in this root directory are the interactive data visualizations.

---

### Interactive Visualizations

Click the links below to explore the interactive charts in a new browser tab. For the best experience, view on a desktop monitor.

**1. AVS Operator Concentration Analysis**
*   A comprehensive comparison of the Herfindahl-Hirschman Index (HHI) for the overall operator market versus the individual security markets of the top 30 Actively Validated Services (AVSs).
*   **[View Interactive Chart](https://mintergmt.github.io/restaking-research-visuals/avs_hhi_concentration_chart.html)**

**2. Anatomy of the ezETH De-Peg Crisis (Ethereum Mainnet)**
*   A multi-panel forensic analysis of the April 24, 2024 de-pegging event, correlating price, liquidity, volume, and on-chain liquidations.
*   **[View Interactive Chart](https://mintergmt.github.io/restaking-research-visuals/ezETH_depeg_mainnet_analysis.html)**

**3. Cross-Chain Contagion on Blast L2**
*   An analysis of the daily flows into and out of the primary ezETH vault on the Blast network, illustrating the "bank run" effect caused by the mainnet crisis.
*   **[View Interactive Chart](https://mintergmt.github.io/restaking-research-visuals/ezETH_depeg_blast_contagion.html)**

*These visualizations are supplemetary materials for the full research document*.

---

### How to Run This Project

To regenerate the outputs, clone the repository, set up the Python environment using `requirements.txt`, create a `.env` file with your `DUNE_API_KEY`, and run the scripts located in the `/src` directory.