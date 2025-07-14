-- Query: ezETH De-Peg Event on ZeroLend (Blast) (ID: 5306342)
-- This query calculates all historical flows for the ezETH vault on Blast, 
-- in both native token and USD terms.

-- The address of the ZeroLend Pool Proxy contract on Blast
WITH zerolend_proxy_address AS (
    SELECT 0xA7DB0F3C15e6e405f54caF3C54BEA7A8 AS address
),
-- The address of the ezETH token contract on Blast
ezeth_blast_address AS (
    SELECT 0x2416092f143378750bb29b79ed961ab195cceea5 AS address
),

-- Isolate deposit events from the 'Supply' event log
deposits AS (
    SELECT
        evt_block_time,
        amount / 1e18 AS deposit_amount
    FROM
        zerolend_blast.PoolProxy_evt_Supply
    WHERE
        reserve = (SELECT address FROM ezeth_blast_address)
),

-- Isolate withdrawal events from the 'Withdraw' event log
withdrawals AS (
    SELECT
        evt_block_time,
        amount / 1e18 AS withdrawal_amount
    FROM
        zerolend_blast.PoolProxy_evt_Withdraw
    WHERE
        reserve = (SELECT address FROM ezeth_blast_address)
),

-- Union all transactions and aggregate flows by day.
daily_flows AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        SUM(deposit_amount) AS gross_deposits,
        0 AS gross_withdrawals
    FROM deposits
    GROUP BY 1

    UNION ALL

    SELECT
        date_trunc('day', evt_block_time) AS day,
        0 AS gross_deposits,
        SUM(withdrawal_amount) AS gross_withdrawals
    FROM withdrawals
    GROUP BY 1
),

-- Aggregate the raw daily flows before joining to prices
aggregated_daily_flows AS (
    SELECT
        day,
        SUM(gross_deposits) AS gross_deposits,
        SUM(gross_withdrawals) AS gross_withdrawals
    FROM daily_flows
    GROUP BY 1
),

-- STEP 1: Calculate the full historical data, including USD values and the running cumulative supply
all_history AS (
    SELECT
        adf.day,
        adf.gross_deposits,
        adf.gross_withdrawals,
        p.price AS price_usd,
        -- Calculate USD values
        adf.gross_deposits * p.price AS gross_deposits_usd,
        adf.gross_withdrawals * p.price AS gross_withdrawals_usd,
        -- Calculate net flows in both units
        (adf.gross_deposits - adf.gross_withdrawals) AS net_flow_ezeth,
        (adf.gross_deposits - adf.gross_withdrawals) * p.price AS net_flow_usd,
        -- Calculate the cumulative supply on the full history
        SUM(adf.gross_deposits - adf.gross_withdrawals) OVER (ORDER BY adf.day ASC) AS cumulative_supply_ezeth
    FROM aggregated_daily_flows AS adf
    LEFT JOIN prices.usd_daily p ON p.blockchain = 'blast'
        AND p.contract_address = (SELECT address FROM ezeth_blast_address)
        AND p.day = adf.day
)

-- STEP 2: Now, select from the pre-calculated history and apply the desired date filter for display
SELECT
    day,
    gross_deposits,
    gross_withdrawals,
    gross_deposits_usd,
    gross_withdrawals_usd,
    net_flow_ezeth,
    net_flow_usd,
    cumulative_supply_ezeth,
    cumulative_supply_ezeth * price_usd AS cumulative_supply_usd
FROM
    all_history
WHERE
    day BETWEEN TIMESTAMP '2024-04-15' AND TIMESTAMP '2024-05-05'
ORDER BY
    day ASC
