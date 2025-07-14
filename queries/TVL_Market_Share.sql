-- Query Name: LRT Historical TVL and Market Share (ID: 5284229)
-- Description: Tracks the daily Total Value Locked (TVL) and circulating token supply
--              for the top 5 Liquid Restaking Token (LRT) protocols.

WITH historical_data AS (
-- This CTE builds a complete daily history for each LRT protocol.
    WITH lrt_tokens AS (
    -- Define the LRT protocols, their ticker, and their token contract address
        SELECT * FROM (VALUES
            (TIMESTAMP '2023-07-10', 'eETH', 0x35fA164735182de50811E8e2E824cFb9B6118ac2),
            (TIMESTAMP '2024-01-31', 'pufETH', 0xD9A442856C234a39a81a089C06451EBAa4306a72),
            (TIMESTAMP '2023-12-10', 'rsETH', 0xA1290d69c65a6FE4DF752f95823FAe25cb99e5a7),
            (TIMESTAMP '2023-12-05', 'ezETH', 0xbf5495efe5db9ce00f80364c8b423567e58d2110),
            (TIMESTAMP '2024-01-26', 'rswETH', 0xFAe103DC9cf190eD75350761e95403b7b8aFa6c0)
        ) AS t (inception_date, protocol, lrt_address)
    ),
    dates AS (
     -- Generate a continuous series of dates from the earliest LRT inception to now.
        SELECT CAST(d AS TIMESTAMP) AS "day"
        FROM UNNEST(SEQUENCE((SELECT MIN(inception_date) FROM lrt_tokens), CAST(NOW() AS DATE), INTERVAL '1' DAY)) AS t(d)
    ),
    daily_supply_changes AS (
    -- Calculate the net change in supply for each LRT token each day by tracking mints (transfers from address 0x0)
        -- and burns (transfers to address 0x0).
        SELECT contract_address, CAST(evt_block_time AS DATE) AS "day", SUM(CASE WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value ELSE -value END) / 1e18 AS supply_change
        FROM erc20_ethereum.evt_Transfer
        WHERE contract_address IN (SELECT lrt_address FROM lrt_tokens) AND ("from" = 0x0000000000000000000000000000000000000000 OR "to" = 0x0000000000000000000000000000000000000000)
        GROUP BY 1, 2
    ),
    daily_total_supply AS (
     -- Calculate the running total (cumulative sum) of the supply for each protocol over time.
        SELECT d.day, l.protocol, l.lrt_address, SUM(COALESCE(c.supply_change, 0)) OVER (PARTITION BY l.protocol ORDER BY d.day) AS total_supply
        FROM dates d CROSS JOIN lrt_tokens l
        LEFT JOIN daily_supply_changes c ON c.contract_address = l.lrt_address AND c.day = CAST(d.day AS DATE)
        WHERE d.day >= l.inception_date
    )
     -- Join with daily ETH prices to calculate TVL in USD. Assumes a 1:1 peg between LRT and ETH.
    SELECT s.day, s.protocol, s.total_supply, s.total_supply * p.price AS tvl_usd
    FROM daily_total_supply s
    -- Assume that the LRTs are pegged 1:1 with ETH:
    JOIN prices.usd_daily p ON p.symbol = 'ETH' AND p.day = s.day
    WHERE s.total_supply > 0
)

-- Final SELECT to get the max daily supply and TVL for each protocol.
SELECT
    day,
    protocol,
    MAX(total_supply) AS total_supply, 
    MAX(tvl_usd) AS tvl_usd 
FROM historical_data
GROUP BY 1, 2
ORDER BY 1, 2;