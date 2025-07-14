--===============================================================================
--===============================================================================

-- IDs identified with this query:
-- SELECT
--    id,
--    marketParams
-- FROM morpho_blue_ethereum.morphoblue_evt_createmarket
-- WHERE
    -- Look for markets featuring the ezETH address
--    marketParams LIKE '%bf5495efe5db9ce00f80364c8b423567e58d2110%'
--    AND evt_block_time < timestamp '2024-04-23 00:00:00' -- Filter for markets created before the crisis

-- 0x093d5b432aace8bf6c4d67494f4ac2542a499571ff7a1bcc9f8778f3200d457d
-- 0x355c9a4c12f60a10ab3b68507bfab21bd6913182037ffe25d94dabffea45429f
-- 0x49bb2d114be9041a787432952927f6f144f05ad3e83196a7d062f374ee11d0ee
-- 0x459687783a68f4cf4e230618f88ce135d1cd459a850f6496751c2a9c1c6e852e

--===============================================================================
--===============================================================================

-- Title: Total ezETH Liquidations on Morpho Blue per Day (ID: 5323305)
-- Description: Aggregates all liquidation events where ezETH was the collateral
--              on the Morpho Blue protocol during the crisis period.

WITH liquidations_detailed AS (
    SELECT
        evt_block_time,
        repaidAssets / 1e18 AS amount_repaid_usd, -- The amount of debt repaid by the liquidator.
        seizedAssets / 1e18 AS ezeth_seized -- The amount of ezETH collateral seized.
    FROM morpho_blue_ethereum.morphoblue_evt_liquidate
    WHERE
          -- Filter for the specific market IDs identified as ezETH collateral markets.
        id IN (
            0x093d5b432aace8bf6c4d67494f4ac2542a499571ff7a1bcc9f8778f3200d457d,
            0x355c9a4c12f60a10ab3b68507bfab21bd6913182037ffe25d94dabffea45429f,
            0x49bb2d114be9041a787432952927f6f144f05ad3e83196a7d062f374ee11d0ee,
            0x459687783a68f4cf4e230618f88ce135d1cd459a850f6496751c2a9c1c6e852e
        )
        -- Analyze a wider window to capture any lead-up or aftermath.
        AND evt_block_time BETWEEN timestamp '2024-04-21 00:00' AND timestamp '2024-04-27 00:00'
)

-- Now, select from the CTE to perform the final aggregation
SELECT
    date_trunc('day', ld.evt_block_time) as day,
    SUM(ld.ezeth_seized) as total_ezeth_liquidated,
    -- Join with price data to get the USD value of the liquidated collateral.
    SUM(ld.ezeth_seized * p.price) as total_usd_liquidated,
    SUM(ld.amount_repaid_usd) as total_debt_repaid_usd,
    COUNT(*) as number_of_liquidations
FROM liquidations_detailed ld -- aliasing the CTE as 'ld'
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', ld.evt_block_time)
    AND p.contract_address = 0xbf5495Efe5DB9ce00f80364C8B423567e58d2110 -- Price of ezETH
GROUP BY 1
HAVING SUM(ld.ezeth_seized) > 0 -- Only show days where liquidations actually occurred
ORDER BY 1 ASC
