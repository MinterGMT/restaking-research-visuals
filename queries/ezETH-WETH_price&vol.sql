-- Title: ezETH/WETH Price and Volume (Balancer v2) (ID: 5299669)
-- Description: Calculates the minute-by-minute price ratio of ezETH to WETH and the
--              corresponding USD trading volume in the primary Balancer V2 liquidity pool
--              during the de-peg crisis window.

-- Define token and pool addresses for clarity.
-- ezETH: 0xbf5495Efe5DB9ce00f80364C8B423567e58d2110
-- WETH: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2

SELECT
    date_trunc('minute', block_time) AS minute,
    
    -- Calculate the direct price ratio of ezETH to WETH from trade data.
    -- This is more accurate than relying on a price oracle during a de-peg.
    AVG(
        CASE
             -- Case 1: Selling ezETH for WETH. Price = WETH received / ezETH sold.
            WHEN token_sold_address = 0xbf5495Efe5DB9ce00f80364C8B423567e58d2110 THEN token_bought_amount / token_sold_amount
            -- Case 2: Buying ezETH with WETH. Price = WETH spent / ezETH bought
            WHEN token_bought_address = 0xbf5495Efe5DB9ce00f80364C8B423567e58d2110 THEN token_sold_amount / token_bought_amount
        END
    ) AS price_ratio_weth,

    -- Sum the USD amount of all trades in the minute.
    SUM(amount_usd) AS volume_usd
FROM dex.trades
WHERE
    blockchain = 'ethereum'
    AND project = 'balancer'
    AND version = '2'
     -- Focus on the 72-hour window around the main de-peg event.
    AND block_time BETWEEN TIMESTAMP '2024-04-23 00:00' AND TIMESTAMP '2024-04-26 00:00'
    -- Filter for trades between ezETH and WETH.
    AND (
        (token_bought_address = 0xbf5495Efe5DB9ce00f80364C8B423567e58d2110 AND 
        token_sold_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2)
        OR
        (token_bought_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 AND 
        token_sold_address = 0xbf5495Efe5DB9ce00f80364C8B423567e58d2110)
    )
GROUP BY 1
ORDER BY 1 ASC
