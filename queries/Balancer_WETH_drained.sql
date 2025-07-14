-- Title: Cumulative Net Flow of WETH from Balancer Pool during ezETH De-peg (ID: 5299808)

-- Define token addresses for clarity and easy modification
WITH tokens AS (
    SELECT
        0xbf5495Efe5DB9ce00f80364C8B423567e58d2110 AS ezeth_address,
        0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 AS weth_address
),

-- Step 1: For each minute, calculate the net amount of WETH that was drained from the pool
minute_flows AS (
    SELECT
        date_trunc('minute', block_time) as minute,
        -- Calculate the net WETH leaving the pool.
        -- If WETH is bought (swappers sell ezETH), it's a positive drain.
        -- If WETH is sold (swappers buy ezETH), it's a negative drain (an inflow).
        SUM(
            CASE
                WHEN token_bought_address = (SELECT weth_address FROM tokens) THEN token_bought_amount
                WHEN token_sold_address = (SELECT weth_address FROM tokens) THEN -token_sold_amount
                ELSE 0
            END
        ) as net_weth_drained_from_pool
    FROM dex.trades
    WHERE
        project = 'balancer'
        AND version = '2'
        AND block_time BETWEEN TIMESTAMP '2024-04-23 00:00:00' AND TIMESTAMP '2024-04-26 00:00:00'
        -- Filter for swaps between the two tokens of interest.
        AND (
            (token_bought_address = (SELECT ezeth_address FROM tokens) AND token_sold_address = (SELECT weth_address FROM tokens))
            OR
            (token_bought_address = (SELECT weth_address FROM tokens) AND token_sold_address = (SELECT ezeth_address FROM tokens))
        )
    GROUP BY 1
)

-- Calculate the running total (cumulative sum) of the net WETH drain 
SELECT
    minute,
    SUM(net_weth_drained_from_pool) OVER (ORDER BY minute ASC) as cumulative_weth_drained
FROM minute_flows
ORDER BY 1 ASC
