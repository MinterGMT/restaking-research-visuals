-- Title: Total ezETH Trading Volume Across Major DEXs (ID: 5301668)

SELECT
    date_trunc('day', block_time) as day,
    project,
    SUM(amount_usd) as total_volume_usd
FROM dex.trades
WHERE
    blockchain = 'ethereum'
    AND block_time BETWEEN TIMESTAMP '2024-04-20 00:00' AND TIMESTAMP '2024-04-28 00:00'
    AND (
        token_bought_address = 0xbf5495Efe5DB9ce00f80364C8B423567e58d2110 -- ezETH
        OR
        token_sold_address = 0xbf5495Efe5DB9ce00f80364C8B423567e58d2110 -- ezETH
    )
GROUP BY 1, 2
ORDER BY 1, 3 DESC
