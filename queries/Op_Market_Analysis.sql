-- Query Name: Operator Market Analysis (Query for Table) (ID: 5292464)
-- Description: This query provides a complete breakdown of all ACTIVE EigenLayer operators, labeled by their
--              affiliated LRT protocol. It is the primary data source for centralization analysis.

WITH
-- Part 1: Strategies lookup table for mapping strategy contracts to underlying LSTs.
strategies AS (
    SELECT 
        strategy AS strategy_address,
        token AS underlying_token
    FROM eigenlayer_ethereum.strategy_and_token_metadata_latest
),
-- Part 2: All delegation events (increases and decreases) from EigenLayer contracts.
delegation_events AS (
    SELECT operator, strategy, shares, 'increase' AS action
    FROM eigenlayer_ethereum.DelegationManager_evt_OperatorSharesIncreased
    UNION ALL
    SELECT operator, strategy, shares, 'decrease' AS action
    FROM eigenlayer_ethereum.DelegationManager_evt_OperatorSharesDecreased
),
-- Part 3: Operator metadata for clean names.
operators AS (
    SELECT operator_name, operator_contract_address
    FROM dune.dune.dataset_dataset_eigenlayer_operator_metadata
),
-- Part 4: Join these three sources to create a "base layer" of all actions.
base_actions AS (
    SELECT
        e.operator, o.operator_name, e.action, e.shares, s.underlying_token
    FROM delegation_events e
    JOIN strategies s ON e.strategy = s.strategy_address
    LEFT JOIN operators o ON e.operator = o.operator_contract_address
),
-- Part 5: Aggregate actions to get the net delegated shares for each operator-asset pair.
net_delegations AS (
    SELECT
        operator, operator_name, underlying_token,
        SUM(CASE WHEN action = 'increase' THEN (shares/1e18) ELSE -(shares/1e18) END) AS net_shares
    FROM base_actions GROUP BY 1, 2, 3
),
-- Part 6: Get latest USD prices for each asset to calculate total value.
latest_prices AS (
    WITH ranked_prices AS (
        SELECT contract_address, price, ROW_NUMBER() OVER(PARTITION BY contract_address ORDER BY minute DESC) as rn
        FROM prices.usd WHERE minute > NOW() - INTERVAL '3' DAY
    )
    SELECT contract_address, price AS usd_price FROM ranked_prices WHERE rn = 1
),
-- Part 7: Aggregate by operator to get total delegated value in USD and ETH.
operator_final_breakdown AS (
    SELECT
        COALESCE(operator_name, 'Unnamed') AS operator_name,
        operator,
        SUM(d.net_shares * p.usd_price) AS total_usd_delegated,
        SUM(d.net_shares * p.usd_price / weth.usd_price) as total_eth_delegated
    FROM net_delegations d
    LEFT JOIN latest_prices p ON d.underlying_token = p.contract_address
    CROSS JOIN latest_prices weth WHERE weth.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    GROUP BY 1, 2
),
-- Part 8: Get a unique list of all currently active operators.
active_operators AS (
    SELECT DISTINCT operator
    FROM eigenlayer_ethereum.avs_operator_registration_status_latest
    WHERE status = 1
)

-- Final SELECT with Labeling, Ranking, and the ACTIVE operator filter.
-- This is the final output used by the Python analysis script.
SELECT
    RANK() OVER (ORDER BY b.total_usd_delegated DESC) AS "Rank",
    b.operator_name AS "Operator Name",
    '0x' || to_hex(b.operator) AS "Operator Address",
    GREATEST(b.total_usd_delegated, 0) AS "USD value Delegated",
    GREATEST(b.total_eth_delegated, 0) AS "ETH value Delegated",
     -- Manually label operators by their known LRT protocol affiliation based on their contract addresses.
    CASE
        WHEN b.operator IN (0x67943ae8e07bfc9f5c9a90d608f7923d9c21e051, 0xfb487f216ca24162119c0c6ae015d680d7569c2f, 0x4bd479a34450d0cb1f5ef16a877bee47e1e4cdb9, 0xea50bb6735703422d2e053452f1f28bff17da51f, 0x5b9b3cf0202a1a3dc8f527257b7e6002d23d8c85, 0x5d4b5ef127c545e5bf8e247f9fcd4e75a0a366b4, 0x17c5f0cc30bd57b308b7f62600b415fd1335e1fe, 0xe0156ef2905c2ea8b1f7571caee85fdf1657ab38, 0xdcae4faf7c7d0f4a78abe147244c6e9d60cfd202, 0x1abdcdd0ec2523dd2c66b8c7d1c734f743e98b4a, 0xd972a58b6a582954e578455e4752b12f2c8fcdbc, 0x8e7e7176d3470c6c2efe71004f496a6ef422a56f, 0xdd777e5158cb11db71b4af93c75a96ea11a2a615, 0x2692fee60ff9037f1b73ef5a5c263539221e8085, 0x2c7cb7d5dc4af9caee654553a144c76f10d4b320) THEN 'eETH'
        WHEN b.operator IN (0xbe7d5f26f5d5f567d35a86dd4d7d02aced2d5bff, 0x96fc0751e0febe7296d4625500f8e4535a002c7d, 0x002a465ef04284f72f3721ec902bce5eabe5360b) THEN 'rsETH'
        WHEN b.operator IN (0x8c81d590cc94ca2451c4bde24c598193da74a575, 0x4d7c3fc856ab52753b91a6c9213adf013309dd25, 0xdd6859450e80665db854022e85fb0ed2f0240cb9, 0x73f23013c5a4c209de945cdc58595a4d53d23084, 0x175da1e44c8fbf124714a3bba5dc18a7e65664d6) THEN 'pufETH'
        WHEN b.operator IN (0x5dcdf02a7188257b7c37dd3158756da9ccd4a9cb, 0xdfcb21ac9b99de986d99f4ce5fce2a6542efe3a1, 0x3f98f47d302a3cfd3746fe35f7cf10c3217e5272, 0x5cd6fdfad710609c828feba2508bcaf89e80501a, 0x865cae37b4f44e73ea1e79577c5bfc6207c98f16, 0x84e84949e26a4328c2a503985db33fb38732483e) THEN 'ezETH'
        WHEN b.operator IN (0x21411ff9163455f5bf51d15a56ae59049abe28c0, 0x9abce41e1486210ad83deb831afcdd214af5b49d, 0xc25d6446d6086218cdaa8dd04630dc5d16b591f6, 0xe3ad2a1e9b0514718680f96ff015d653105d51b9, 0x01412450d52d5afeda71b91602d3e0d9da5231c7, 0xdb69c57e9ec197a59d8144a42ecdfb37641be80d, 0xc28af4af11181b72194e6577ff4b556ed4cd27a4, 0x584993e7c47e77098b959bd5e3766592c42bb84f) THEN 'rswETH'
        ELSE 'Other'
    END AS "Protocol"
FROM operator_final_breakdown b
-- Join with active operators to filter out any that are no longer active.
INNER JOIN active_operators s ON b.operator = s.operator
ORDER BY "Rank" ASC;
