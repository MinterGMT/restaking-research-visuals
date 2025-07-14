-- Query Name: Master AVS Operator Concentration (ID: 5391472)
-- Description: A parameterized query that takes an AVS contract address and returns
--              the list of operators securing it, along with their total delegated stake.

-- Step 1: Define a CTE that gets the list of all operators registered for a specific AVS.
-- The AVS address is passed as a parameter {{avs_address}}.
WITH eigenda_operators AS (
    SELECT
        operator
    FROM
        eigenlayer_ethereum.avs_operator_registration_status_latest
    WHERE
        -- The avs parameter must be provided when executing the query.
        avs = {{avs_address}}
        AND status = 1 -- Filter for current registered/opted-in operators.
)

-- Step 2: Run the main operator delegation query (from B.2), but filter it
-- using the list from Step 1 to only consider operators validating the AVS of interest.
SELECT
    "Operator Name",
    "Operator Address",
    "USD value Delegated",
    "Protocol"
FROM query_5292464 -- This references the results of the main operator query.
WHERE
    -- Filter the main list to include only operators from the avs_operators CTE.
    LOWER("Operator Address") IN (SELECT LOWER (CAST(operator AS VARCHAR)) FROM eigenda_operators)
