-- Block-level Inclusion List Metrics - Simplified for Python processing
-- This query just gets block data; mempool analysis done separately
-- Parameters: {start_block}, {end_block}

SELECT
    execution_payload_block_number as block_number,
    toUnixTimestamp(slot_start_date_time) as block_timestamp,
    toString(execution_payload_base_fee_per_gas) as base_fee,
    execution_payload_transactions_count as included_tx_count,
    execution_payload_gas_used as gas_used,
    execution_payload_gas_limit as gas_limit
FROM canonical_beacon_block
WHERE execution_payload_block_number >= {start_block}
  AND execution_payload_block_number < {end_block}
ORDER BY execution_payload_block_number
