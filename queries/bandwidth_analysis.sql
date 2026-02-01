-- Bandwidth Savings Analysis (1-slot vs 2-slot)
-- Compares bandwidth usage between current-block IL and previous-block IL
-- Parameters: {start_block}, {end_block}, {window_start_secs}, {window_end_secs}
--
-- NOTE: Simplified version without inclusion checking since we don't have
-- transaction hashes per block in the schema

WITH current_block_ils AS (
    -- Get IL for each block (1-slot strategy)
    SELECT
        b.execution_payload_block_number as block_number,
        b.slot_start_date_time as block_timestamp,
        b.execution_payload_base_fee_per_gas as base_fee_per_gas,
        b.execution_payload_transactions_count as tx_count,
        count(m.hash) as il_tx_count,
        sum(m.size) as il_size_bytes
    FROM canonical_beacon_block b
    GLOBAL CROSS JOIN mempool_transaction m
    WHERE b.execution_payload_block_number >= {start_block}
      AND b.execution_payload_block_number < {end_block}
      AND m.event_date_time >= addSeconds(b.slot_start_date_time, {window_start_secs})
      AND m.event_date_time <= addSeconds(b.slot_start_date_time, {window_end_secs})
      AND toUInt256(m.gas_fee_cap) >= b.execution_payload_base_fee_per_gas
    GROUP BY
        b.execution_payload_block_number,
        b.slot_start_date_time,
        b.execution_payload_base_fee_per_gas,
        b.execution_payload_transactions_count
),
previous_block_ils AS (
    -- Shift ILs by one block (2-slot strategy)
    SELECT
        block_number + 1 as block_number,
        il_tx_count as prev_il_tx_count,
        il_size_bytes as prev_il_size_bytes
    FROM current_block_ils
)
SELECT
    c.block_number,
    toUnixTimestamp(c.block_timestamp) as block_timestamp,
    c.tx_count as included_tx_count,

    -- 1-slot metrics (current block IL)
    c.il_tx_count as one_slot_tx_count,
    c.il_size_bytes as one_slot_size_bytes,

    -- 2-slot metrics (previous block IL)
    p.prev_il_tx_count as two_slot_tx_count,
    p.prev_il_size_bytes as two_slot_size_bytes,

    -- Bandwidth comparison
    -- Assume ~70% of IL txs get included (typical rate)
    toInt64(c.il_size_bytes * 0.7) as one_slot_effective_bytes,
    toInt64(p.prev_il_size_bytes * 0.7) as two_slot_effective_bytes,

    -- Savings: positive means 2-slot saves bandwidth
    (one_slot_effective_bytes - two_slot_effective_bytes) as bandwidth_savings_bytes,

    -- Percentage savings
    if(one_slot_effective_bytes > 0,
       (toFloat64(bandwidth_savings_bytes) / toFloat64(one_slot_effective_bytes)) * 100.0,
       0.0
    ) as bandwidth_savings_percent

FROM current_block_ils c
LEFT JOIN previous_block_ils p ON c.block_number = p.block_number
WHERE c.block_number > {start_block}  -- Skip first block (no previous IL)
ORDER BY c.block_number
