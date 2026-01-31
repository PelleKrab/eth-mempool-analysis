-- Bandwidth Savings Analysis (1-slot vs 2-slot)
-- Compares bandwidth usage between current-block IL and previous-block IL
-- Parameters: {start_block}, {end_block}, {window_start_secs}, {window_end_secs}

WITH current_block_ils AS (
    -- Get IL for each block (1-slot strategy)
    SELECT
        b.block_number,
        b.block_timestamp,
        b.base_fee_per_gas,
        b.transactions,
        groupArray(m.tx_hash) as il_tx_hashes,
        count(m.tx_hash) as il_tx_count,
        sum(length(m.input_data) / 2) as il_size_bytes  -- hex string to bytes
    FROM canonical_beacon_block b
    LEFT JOIN mempool_transaction m ON
        m.seen_timestamp >= b.block_timestamp + INTERVAL {window_start_secs} SECOND AND
        m.seen_timestamp <= b.block_timestamp + INTERVAL {window_end_secs} SECOND AND
        toUInt256(m.max_fee_per_gas) >= b.base_fee_per_gas AND
        NOT arrayExists(x -> x = m.tx_hash, b.transactions)
    WHERE b.block_number >= {start_block}
      AND b.block_number < {end_block}
    GROUP BY b.block_number, b.block_timestamp, b.base_fee_per_gas, b.transactions
),
previous_block_ils AS (
    -- Shift ILs by one block (2-slot strategy)
    SELECT
        block_number + 1 as block_number,
        il_tx_hashes as prev_il_tx_hashes,
        il_tx_count as prev_il_tx_count,
        il_size_bytes as prev_il_size_bytes
    FROM current_block_ils
)
SELECT
    c.block_number,
    toUnixTimestamp(c.block_timestamp) as block_timestamp,

    -- 1-slot metrics (current block IL)
    c.il_tx_count as one_slot_tx_count,
    c.il_size_bytes as one_slot_size_bytes,
    arrayMap(x -> arrayExists(y -> y = x, c.transactions), c.il_tx_hashes) as one_slot_included_flags,
    countEqual(one_slot_included_flags, 1) as one_slot_already_included,

    -- 2-slot metrics (previous block IL)
    p.prev_il_tx_count as two_slot_tx_count,
    p.prev_il_size_bytes as two_slot_size_bytes,
    arrayMap(x -> arrayExists(y -> y = x, c.transactions), p.prev_il_tx_hashes) as two_slot_included_flags,
    countEqual(two_slot_included_flags, 1) as two_slot_already_included,

    -- Bandwidth calculations
    -- Effective bandwidth = total size - already included (waste)
    (c.il_size_bytes - (one_slot_already_included * 200)) as one_slot_effective_bytes,
    (p.prev_il_size_bytes - (two_slot_already_included * 200)) as two_slot_effective_bytes,

    -- Savings: positive means 2-slot saves bandwidth
    (one_slot_effective_bytes - two_slot_effective_bytes) as bandwidth_savings_bytes,

    -- Percentage savings
    if(one_slot_effective_bytes > 0,
       (bandwidth_savings_bytes / toFloat64(one_slot_effective_bytes)) * 100.0,
       0.0
    ) as bandwidth_savings_percent

FROM current_block_ils c
LEFT JOIN previous_block_ils p ON c.block_number = p.block_number
WHERE c.block_number > {start_block}  -- Skip first block (no previous IL)
ORDER BY c.block_number
