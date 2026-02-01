-- Censorship Event Detection
-- Identifies transactions that should have been included but weren't
-- Parameters: {start_block}, {end_block}, {min_pending_blocks}, {window_start_secs}, {window_end_secs}
--
-- NOTE: Simplified version - we can't definitively check if a tx was included in a specific block
-- since we don't have transaction hashes per block. This version identifies txs that appeared
-- in mempool and stayed pending across multiple blocks.

WITH tx_first_seen AS (
    -- Track when each tx first appeared in mempool
    SELECT
        hash as tx_hash,
        `from` as sender,
        nonce,
        toUInt256(gas_fee_cap) as max_fee,
        toUInt256(gas_tip_cap) as priority_fee,
        min(event_date_time) as first_seen
    FROM mempool_transaction
    GROUP BY hash, `from`, nonce, gas_fee_cap, gas_tip_cap
),
block_fee_percentiles AS (
    -- Calculate fee percentiles per block
    SELECT
        b.execution_payload_block_number as block_number,
        b.slot_start_date_time as block_timestamp,
        b.execution_payload_base_fee_per_gas as base_fee_per_gas,
        quantile(0.25)(toFloat64(m.gas_tip_cap)) as p25_fee,
        quantile(0.50)(toFloat64(m.gas_tip_cap)) as p50_fee,
        quantile(0.75)(toFloat64(m.gas_tip_cap)) as p75_fee
    FROM canonical_beacon_block b
    GLOBAL CROSS JOIN mempool_transaction m
    WHERE b.execution_payload_block_number >= {start_block}
      AND b.execution_payload_block_number < {end_block}
      AND m.event_date_time >= addSeconds(b.slot_start_date_time, -30)
      AND m.event_date_time <= b.slot_start_date_time
    GROUP BY
        b.execution_payload_block_number,
        b.slot_start_date_time,
        b.execution_payload_base_fee_per_gas
),
pending_txs AS (
    -- Find txs that were pending during each block
    SELECT
        b.execution_payload_block_number as block_number,
        b.slot_start_date_time as block_timestamp,
        b.execution_payload_base_fee_per_gas as base_fee_per_gas,
        t.tx_hash,
        t.sender,
        t.priority_fee,
        t.max_fee,
        t.first_seen,
        dateDiff('second', t.first_seen, b.slot_start_date_time) as seconds_pending,
        -- Approximate blocks pending (12 seconds per block)
        toInt64(seconds_pending / 12) as blocks_pending,
        -- Check if in time window
        if(
            t.first_seen >= addSeconds(b.slot_start_date_time, {window_start_secs}) AND
            t.first_seen <= addSeconds(b.slot_start_date_time, {window_end_secs}),
            1, 0
        ) as in_time_window
    FROM canonical_beacon_block b
    GLOBAL CROSS JOIN tx_first_seen t
    WHERE b.execution_payload_block_number >= {start_block}
      AND b.execution_payload_block_number < {end_block}
      -- Tx must have been seen before the block
      AND t.first_seen < b.slot_start_date_time
      -- Tx meets base fee requirement
      AND t.max_fee >= b.execution_payload_base_fee_per_gas
      -- Only look at txs that were pending for a reasonable time
      AND seconds_pending >= 12
)
SELECT
    p.block_number,
    toUnixTimestamp(p.block_timestamp) as block_timestamp,
    p.tx_hash,
    p.sender,
    toString(p.priority_fee) as priority_fee,
    toString(p.max_fee) as max_fee,
    toString(fp.p25_fee) as threshold_fee_p25,
    toString(fp.p50_fee) as threshold_fee_p50,
    p.blocks_pending,
    p.seconds_pending,
    p.in_time_window,
    -- Confidence scoring
    if(toFloat64(p.priority_fee) >= fp.p25_fee, 1, 0) as meets_p25_fee,
    if(toFloat64(p.priority_fee) >= fp.p50_fee, 1, 0) as meets_p50_fee,
    if(p.blocks_pending >= {min_pending_blocks}, 1, 0) as waited_long_enough,
    -- Overall confidence
    (meets_p25_fee * waited_long_enough * 0.5 +
     meets_p50_fee * waited_long_enough * 0.5) as confidence_score

FROM pending_txs p
JOIN block_fee_percentiles fp ON p.block_number = fp.block_number

WHERE
    -- Only flag as potential censorship if meets criteria
    p.blocks_pending >= {min_pending_blocks}
    AND toFloat64(p.priority_fee) >= fp.p25_fee
    AND p.in_time_window = 1

ORDER BY p.block_number, confidence_score DESC
