-- Censorship Event Detection
-- Identifies transactions that should have been included but weren't
-- Parameters: {start_block}, {end_block}, {min_pending_blocks}, {window_start_secs}, {window_end_secs}

WITH tx_first_seen AS (
    -- Track when each tx first appeared in mempool
    SELECT
        tx_hash,
        sender,
        nonce,
        toUInt256(max_fee_per_gas) as max_fee,
        toUInt256(tip) as priority_fee,
        min(seen_timestamp) as first_seen
    FROM mempool_transaction
    GROUP BY tx_hash, sender, nonce, max_fee_per_gas, tip
),
block_fee_percentiles AS (
    -- Calculate fee percentiles per block
    SELECT
        block_number,
        block_timestamp,
        base_fee_per_gas,
        quantile(0.25)(toFloat64(m.tip)) as p25_fee,
        quantile(0.50)(toFloat64(m.tip)) as p50_fee,
        quantile(0.75)(toFloat64(m.tip)) as p75_fee
    FROM canonical_beacon_block b
    LEFT JOIN mempool_transaction m ON
        m.seen_timestamp >= b.block_timestamp - INTERVAL 30 SECOND AND
        m.seen_timestamp <= b.block_timestamp
    WHERE b.block_number >= {start_block}
      AND b.block_number < {end_block}
    GROUP BY block_number, block_timestamp, base_fee_per_gas
),
pending_txs AS (
    -- Find txs that were pending during each block
    SELECT
        b.block_number,
        b.block_timestamp,
        b.base_fee_per_gas,
        b.transactions,
        t.tx_hash,
        t.sender,
        t.priority_fee,
        t.max_fee,
        t.first_seen,
        dateDiff('second', t.first_seen, b.block_timestamp) as seconds_pending,
        -- Count how many blocks this tx has been pending
        (SELECT count(*)
         FROM canonical_beacon_block b2
         WHERE b2.block_timestamp >= t.first_seen
           AND b2.block_timestamp <= b.block_timestamp
        ) as blocks_pending,
        -- Check if in time window
        if(
            t.first_seen >= b.block_timestamp + INTERVAL {window_start_secs} SECOND AND
            t.first_seen <= b.block_timestamp + INTERVAL {window_end_secs} SECOND,
            1, 0
        ) as in_time_window
    FROM canonical_beacon_block b
    CROSS JOIN tx_first_seen t
    WHERE b.block_number >= {start_block}
      AND b.block_number < {end_block}
      -- Tx must have been seen before the block
      AND t.first_seen < b.block_timestamp
      -- Tx not included in this block
      AND NOT arrayExists(x -> x = t.tx_hash, b.transactions)
      -- Tx meets base fee requirement
      AND t.max_fee >= b.base_fee_per_gas
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
