-- Block-level Inclusion List Metrics
-- This query calculates IL-related metrics for each block
-- Parameters: {start_block}, {end_block}, {window_start_secs}, {window_end_secs}

SELECT
    b.block_number,
    toUnixTimestamp(b.block_timestamp) as block_timestamp,
    toString(b.base_fee_per_gas) as base_fee,
    length(b.transactions) as included_tx_count,
    b.gas_used,
    b.gas_limit,

    -- Time window transaction counts
    countIf(
        m.tx_hash IS NOT NULL AND
        m.seen_timestamp >= b.block_timestamp + INTERVAL {window_start_secs} SECOND AND
        m.seen_timestamp <= b.block_timestamp + INTERVAL {window_end_secs} SECOND AND
        toUInt256(m.max_fee_per_gas) >= b.base_fee_per_gas
    ) as time_window_tx_count,

    -- Txs in time window that were already included
    countIf(
        m.tx_hash IS NOT NULL AND
        arrayExists(x -> x = m.tx_hash, b.transactions) AND
        m.seen_timestamp >= b.block_timestamp + INTERVAL {window_start_secs} SECOND AND
        m.seen_timestamp <= b.block_timestamp + INTERVAL {window_end_secs} SECOND
    ) as time_window_included_count,

    -- IL candidates (in window, not included, meets base fee)
    countIf(
        m.tx_hash IS NOT NULL AND
        NOT arrayExists(x -> x = m.tx_hash, b.transactions) AND
        m.seen_timestamp >= b.block_timestamp + INTERVAL {window_start_secs} SECOND AND
        m.seen_timestamp <= b.block_timestamp + INTERVAL {window_end_secs} SECOND AND
        toUInt256(m.max_fee_per_gas) >= b.base_fee_per_gas
    ) as il_tx_count,

    -- Estimate IL size in bytes (very rough: 200 bytes avg per tx)
    il_tx_count * 200 as il_size_bytes,

    -- Average offset from block time (in seconds)
    avgIf(
        toUnixTimestamp(m.seen_timestamp) - toUnixTimestamp(b.block_timestamp),
        m.tx_hash IS NOT NULL AND
        m.seen_timestamp >= b.block_timestamp + INTERVAL {window_start_secs} SECOND AND
        m.seen_timestamp <= b.block_timestamp + INTERVAL {window_end_secs} SECOND
    ) as avg_time_offset_secs,

    -- Fee statistics for time window txs
    avgIf(
        toFloat64(m.tip),
        m.tx_hash IS NOT NULL AND
        m.seen_timestamp >= b.block_timestamp + INTERVAL {window_start_secs} SECOND AND
        m.seen_timestamp <= b.block_timestamp + INTERVAL {window_end_secs} SECOND
    ) as avg_priority_fee,

    medianIf(
        toFloat64(m.tip),
        m.tx_hash IS NOT NULL AND
        m.seen_timestamp >= b.block_timestamp + INTERVAL {window_start_secs} SECOND AND
        m.seen_timestamp <= b.block_timestamp + INTERVAL {window_end_secs} SECOND
    ) as median_priority_fee

FROM canonical_beacon_block b
LEFT JOIN mempool_transaction m ON
    -- Join on a wider window to capture all relevant txs
    m.seen_timestamp BETWEEN b.block_timestamp + INTERVAL ({window_start_secs} - 2) SECOND
                         AND b.block_timestamp + INTERVAL ({window_end_secs} + 2) SECOND

WHERE b.block_number >= {start_block}
  AND b.block_number < {end_block}

GROUP BY
    b.block_number,
    b.block_timestamp,
    b.base_fee_per_gas,
    b.transactions,
    b.gas_used,
    b.gas_limit

ORDER BY b.block_number
