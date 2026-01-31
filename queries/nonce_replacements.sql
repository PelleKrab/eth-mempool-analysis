-- Transaction Nonce Replacements
-- Identifies transactions that were replaced by higher-fee versions
-- Parameters: {start_timestamp}, {end_timestamp}

WITH ranked_txs AS (
    SELECT
        sender,
        nonce,
        tx_hash,
        toUInt256(max_fee_per_gas) as max_fee,
        toUInt256(tip) as priority_fee,
        seen_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY sender, nonce
            ORDER BY toUInt256(max_fee_per_gas) DESC, seen_timestamp DESC
        ) as fee_rank,
        COUNT(*) OVER (PARTITION BY sender, nonce) as version_count
    FROM mempool_transaction
    WHERE seen_timestamp >= toDateTime({start_timestamp})
      AND seen_timestamp < toDateTime({end_timestamp})
),
replacements AS (
    SELECT
        sender,
        nonce,
        version_count,
        -- Original (lowest fee) tx
        minIf(tx_hash, fee_rank = version_count) as original_tx_hash,
        minIf(max_fee, fee_rank = version_count) as original_max_fee,
        minIf(seen_timestamp, fee_rank = version_count) as original_timestamp,
        -- Final (highest fee) tx
        minIf(tx_hash, fee_rank = 1) as final_tx_hash,
        minIf(max_fee, fee_rank = 1) as final_max_fee,
        minIf(seen_timestamp, fee_rank = 1) as final_timestamp,
        -- Fee increase
        (final_max_fee - original_max_fee) as fee_increase
    FROM ranked_txs
    WHERE version_count > 1
    GROUP BY sender, nonce, version_count
)
SELECT
    sender,
    nonce,
    version_count as replacement_count,
    original_tx_hash,
    toString(original_max_fee) as original_max_fee,
    toUnixTimestamp(original_timestamp) as original_timestamp,
    final_tx_hash,
    toString(final_max_fee) as final_max_fee,
    toUnixTimestamp(final_timestamp) as final_timestamp,
    toString(fee_increase) as fee_increase,
    toFloat64(final_max_fee) / toFloat64(original_max_fee) as fee_multiplier,
    dateDiff('second', original_timestamp, final_timestamp) as replacement_time_secs
FROM replacements
ORDER BY final_timestamp
