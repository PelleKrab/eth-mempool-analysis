-- Transaction Nonce Replacements
-- Identifies transactions that were replaced by higher-fee versions
-- Parameters: {start_timestamp}, {end_timestamp}

WITH ranked_txs AS (
    SELECT
        `from` as sender,
        nonce,
        hash as tx_hash,
        toUInt256(gas_fee_cap) as max_fee,
        toUInt256(gas_tip_cap) as priority_fee,
        event_date_time as seen_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY `from`, nonce
            ORDER BY toUInt256(gas_fee_cap) DESC, event_date_time DESC
        ) as fee_rank,
        COUNT(*) OVER (PARTITION BY `from`, nonce) as version_count
    FROM mempool_transaction
    WHERE event_date_time >= toDateTime({start_timestamp})
      AND event_date_time < toDateTime({end_timestamp})
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
    toUnixTimestamp(final_timestamp) - toUnixTimestamp(original_timestamp) as replacement_time_secs
FROM replacements
ORDER BY final_timestamp
