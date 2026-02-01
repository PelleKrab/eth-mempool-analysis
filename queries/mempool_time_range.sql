-- Fetch mempool transactions for a time range
-- Parameters: {start_timestamp}, {end_timestamp}

SELECT
    hash as tx_hash,
    toUnixTimestamp(event_date_time) as seen_timestamp,
    toString(gas_fee_cap) as max_fee,
    toString(gas_tip_cap) as priority_fee,
    size as tx_size
FROM mempool_transaction
WHERE event_date_time >= toDateTime({start_timestamp})
  AND event_date_time < toDateTime({end_timestamp})
ORDER BY event_date_time
