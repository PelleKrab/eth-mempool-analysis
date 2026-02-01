-- FOCIL Slot Delay Bandwidth Analysis
-- Calculates L₀, L₋₁, L₋₂ inclusion lists and their intersections
-- Parameters: {start_block}, {end_block}, {window_start_secs}, {window_end_secs}
--
-- L₀: IL for block N based on block N timestamp (0-slot delay)
-- L₋₁: IL for block N based on block N-1 timestamp (1-slot delay)
-- L₋₂: IL for block N based on block N-2 timestamp (2-slot delay)

SELECT
    b.execution_payload_block_number as block_number,
    toUnixTimestamp(b.slot_start_date_time) as block_timestamp,
    b.execution_payload_transactions_count as included_tx_count,

    -- L₀: Transactions in time window around current block
    count(DISTINCT m0.hash) as L0_tx_count,
    sum(DISTINCT m0.size) as L0_size_bytes,

    -- L₋₁: Transactions in time window around previous block
    count(DISTINCT m1.hash) as L1_tx_count,
    sum(DISTINCT m1.size) as L1_size_bytes,

    -- L₋₂: Transactions in time window around block N-2
    count(DISTINCT m2.hash) as L2_tx_count,
    sum(DISTINCT m2.size) as L2_size_bytes,

    -- Intersections: transactions in both lists
    count(DISTINCT if(m0.hash IS NOT NULL AND m1.hash IS NOT NULL, m0.hash, NULL)) as L0_L1_intersection,
    count(DISTINCT if(m0.hash IS NOT NULL AND m2.hash IS NOT NULL, m0.hash, NULL)) as L0_L2_intersection,
    count(DISTINCT if(m1.hash IS NOT NULL AND m2.hash IS NOT NULL, m1.hash, NULL)) as L1_L2_intersection,

    -- Bandwidth savings calculations
    -- Savings = total size - already included size
    (L0_size_bytes - L0_L1_intersection * 200) as bandwidth_savings_1slot,
    (L0_size_bytes - L0_L2_intersection * 200) as bandwidth_savings_2slot

FROM canonical_beacon_block b

-- L₀: Transactions around current block (N)
LEFT JOIN mempool_transaction m0 ON
    m0.event_date_time >= addSeconds(b.slot_start_date_time, {window_start_secs})
    AND m0.event_date_time <= addSeconds(b.slot_start_date_time, {window_end_secs})
    AND toUInt256(m0.gas_fee_cap) >= b.execution_payload_base_fee_per_gas

-- L₋₁: Transactions around previous block (N-1)
LEFT JOIN canonical_beacon_block b1 ON b1.execution_payload_block_number = b.execution_payload_block_number - 1
LEFT JOIN mempool_transaction m1 ON
    m1.event_date_time >= addSeconds(b1.slot_start_date_time, {window_start_secs})
    AND m1.event_date_time <= addSeconds(b1.slot_start_date_time, {window_end_secs})
    AND toUInt256(m1.gas_fee_cap) >= b.execution_payload_base_fee_per_gas

-- L₋₂: Transactions around block N-2
LEFT JOIN canonical_beacon_block b2 ON b2.execution_payload_block_number = b.execution_payload_block_number - 2
LEFT JOIN mempool_transaction m2 ON
    m2.event_date_time >= addSeconds(b2.slot_start_date_time, {window_start_secs})
    AND m2.event_date_time <= addSeconds(b2.slot_start_date_time, {window_end_secs})
    AND toUInt256(m2.gas_fee_cap) >= b.execution_payload_base_fee_per_gas

WHERE b.execution_payload_block_number >= {start_block}
  AND b.execution_payload_block_number < {end_block}
  AND b.execution_payload_block_number > {start_block} + 2  -- Need 2 previous blocks

GROUP BY
    b.execution_payload_block_number,
    b.slot_start_date_time,
    b.execution_payload_transactions_count

ORDER BY b.execution_payload_block_number
