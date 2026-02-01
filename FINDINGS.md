# Ethereum Inclusion List Analysis - January 2025

## Executive Summary

This analysis examines the first week of January 2025 (blocks 21,575,000 to 21,624,999) to evaluate the bandwidth and operational characteristics of Inclusion Lists (ILs) for Ethereum's proposed inclusion list mechanism.

**Key Finding:** Inclusion Lists constructed with a time window of -4 to +8 seconds around block time contain **~3.5x more transactions** than the actual block contents, averaging **466 transactions (~91 KB)** per block.

## Dataset

- **Block Range:** 21,575,000 to 21,624,999 (51,010 blocks)
- **Time Period:** January 7-14, 2025 (~167.5 hours / ~7 days)
- **Time Window:** -4 to +8 seconds relative to block timestamp
- **Base Fee Filter:** Only transactions with `max_fee >= base_fee` included in IL

## Inclusion List Size Statistics

### Transaction Counts

| Metric | Value |
|--------|-------|
| **Average IL size** | 466 txs |
| **Median IL size** | 471 txs |
| **Standard deviation** | 139 txs |
| **Min** | 106 txs |
| **Max** | 5,386 txs |
| **95th percentile** | 673 txs |
| **99th percentile** | 834 txs |

### Bandwidth Usage

| Metric | Value |
|--------|-------|
| **Average IL size** | 91 KB |
| **Median IL size** | 92 KB |
| **Max IL size** | 1,052 KB (1.03 MB) |
| **Total bandwidth (7 days)** | 4,530 MB (4.5 GB) |
| **Bandwidth per day** | ~647 MB/day |

## IL vs Block Size Comparison

- **Average block size:** 165 transactions
- **Average IL size:** 466 transactions
- **IL/Block ratio:** **3.47x** (median: 2.93x)

This means Inclusion Lists contain approximately **3.5 times as many transactions** as the blocks themselves. This is significant for:
1. **Bandwidth requirements** for validators
2. **Propagation time** across the network
3. **Storage requirements** if ILs are persisted

## Time Window Analysis

The time window of -4 to +8 seconds was chosen to capture transactions that:
- Arrived slightly before the block (validators may have seen them)
- Arrived in the first few seconds after the previous block

**Results:**
- **Average time offset:** +2.07 seconds
- **Median time offset:** +2.07 seconds
- **Standard deviation:** 0.46 seconds

The distribution is centered almost exactly at +2 seconds, which is the midpoint of the -4 to +8 second window. This suggests the time window is well-calibrated to capture the relevant transaction pool.

## Fee Statistics

| Metric | Value |
|--------|-------|
| **Average priority fee** | 7.94 Gwei |
| **Median priority fee** | 1.50 Gwei |
| **Max median priority fee** | 300 Gwei |

The large difference between average and median suggests occasional high-fee transactions skew the distribution.

## Block Utilization Context

- **Average gas used:** 15.15 M
- **Average gas limit:** 30.02 M
- **Block utilization:** 50.5%

Blocks are only ~50% full on average, yet ILs are 3.5x larger than block contents. This indicates:
1. Many transactions in the mempool are not immediately included
2. Block builders are selective (likely optimizing for MEV)
3. ILs would force inclusion of transactions that builders might otherwise skip

## Top 10 Largest ILs

| Block Number | IL Size (txs) | IL Size (KB) | Included Txs |
|-------------|---------------|--------------|--------------|
| 21,582,347 | 5,386 | 1,052 | 171 |
| 21,614,797 | 4,828 | 943 | 185 |
| 21,578,285 | 3,275 | 640 | 246 |
| 21,578,284 | 2,922 | 571 | 80 |
| 21,578,608 | 2,918 | 570 | 76 |
| 21,577,148 | 2,743 | 536 | 183 |
| 21,614,798 | 2,536 | 495 | 111 |
| 21,616,184 | 2,471 | 483 | 473 |
| 21,599,243 | 2,465 | 481 | 698 |
| 21,580,257 | 2,454 | 479 | 140 |

The largest IL was over 1 MB, containing 5,386 transactions but only 171 were actually included in the block (31x difference!).

## Implications

### 1. Bandwidth Requirements

**Per-block:** 91 KB average (673 KB at 95th percentile)
**Daily total:** ~647 MB
**Annual total:** ~236 GB

For validators running on modest bandwidth connections, this represents a significant overhead.

### 2. 1-Slot vs 2-Slot Delay

The **slot delay** question is crucial:
- **1-slot:** IL built from transactions seen -4 to +8 seconds around block N, sent with block N+1
- **2-slot:** IL built from transactions seen around block N, sent with block N+2

**Trade-off:**
- 1-slot: Lower latency for censorship resistance, but higher bandwidth waste (many txs already included)
- 2-slot: More bandwidth efficient (txs have time to be included), but censorship window is longer

Our data shows ILs are 3.5x larger than blocks, suggesting significant overlap. **2-slot strategy would likely save substantial bandwidth** by allowing more time for transactions to be naturally included.

### 3. Censorship Resistance

With ILs averaging 466 transactions, this provides:
- Strong censorship resistance (hard to skip 466 txs)
- Good coverage of the transaction pool
- Forces inclusion of transactions that might otherwise be censored

However, the 99th percentile of 834 transactions suggests occasional very large ILs that could cause propagation delays.

## Recommendations

1. **Investigate 2-slot delay** to reduce bandwidth waste
2. **Consider IL size caps** (e.g., 500 txs or 100 KB) to prevent extreme cases
3. **Further analyze** which transactions in ILs are vs aren't included
4. **Study transaction replacement behavior** (nonce replacements) during the time window

## Methodology

Data sourced from the Xatu ClickHouse mempool dataset:
- **Blocks:** `canonical_beacon_block` table
- **Mempool:** `mempool_transaction` table
- **Processing:** Python batch processor with 1000-block batches
- **Time window:** Transactions with `event_date_time` in [-4s, +8s] relative to `slot_start_date_time`
- **Fee filter:** `gas_fee_cap >= execution_payload_base_fee_per_gas`

## Next Steps

1. ✅ Inclusion List metrics (completed)
2. 🔄 Nonce replacement analysis (in progress)
3. ⏳ Bandwidth savings analysis (1-slot vs 2-slot)
4. ⏳ Censorship event detection

---

*Analysis performed: January 31, 2026*
*Data repository: https://github.com/PelleKrab/eth-mempool-analysis*

## Transaction Replacement Analysis

### Overview

**17.5 million replacement events** were detected during the week, representing transactions where the same sender/nonce pair appeared multiple times with different parameters.

### Replacement Types

| Type | Count | Percentage |
|------|-------|------------|
| **Same-fee replacements** | 17,045,738 | 97.2% |
| **Fee increases (RBF)** | 493,483 | 2.8% |
| **Fee decreases** | 0 | 0.0% |

**Key insight:** Only 2.8% of replacements actually increase fees (true Replace-By-Fee). The vast majority (97.2%) keep the same fee, suggesting they are:
- Nonce cancellations
- Transaction modifications (changing recipient, data, etc.)
- MEV bundle adjustments

### Fee Multiplier Statistics

For the 493,483 true RBF transactions (fee increases):
- **Median multiplier:** 1.099x (~10% increase)
- **95th percentile:** 4.3x
- **99th percentile:** 71.4x
- **Mean:** 922,825x (skewed by extreme outliers)

Most users increase fees modestly (~10%), but some increase dramatically when urgent.

### Replacement Frequency

| Versions | Count | Percentage |
|----------|-------|------------|
| 2 | 15,646,717 | 89.2% |
| 3 | 615,657 | 3.5% |
| 4 | 979,604 | 5.6% |
| 5 | 72,572 | 0.4% |
| 6 | 91,467 | 0.5% |
| 7-10 | 86,403 | 0.5% |
| >10 | 47,006 | 0.3% |
| **Max** | **239** | - |

**Key insight:** Most replacements are simple (2 versions = 89.2%), but some transactions go through hundreds of iterations.

### Timing

| Metric | Value |
|--------|-------|
| **Quick replacements (<1 min)** | 87.4% |
| **Median replacement time** | 0 seconds |
| **95th percentile** | 6,049s (~1.7 hours) |
| **Slow replacements (>1 hour)** | 9.2% |

Most replacements happen immediately (same second), but ~9% take over an hour, suggesting users trying to adjust stuck transactions.

### Implications for Inclusion Lists

1. **Nonce tracking complexity:** With 17.5M replacements/week, IL builders must track which transaction version is "latest"
2. **Rapid updates:** 87.4% of replacements happen <1 minute, requiring frequent IL updates
3. **RBF behavior:** Only 2.8% are true RBF, most are other modifications
4. **Edge cases:** 239 versions of a single transaction shows extreme iteration in some cases

