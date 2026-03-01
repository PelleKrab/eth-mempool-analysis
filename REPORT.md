# Ethereum FOCIL Inclusion List Analysis: Bandwidth and Censorship Metrics

**Draft Report** - February 2026
**Data period:** 1 January 2024 to 25 December 2024
**Dataset:** BlockNative public mempool (2,537,147 blocks, ~96.5% of 2024)

---

## 1. Introduction

Fork-Choice Enforced Inclusion Lists (FOCIL, [EIP-7805](https://eips.ethereum.org/EIPS/eip-7805)) allow a committee of validators to force transaction inclusion, providing a trustless censorship-resistance mechanism. A key practical question is how much bandwidth validators would consume propagating these ILs, and whether delaying enforcement by 1-2 slots meaningfully reduces that cost without undermining censorship resistance.

This report addresses three research questions:

**RQ1 - Baseline redundancy:** What percentage of transactions placed in an Inclusion List under standard IL-building strategies are redundant, i.e., would have been included by builders without any FOCIL enforcement?

**RQ2 - Delay effect on redundancy:** How does delaying IL broadcast (and therefore enforcement) by 1 or 2 slots change the proportion of redundant transactions?

**RQ3 - Strategy comparison:** How do the answers to RQ1 and RQ2 differ between a Top Fee IL (highest-fee pending transactions) and a Censorship-Only IL (transactions meeting explicit delay/exclusion criteria)?

---

## 2. Methodology

### 2.1 Data Sources

Analysis was conducted over two mempool datasets to validate findings across different node perspectives:

**BlockNative (primary):** A commercial mempool monitoring service with full transaction lifecycle tracking (pending to confirmed / cancelled / evicted). Lifecycle data enables direct detection of replacement transactions (cancel/speedup) without heuristics, and `timepending` / `blockspending` fields provide ground-truth inclusion timing. BN data covers blocks 18,908,895 to 21,475,794 (Jan-Dec 2024).

**Xatu (validation):** An open-source ethpandaops project providing mempool observations from globally distributed nodes. Xatu data required additional filtering (detailed in §2.3) due to phantom transactions absent from BN.

Data processing was performed in Python against the ethpandaops ClickHouse cluster. Block metadata (slot timestamp, base fee, gas used, gas limit, transaction count) was sourced from the `canonical_beacon_block` table. Included transaction hashes were sourced from `canonical_execution_transaction`. All cross-table joins were performed locally after fetching each table separately, as distributed JOINs are not supported on the cluster.

### 2.2 Inclusion List Construction

For each slot N, two independent ILs are built:

**Top Fee IL ($L^{tf}_N$):** The highest effective-priority-fee transactions visible in the mempool window $[T_N - 4s,\ T_N + 8s]$, packed greedily by size up to the 8 KiB EIP-7805 cap. This represents the standard "validator builds from best pending txs" strategy and approximates what an honest IL builder would submit.

**Censored IL ($L^{cs}_N$):** Transactions meeting the censorship criteria defined in §2.3, packed greedily. This IL contains only transactions suspected to have been deliberately or structurally excluded.

Both ILs are built at slot N and are then evaluated for redundancy at three enforcement delay levels (0, 1, 2 slots), giving six variants total.

### 2.3 Censorship Detection

A transaction is flagged as censored at slot N if it satisfies all of the following:

1. **FOCIL-valid:** `max_fee_per_gas >= base_fee` of block N.
2. **Competitive fee:** Effective priority fee >= 50th percentile of all FOCIL-valid transactions in the censored window $[T_N - 12s,\ T_N]$. Effective priority fee is computed per EIP-1559 as $f_{eff} = \min(f_p,\ f_{max} - f_b)$, where $f_p$ is the priority fee, $f_{max}$ is max fee per gas, and $f_b$ is the block base fee.
3. **Sufficient dwell time:** First seen in mempool >= 12 seconds before $T_N$ (exactly 1 slot), and <= 12 seconds (capped at 1 slot to avoid overlap with the prior slot's window).
4. **Not replaced:** Not a nonce replacement (detected via BN cancel/speedup status, or via nonce-collision heuristics on Xatu data).
5. **Gas fits:** `gas_limit` <= remaining gas capacity in both block N-1 and block N. A transaction that physically could not have fit is not considered censored.
6. **Not on-chain:** Not included in any block through N.
7. **Active sender (Xatu only):** Sender has at least one on-chain transaction within a +/-100,000 block window. This filter is required for Xatu but not BN because ~75% of Xatu mempool senders are phantom addresses: wallets that broadcast high-fee transactions to all nodes but are never confirmed by any builder and have no other on-chain history. BN's lifecycle tracking rejects these natively.

### 2.4 Redundancy Measurement

"Redundancy" refers to IL transactions that are included canonically in the blocks following the slot at which the IL was built, without FOCIL enforcement being required. This measures the fraction of IL bandwidth that would have been unnecessary because builders included those transactions on their own.

For a given IL $L$ built at slot $N$ and enforced with delay $D$:

$$\text{Redundant}(L, D) = \left\{ tx \in L \ \middle|\ tx \in \bigcup_{n=N+1}^{N+1+D} B_n \right\}$$

$$\text{Inclusion Rate}(L, D) = \frac{|\text{Redundant}(L, D)|}{|L|} \times 100\%$$

$$\text{UsefulBytes}(L, D) = \sum_{tx \in L} \text{size}(tx) \cdot \mathbf{1}[tx \notin \text{Redundant}(L, D)]$$

Where $B_n$ is the set of transaction hashes in canonical block $n$. Both useful and redundant bytes are tracked directly per block. For BN data, `size(tx)` is estimated as calldata bytes plus 125 bytes of fixed overhead, as BN does not expose the full RLP-encoded transaction size. This affects absolute byte totals but not inclusion rate or redundancy percentage calculations.

The IL composition is identical across all delay levels; only the evaluation window differs. Delaying enforcement does not change which transactions are placed in the IL, only how many get included naturally before it takes effect.

---

## 3. Results

### 3.1 Dataset Overview (Block Native)

| Metric | Value |
| :--- | :--- |
| Blocks analyzed | 2,537,147 |
| Block range | 18,908,895 to 21,475,794 |
| Time period | 2024-01-01 to 2024-12-25 (359 days) |
| Missing blocks | ~29,753 (~1.2%) |
| Avg gas utilization | 50.5% of limit |
| Avg transactions/block | 163.4 |
| Avg base fee | 8.52 Gwei (median: 5.04 Gwei, p95: 29.50 Gwei) |
| Mempool coverage of N+1 | 54.7% |

The 54.7% mempool coverage figure means that on average, just over half of the transactions included in a given block were observable in the BN mempool during the 12-second window around that block's timestamp. The remainder represents private orderflow, late-arriving transactions, and coverage gaps.

### 3.2 RQ1 and RQ2 - Top Fee IL Redundancy and Delay Effect

| Delay | Total (KiB/blk) | Useful (KiB/blk) | Redundant (KiB/blk) | Efficiency | Inclusion Rate | Ann. Total (GB) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 0-slot | 7.24 | **1.19** | 6.05 | 16.4% | 84.0% | 18.1 |
| 1-slot | 7.24 | **1.08** | 6.16 | 14.9% | 85.5% | 18.1 |
| 2-slot | 7.24 | **1.06** | 6.18 | 14.7% | 85.7% | 18.1 |

The Top Fee IL fills near-capacity: the median IL is 7.81 KiB and the distribution is left-skewed, with <0.3% of blocks hitting the hard 8 KiB cap. Average is pulled down by low-activity blocks.

**RQ1:** 84.0% of Top Fee IL transactions are redundant at 0-delay. Builders already include these transactions. Only 1.19 KiB/block (16.4%) represents genuine enforcement value.

**RQ2:** Delaying enforcement by 1 slot increases redundancy to 85.5% (+1.5 pp), reducing useful bytes by 9.4%. A 2-slot delay reaches 85.7% (+1.7 pp total), reducing useful bytes by 10.8%. The marginal gain from a second slot of delay is only 1.4 pp, indicating that naturally-included transactions land in N+1 rather than N+2 or N+3. The total propagated IL bandwidth is 7.24 KiB/block (18.1 GB/year).

### 3.3 RQ1 and RQ2 - Censored IL Redundancy and Delay Effect

| Delay | Total (KiB/blk) | Useful (KiB/blk) | Redundant (KiB/blk) | Efficiency | Inclusion Rate | Ann. Total (GB) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 0-slot | 0.163 | **0.144** | 0.019 | 88.4% | 14.3% | 0.41 |
| 1-slot | 0.163 | **0.139** | 0.024 | 85.3% | 18.1% | 0.41 |
| 2-slot | 0.163 | **0.136** | 0.027 | 83.7% | 20.1% | 0.41 |

**RQ1:** Only 14.3% of Censored IL transactions are redundant at 0-delay, meaning 88.4% of the Censored IL represents genuine enforcement value. Flagged transactions are not being included by builders on their own.

**RQ2:** Delaying enforcement by 1 slot increases redundancy to 18.1% (+3.8 pp), and by 2 slots to 20.1% (+5.8 pp). Efficiency degrades from 88.4% to 83.7% over 2 slots. Unlike the Top Fee IL, additional delay has a more meaningful proportional effect on the Censored IL's redundancy, though the absolute change in useful bytes remains small given the IL's compact size (0.41 GB/year total).

### 3.4 Censorship Detection

**Note on data quality:** 24.4% of blocks (618,051) have a recorded base fee of zero, caused by NULL values in `canonical_beacon_block.execution_payload_base_fee_per_gas` being silently cast to zero. When base_fee is zero, every transaction in the BN window passes the FOCIL-validity check, making the censorship detection criteria trivially satisfiable and inflating counts significantly. The table below presents figures for the 1,919,096 blocks with a valid (non-zero) base fee.

| Metric | Valid blocks only (base_fee > 0) |
| :--- | :--- |
| Avg censored txs/block | 0.31 |
| Median censored txs/block | 0 |
| Blocks with any detection | 155,330 (8.1%) |
| 95th percentile | 2 txs/block |
| 99th percentile | 7 txs/block |
| Maximum observed | 837 txs/block |

Across valid blocks, censorship detections are infrequent: 91.9% of blocks have no flagged transactions and the median is zero. The distribution has a long tail with a small number of high-count blocks. The single-block maximum of 837 remains an extreme outlier warranting further investigation.

### 3.5 BlockNative Mempool Characteristics

| Metric | Value |
| :--- | :--- |
| Avg pending txs in 12s window | 469 |
| Avg eventually confirmed (of window txs) | 290 (61.8%) |
| Avg replaced (cancel/speedup) | 2.9/block |
| Mempool coverage of N+1 | 54.7% |

"Eventually confirmed" counts window transactions that were included in any block, not necessarily block N. The BN window spans 12 seconds of mempool activity, overlapping with transactions destined for several adjacent blocks. Block N itself contains ~163 transactions, of which ~89 (54.7%) are visible in the window. The remaining ~200 of the 290 confirmed window transactions are bound for neighbouring blocks.

---

## 4. Discussion

### 4.1 RQ1: Top Fee ILs Are Structurally Dominated by Redundancy

The most significant finding is that 83.6% of Top Fee IL bandwidth is redundant. These transactions would have been included by builders regardless. This is a consequence of the Top Fee IL selecting the same high-fee transactions that rational builders are already maximally incentivised to include. FOCIL's censorship resistance value in the top-fee strategy comes almost entirely from the remaining 16.4% (1.19 KiB/block).

This redundancy is an inherent property of the top-fee strategy. Any IL-building algorithm that selects by fee will converge on the same candidate set as profit-maximising builders. The utility of the Top Fee IL is therefore that it forces inclusion of the last 16% that a censoring builder might otherwise drop, at the cost of propagating 6x more data than strictly necessary.

The Censored IL inverts this: 88.4% of its bandwidth is useful because it targets only transactions that builders are actively not including. The trade-off is that the Censored IL is 44x smaller in absolute size, covering far fewer transactions per slot.

### 4.2 RQ2: Delays Reduce Useful Bandwidth

Delaying enforcement by 1-2 slots reduces the useful bandwidth of the Top Fee IL by only 9-11%, and the total propagated bandwidth is unchanged. Validators still send the same ~7.24 KiB/block regardless of delay.

For the Censored IL, the proportional redundancy increase with delay is larger (14.3% to 20.1%) but absolute useful bytes change by less than 0.01 KiB/block. Delay does not provide a meaningful impact useful bandwith.

### 4.3 RQ3: The Two Strategies Have Opposite Redundancy Profiles

The contrast between the two ILs directly answers RQ3:

| | Top Fee IL | Censored IL |
| :--- | :--- | :--- |
| Baseline redundancy (RQ1) | 84.0% | 14.3% |
| Redundancy at 2-slot delay (RQ2) | 85.7% (+1.7 pp) | 20.1% (+5.8 pp) |
| Useful bytes/block | 1.19 KiB | 0.144 KiB |
| Total bandwidth/block | 7.24 KiB | 0.163 KiB |
| Annual bandwidth | 18.1 GB | 0.41 GB |

The Top Fee IL is bandwidth-heavy but highly redundant; delay barely changes its redundancy profile. The Censored IL is bandwidth-light with high useful efficiency; delay erodes that efficiency proportionally faster, though the absolute impact is small.



---

## 5. Limitations

**Mempool visibility ceiling (~55%):** Only 54.7% of next-block transactions were observable in the BN mempool window. The remainder (~45%) arrives via private orderflow channels (e.g., Flashbots MEV-Boost, direct builder submission) or is submitted too close to block production to be captured. Censorship detection can only flag transactions visible in the public mempool; private-orderflow censorship is invisible to this methodology.

**Estimated transaction sizes (BN):** BlockNative provides `datasize` (calldata bytes) but not the full RLP-encoded transaction size. Tx size is estimated as `datasize + 125 bytes` overhead. This introduces noise into the per-block byte totals but does not affect inclusion rate measurements.

**Natural censorship frequency is low:** Organic censorship on Ethereum mainnet is rare. The 15.8% of blocks with detections primarily reflects structural exclusion (gas fits but was not chosen) rather than deliberate OFAC-style filtering.

---

## 6. Conclusions

Across 2,537,147 blocks spanning January to December 2024:

**RQ1 - Baseline redundancy:** The Top Fee IL has an 84.0% redundancy rate at 0-delay; builders already include the vast majority of its transactions without any FOCIL enforcement. Only 16.4% (1.19 KiB/block) represents genuine enforcement value. The Censored IL has the inverse profile: 88.4% of its bandwidth is useful, as flagged transactions are genuinely not being included by builders.

**RQ2 - Delay effect:** Delaying Top Fee IL enforcement by 1-2 slots increases redundancy by only 1.5-1.7 pp and reduces useful bytes by 9-11%, while total propagated bandwidth is unchanged. Delaying Censored IL enforcement has a larger proportional redundancy increase (14.3% to 20.1% over 2 slots) but negligible absolute impact given the IL's compact size. Neither strategy achieves meaningful bandwidth savings through delayed enforcement.

**RQ3 - Strategy comparison:** The two strategies have opposing redundancy profiles that change differently with delay. The Top Fee IL is high-bandwidth (18.1 GB/year) and highly redundant (84%) with minimal sensitivity to delay. The Censored IL is low-bandwidth (0.41 GB/year) and highly useful (88%) but degrades proportionally faster with delay. The choice between strategies reflects a fundamental trade-off: broad coverage at high redundancy cost versus targeted enforcement at low bandwidth cost.

---

## Appendix: Implementation Notes

| Parameter | Value |
| :--- | :--- |
| IL size cap | 8,192 bytes (EIP-7805) |
| Top Fee window | [T_N - 4s, T_N + 8s] |
| Censored window | [T_N - 12s, T_N] |
| Min dwell time |0s |
| Max dwell time | 12s (capped to 1 slot) |
| Fee percentile threshold | 50th percentile of valid pending txs |
| TX type filter (Xatu) | EIP-1559 (type 2) only |
| Active sender lookback | +/-100,000 blocks |
| Tx size estimation (BN) | datasize + 125 bytes overhead |
| Delay levels evaluated | 0, 1, 2 slots |
| Source code | [GitHub](https://github.com/ethpandaops/eth-mempool-analysis) |
| Data source | BlockNative `block_native_mempool_transaction` |
| Data source | Xatu `mempool_transaction` |

**References:**
- [EIP-7805: Fork-Choice Enforced Inclusion Lists](https://eips.ethereum.org/EIPS/eip-7805)
- [Execution Specs #1928: FOCIL validation rules](https://github.com/ethereum/execution-specs/pull/1928)
- [Xatu documentation](https://ethpandaops.io/docs/tooling/xatu/)
- [Prysm bandwidth discussion](https://discord.com/channels/595666850260713488/1210529202458071050/1443658440609239151)

