# Ethereum FOCIL Inclusion List Analysis: Bandwidth and Censorship Metrics

**Draft Report** - March 2026
**Data period:** 13 March 2024 to 25 December 2024
**Dataset:** BlockNative public mempool (699,363 blocks, bn_full_analysis_v2)

---

**TL;DR: It is hard to construct an IL that does not broadcast transactions that will be included anyway. Both building ILs based on top fees and potentially censored TXs suffer from this, although censored TX strategies result in ~32 percentage points fewer redundant TXs. Delaying IL enforcement by a slot or two increases redundancy by ~2 pp. Live testnet data is needed to help refine IL building strategies.**

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

**BlockNative (primary):** A commercial mempool monitoring service with full transaction lifecycle tracking (pending to confirmed / cancelled / evicted). Lifecycle data enables direct detection of replacement transactions (cancel/speedup) without heuristics, and `timepending` / `blockspending` fields provide ground-truth inclusion timing.

**Xatu (explored, not used for primary results):** An open-source ethpandaops project providing mempool observations from globally distributed nodes. Xatu was initially pursued as a validation dataset. However, two practical obstacles prevented a full run: (1) approximately 75% of Xatu mempool senders are phantom addresses, wallets that broadcast high-fee transactions to all nodes but are never confirmed on-chain, requiring a per-block on-chain address lookup against `canonical_execution_address_appearances` to filter them; and (2) this lookup, combined with the Xatu mempool query volume, makes the pipeline significantly slower than BN. A full-year Xatu run at equivalent block coverage is estimated at ~30 days on a laptop. The Xatu mempool query filters to `gas_fee_cap IS NOT NULL` to avoid fetching legacy-format rows where `toUInt256(gas_fee_cap)` would resolve to 0 and be discarded downstream.

### 2.2 Inclusion List Construction

For each slot N, two independent ILs are built:

**Top Fee IL ($L^{tf}_N$):** The highest effective-priority-fee transactions visible in the mempool window $[T_N - 4s,\ T_N + 8s]$, packed greedily by size up to the 8 KiB EIP-7805 cap. This represents the standard "validator builds from best pending txs" strategy and approximates what an honest IL builder would submit.

**Censored IL ($L^{cs}_N$):** Transactions meeting the censorship criteria defined in §2.3, packed greedily. This IL contains only transactions suspected to have been deliberately or structurally excluded.

Both ILs are built at slot N and are then evaluated for redundancy at three enforcement delay levels (0, 1, 2 slots), giving six variants total.

### 2.3 Censorship Detection

A transaction is flagged as censored at slot N if it satisfies all of the following:

1. **Base fee eligible:** `max_fee_per_gas >= base_fee` of block N.
2. **Competitive fee:** Effective priority fee >= 50th percentile of all valid transactions in the censored window $[T_N - 12s,\ T_N]$. Effective priority fee is computed per EIP-1559 as $f_{eff} = \min(f_p,\ f_{max} - f_b)$, where $f_p$ is the priority fee, $f_{max}$ is max fee per gas, and $f_b$ is the block base fee.
3. **Dwell time:** First seen >= 0s and <= 12s before $T_N$ (capped to 1 slot to avoid overlap with the prior slot's window).
4. **Not a blob tx:** Transaction type != 3 (EIP-4844 blob transactions excluded).
5. **Not replaced:** Not a nonce replacement (detected via BN cancel/speedup status).
6. **Gas fits:** `gas_limit` <= remaining gas capacity in both block N-1 and block N. A transaction that physically could not have fit is not considered censored.
7. **Not on-chain:** Not included in any block through N.

### 2.4 Redundancy Measurement

"Redundancy" refers to IL transactions that are included canonically in the blocks following the slot at which the IL was built. This measures the fraction of IL bandwidth that would have been unnecessary because builders included those transactions on their own.

For a given IL $L$ built at slot $N$ and enforced with delay $D$:

$$\text{Redundant}(L, D) = \left\{ tx \in L \ \middle|\ tx \in \bigcup_{n=N+1}^{N+1+D} B_n \right\}$$

$$\text{Inclusion Rate}(L, D) = \frac{|\text{Redundant}(L, D)|}{|L|} \times 100\%$$

$$\text{UsefulBytes}(L, D) = \sum_{tx \in L} \text{size}(tx) \cdot [tx \notin \text{Redundant}(L, D)]$$

Where $B_n$ is the set of transactions in canonical block $n$. Both useful and redundant bytes are tracked directly per block. For BN data, `size(tx)` is estimated as calldata bytes plus 125 bytes of fixed overhead, as BN does not expose the full RLP-encoded transaction size. This affects absolute byte totals but not inclusion rate or redundancy percentage calculations.

The IL composition is identical across all delay levels; only the evaluation window differs. Delaying enforcement does not change which transactions are placed in the IL, only how many get included naturally before it takes effect.

---

## 3. Results

### 3.1 Dataset Overview (Block Native)

| Metric | Value |
| :--- | :--- |
| Blocks analyzed | 699,363 |
| Block range | 19,426,587 to 21,475,794 |
| Time period | 2024-03-13 to 2024-12-25 (286 days) |
| Avg gas utilization | 50.5% of limit |
| Avg base fee | 20.90 Gwei (median: 17.08 Gwei) |
| Mempool coverage of N+1 | 53.2% |

The 53.2% mempool coverage figure means that on average, just over half of the transactions included in a given block were observable in the BN mempool during the 12-second window around that block's timestamp. The remainder represents private orderflow, late-arriving transactions, and coverage gaps.

### 3.2 RQ1 and RQ2 - Top Fee IL Redundancy and Delay Effect

| Delay | Total (KiB/blk) | Useful (KiB/blk) | Redundant (KiB/blk) | Inclusion Rate | Ann. Total (GB) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 0-slot | 7.35 | **0.58** | 6.76 | 92.5% | 18.4 |
| 1-slot | 7.35 | **0.47** | 6.88 | 94.1% | 18.4 |
| 2-slot | 7.35 | **0.46** | 6.89 | 94.2% | 18.4 |

The Top Fee IL fills near-capacity at 7.35 KiB/block average against the 8 KiB cap.

**RQ1:** 92.5% of Top Fee IL transactions are redundant at 0-delay. Builders already include these transactions. Only 0.58 KiB/block represents genuine enforcement value.

**RQ2:** Delaying enforcement by 1 slot increases redundancy to 94.1% (+1.6 pp), reducing useful bytes by 19.7%. A 2-slot delay reaches 94.2% (+1.7 pp total), with negligible additional change beyond 1 slot. The total propagated IL bandwidth is 7.35 KiB/block (18.4 GB/year).

### 3.3 RQ1 and RQ2 - Censored IL Redundancy and Delay Effect

| Delay | Total (KiB/blk) | Useful (KiB/blk) | Redundant (KiB/blk) | Inclusion Rate | Ann. Total (GB) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 0-slot | 1.34 | **0.65** | 0.69 | 60.8% | 3.36 |
| 1-slot | 1.34 | **0.62** | 0.72 | 62.8% | 3.36 |
| 2-slot | 1.34 | **0.61** | 0.73 | 63.4% | 3.36 |

**RQ1:** 60.8% of Censored IL transactions are redundant at 0-delay. By bytes, 48.2% of the Censored IL (0.65 KiB/block) represents genuine enforcement value, as non-redundant transactions tend to be larger than redundant ones.

**RQ2:** Delaying enforcement by 1 slot increases redundancy to 62.8% (+2.0 pp), and by 2 slots to 63.4% (+2.6 pp). The total propagated IL bandwidth is 1.34 KiB/block (3.36 GB/year).

### 3.4 Censorship Detection

| Metric | Value |
| :--- | :--- |
| Avg censored txs/block | 6.71 |
| Median censored txs/block | 2 |
| Blocks with any detection | 496,738 (71.0%) |
| Blocks with 0 detections | 202,624 (29.0%) |
| Blocks with 1 detection | 106,284 (15.2%) |
| Blocks with 2 detections | 72,862 (10.4%) |
| Blocks with 3 detections | 53,073 (7.6%) |
| Blocks with 4 detections | 41,031 (5.9%) |

---

## 4. Limitations

**Mempool visibility ceiling (~53%):** Only 53.2% of next-block transactions were observable in the BN mempool window. The remainder (~47%) arrives via private orderflow channels (e.g., Flashbots MEV-Boost, direct builder submission). Censorship detection can only flag transactions visible in the public mempool; private-orderflow censorship is invisible to this methodology.

**Estimated transaction sizes (BN):** BlockNative provides `datasize` (calldata bytes) but not the full RLP-encoded transaction size. Tx size is estimated as `datasize + 125 bytes` overhead. This introduces noise into the per-block byte totals but does not affect inclusion rate measurements.

**Censorship Window:**
In an effort to remove overlapping TXs in ILs, since a truly censored TX in this study would appear in multiple ILs, the censorship window only looks at TXs from the last 12 seconds. This ensures that ILs are fresh and only contain new censored TXs. While not a perfect simulation for ILs, since in practice IL builders would look further back causing ILs to be larger, it gives us a better picture of IL metrics under the assumption that ILs would force-include censored TXs quickly due to the current low censorship environment on Ethereum today.

---

## 5. Conclusions

The contrast between the two IL strategies:

| | Top Fee IL | Censored IL |
| :--- | :--- | :--- |
| Baseline redundancy (RQ1) | 92.5% | 60.8% |
| Redundancy at 2-slot delay (RQ2) | 94.2% (+1.7 pp) | 63.4% (+2.6 pp) |
| Useful bytes/block (0-slot delay) | 0.58 KiB | 0.65 KiB |
| Total bandwidth/block | 7.35 KiB | 1.34 KiB |
| Annual bandwidth | 18.4 GB | 3.36 GB |

With these metrics, we now have a better look into how effective ILs would be in practice. The assumption that ILs built from TXs with high fees are redundant holds true. Over 90% of the TXs result in wasted bandwidth. More surprisingly, the redundancy in the censored TX ILs is high at ~61%. However, this is likely influenced by the difficulty of spotting censored TXs and the short dwell window used in this study.

At the beginning of this study, ideas were floated that we should exclusively use ILs with censored TXs in order to reduce bandwidth. This idea, while utopian, does not make sense in a trustless environment. There is an IL size limit for a reason, and there is no feasible way to police such a policy on-chain.

The question then becomes how we should construct ILs once FOCIL is added to Ethereum to minimize redundancy and the trade-off between UX and the ease of implementing FOCIL with respect to delays. It should be prioritized to ensure that ILs are enforced as soon as possible (0 delay), so we can reap the benefits of better UX for users and reduce redundancy. With that said, depending on the implementation of ePBS, a one-slot delay is tenable. It would only increase redundancy by ~2 pp and mostly affects UX.

The final implementation of IL building will likely be a combination of top fees and flagged censored TXs. This will make the ILs more effective at their stated goal of reducing censorship, but will likely not decrease redundancy by a large percentage. However, in a censorship-heavy environment, the redundancy may collapse rapidly as it becomes easier to spot censored TXs.

This was a brief study into the early landscape of IL building. As FOCIL nears mainnet, testnets and further research will help the community gain a more in-depth understanding of the forces at play in IL building.


---

## Appendix: Implementation Notes

| Parameter | Value |
| :--- | :--- |
| IL size cap | 8,192 bytes (EIP-7805) |
| Top Fee window | [T_N - 4s, T_N + 8s] |
| Censored window | [T_N - 12s, T_N] |
| Min dwell time | 0s |
| Max dwell time | 12s (capped to 1 slot) |
| Fee percentile threshold | 50th percentile of valid pending txs |
| Tx size estimation (BN) | datasize + 125 bytes overhead |
| Delay levels evaluated | 0, 1, 2 slots |
| Data | [Link](https://docs.blocknative.com/data-archive/mempool-archive) |
| Data source | BlockNative `block_native_mempool_transaction` |

**References:**
- [EIP-7805: Fork-Choice Enforced Inclusion Lists](https://eips.ethereum.org/EIPS/eip-7805)
- [Execution Specs #1928: FOCIL validation rules](https://github.com/ethereum/execution-specs/pull/1928)
- [Xatu documentation](https://ethpandaops.io/docs/tooling/xatu/)
- [Prysm bandwidth discussion](https://discord.com/channels/595666850260713488/1210529202458071050/1443658440609239151)

