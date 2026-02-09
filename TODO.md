# TODO

## Active Sender Filter — Revisit

The censorship detection currently requires a transaction's sender to have at
least one included tx in the prior 10 blocks. This was added to filter
phantom/spam transactions (~70% of FOCIL-valid mempool candidates are never
mined in any nearby block), and it reduced the censored candidate count from
1,519 to 553 per block.

**Problem**: the filter is too broad. It excludes:
- First-time senders (new wallets, contract deployments)
- Low-frequency users who haven't transacted in 10 blocks (~2 minutes)
- Legitimately censored senders who are being targeted precisely because
  they have no recent inclusion history (e.g., OFAC-sanctioned addresses)

**Alternatives to investigate**:
- Filter on tx_type == 2 (EIP-1559) only — prior analysis showed this gets
  inclusion from 0% to ~12.5% for Top Fee, and phantom txs are
  disproportionately type 0 (legacy). Simpler, less biased.
- Multi-node observation spread (seen by >= 2 nodes, or spread >= 1s) —
  real txs propagate, spam often appears briefly from one node
- Sender nonce continuity — if a sender's nonce is far ahead of their
  on-chain nonce, the tx can't be included regardless of fees
- Combination: type 2 + spread >= 1s might be sufficient without sender
  history requirement
- Cross-reference against known bot contract addresses (top `to` addresses
  in phantom set cluster around specific contracts)

**Data available**: tx_type, sender, nonce, seen_timestamp (min/max across
nodes gives spread), `to` address (would need adding to mempool query)
