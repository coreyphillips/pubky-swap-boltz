# Protocol and operations

## Required provider capabilities

The current Pubky offer must advertise `boltz-taproot-v1` and `swap-status-v1`, match the configured network and authenticated provider, and have a current expiry. Legacy P2WSH offers cannot be used for this profile.

Offer, quote, and status requests carry correlation IDs. Swap admission uses the persisted native quote ID. Acceptance replay is authorized by the provider's authenticated Pubky sender and compares the entire original request. The provider's public recovery snapshot contains its original acceptance and lifecycle data, never the full secret-bearing swap record.

The proxy requests `taproot_boltz` explicitly. Native clients that omit the script type continue using P2WSH.

## REST surface

| Endpoint | Behavior |
| --- | --- |
| `GET /health` | Checks the provider offer and chain tip |
| `GET /v2/version` | Reports the proxy version and named SDK profile |
| `GET /v2/nodes` | Provider Lightning public key, when advertised |
| `GET /v2/swap/submarine` | BTC pair, fees, limits, and current pair hash |
| `GET /v2/swap/reverse` | BTC pair, fees, limits, and current pair hash |
| `POST /v2/swap/submarine` | Invoice-first submarine creation |
| `POST /v2/swap/reverse` | Reverse creation using exactly one amount field |
| `GET /v2/swap/{id}` | Fresh authenticated status plus independent chain observations |
| `GET /v2/swap/submarine/{id}/transaction` | Observed funding transaction and refund timeout |
| `GET /v2/swap/reverse/{id}/transaction` | Reverse funding only when safe to expose |
| `GET /v2/chain/BTC/fee` | Live clamped fee estimate |
| `GET /v2/chain/BTC/height` | Chain tip |
| `GET /v2/chain/BTC/transaction/{txid}` | Transaction bytes by txid |
| `POST /v2/chain/BTC/transaction` | Broadcast a client-signed transaction |
| `GET /v2/ws` | Bounded `swap.update` subscriptions |
| `GET /v2/swap/chain` | Empty pair map |
| Other routes, assets, and cooperative operations | JSON `error` response |

Unknown creation fields are rejected. Invoice-later, referrals, descriptions, invoice expiry overrides, extra fees, address signatures, chain swaps, and non-Bitcoin assets are not silently ignored.

## Lifecycle mapping

Provider lifecycle state alone does not imply a confirmed Bitcoin transaction. The proxy independently observes the output and spend on its configured Electrum server.

| Evidence | External status |
| --- | --- |
| Submarine accepted, no funding | `invoice.set` |
| Reverse accepted, waiting for Lightning | `swap.created` |
| Reverse funding observed but unconfirmed or too close to refund | `invoice.paid`, without funding transaction details |
| Submarine funding unconfirmed | `transaction.mempool` |
| Eligible confirmed funding | `transaction.confirmed` |
| Confirmed submarine spend revealing the expected preimage | `transaction.claimed` |
| Confirmed reverse claim plus provider settlement completion | `invoice.settled` |
| Confirmed refund spend | `transaction.refunded` |
| Provider expired with no funded recovery pending | `swap.expired` |
| Provider failed with no independently observed active funding | `transaction.failed` |

No `transaction.claim.pending` cooperative-signing event is emitted. Polling may skip intermediate native states. WebSocket reconnects acknowledge the subscription and obtain fresh snapshots before sending transaction details. A slow consumer receives a fresh snapshot after bounded event-buffer overflow.

## Limits and trust

- One active creation per process; eight queued Pubky exchanges.
- HTTP JSON bodies up to 32 KiB.
- Up to 32 WebSocket connections, 100 swap IDs per connection, and 8 KiB command frames.
- A 128-event in-memory notification buffer; durable latest state lives in SQLite.
- Up to 10,000 retained swaps per state directory. Archive by moving a complete inactive directory; do not delete in-flight recovery state.
- Provider request timeout defaults to 60 seconds. Ambiguous admission failures must be recovered using the same request.
- Electrum is trusted for chain history and confirmation evidence. Its genesis block is checked against the configured network.
- Polling and backend outages can delay updates. Keep the original client's independent refund monitoring enabled.

The client must verify the returned Taproot contract and amount using its SDK. A localhost proxy cannot make a malicious provider or chain server trustworthy. The implementation validates both boundaries and preserves the client's independent recovery keys.
