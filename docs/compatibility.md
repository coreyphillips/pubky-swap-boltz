# Boltz compatibility profile

This project targets the Bitcoin submarine and reverse swap interfaces in the official Go library `github.com/BoltzExchange/boltz-client/v2/pkg/boltz` at `v2.12.5`. It exposes these interfaces through a separate local Pubky client. The Pubky swap provider remains responsible for negotiation, funding, settlement, and recovery.

The target is the unchanged SDK package, with its endpoint pointed at the proxy. This is a bounded compatibility profile, not a claim that every Boltz application or the complete `boltz-client` daemon works without configuration changes.

## Pinned references

| Reference | Exact version | Commit |
| --- | --- | --- |
| Official Go SDK | `v2.12.5` | `1eab2ade0d817af0091051f0cc9e014124d93e36` |
| Official TypeScript reference library | `boltz-core@5.0.0` | `7612d67102ad370673c26be000ecc8804a5bec45` |

The Go test module and npm lockfile pin the dependencies used for verification. The TypeScript reference library is a test dependency only.

## Supported subset

- Bitcoin to Bitcoin swaps on the configured Bitcoin network.
- Submarine creation with an already generated BOLT11 invoice and a client refund public key.
- Reverse creation with a payment hash, client claim public key, and Lightning invoice amount.
- Pair discovery, creation responses, status lookup, and `swap.update` WebSocket subscription snapshots and updates.
- Taproot trees whose leaves and address can be independently checked by the official SDK.
- Unilateral Taproot claims and refunds constructed and signed by the original client.

Chain swaps, other assets, invoice-later submarine creation, cooperative signing, and the complete daemon API are outside this profile. Unsupported operations return a JSON error. They must not be advertised as available pairs.

## Fee translation

Native provider fees are computed on the requested swap output amount. Reverse creation inverts that integer fee schedule to preserve the exact requested Lightning invoice amount. Some invoice amounts fall into rounding gaps and cannot be represented. In that case the proxy rejects the request rather than changing what the client pays; the caller can use the supported `onchainAmount` form instead.

Boltz's displayed reverse percentage applies to the Lightning input, while the native percentage applies to the onchain output. The advertised reverse percentage is therefore an approximation of the native schedule. Creation responses and the signed invoice remain authoritative, and the proxy verifies their exact amounts against the native quote. The fixture uses zero fees to isolate transport and script conformance; Rust tests separately cover integer fee translation.

## Exact onchain format

Both leaves use Tapscript version `192`. Public keys inside leaves are x-only. The payment hash is `SHA256(preimage)`.

Submarine claim leaf:

```text
OP_HASH160 <RIPEMD160(payment_hash)> OP_EQUALVERIFY
<provider_claim_public_key> OP_CHECKSIG
```

Reverse claim leaf:

```text
OP_SIZE <32> OP_EQUALVERIFY
OP_HASH160 <RIPEMD160(payment_hash)> OP_EQUALVERIFY
<client_claim_public_key> OP_CHECKSIG
```

Refund leaf in either direction:

```text
<refund_public_key> OP_CHECKSIGVERIFY
<timeout_block_height> OP_CHECKLOCKTIMEVERIFY
```

The internal key aggregates compressed public keys in provider-first, client-second order with MuSig2 key sorting disabled. The output key applies the BIP341 Taproot tweak over that internal key and the two-leaf Merkle root. Supporting this address construction does not imply support for cooperative MuSig2 signing.

The response represents each leaf as `{ "version": 192, "output": "hex_script" }` under `swapTree.claimLeaf` and `swapTree.refundLeaf`.

These details are checked against the official [Go tree implementation](https://github.com/BoltzExchange/boltz-client/blob/1eab2ade0d817af0091051f0cc9e014124d93e36/pkg/boltz/swaptree.go), [submarine tree](https://github.com/BoltzExchange/boltz-core/blob/7612d67102ad370673c26be000ecc8804a5bec45/lib/swap/SwapTree.ts), and [reverse tree](https://github.com/BoltzExchange/boltz-core/blob/7612d67102ad370673c26be000ecc8804a5bec45/lib/swap/ReverseSwapTree.ts).

## Independent confirmation checks

Reverse funding transaction details are exposed only after the configured confirmation requirement is met, the exact funding outpoint has no observed spend, and more than the required claim window remains before refund eligibility. A provider's claimed state alone cannot produce a successful completion status. Confirmed spend evidence and the expected preimage are checked independently against the configured Electrum server. The server is trusted for chain history and confirmation evidence; its genesis block must match the configured network.

History classification ignores dust and unrelated outputs, rejects ambiguous eligible funding, and tracks spends of the exact output. WebSocket reconnect snapshots refresh their evidence before sending transaction details. During an outage or when a reverse output is near timeout or already spent, the proxy withholds a new claim-triggering funding update. The original client must retain its own keys and refund monitoring.

## Cooperative fallback and preimage handling

The pinned official SDK automatically reconstructs a reverse claim as a script-path spend when the cooperative claim endpoint returns an error. The interoperability suite exercises this behavior without changing the SDK. A client can instead request a unilateral transaction by passing `Cooperative: false` to the library's existing transaction constructor.

A default cooperative reverse claim attempt includes the preimage in its HTTP request. The proxy refuses cooperative signing. Clients that require the preimage to stay entirely out of the proxy's HTTP process must select the unilateral path before making that request. The proxy does not need the reverse preimage or any client private key to create swaps. Do not claim that an unchanged default cooperative client never transmits its preimage.

Submarine refunds have a different fallback rule. The SDK transaction constructor reports a cooperative refund error rather than immediately constructing a timelocked refund. A unilateral refund must use the refund leaf, an input sequence below the final sequence, and a transaction locktime at least equal to the swap timeout. The full daemon selects this path when the timeout has expired. Immediate cooperative refunds are not supported by this profile.

The authoritative fallback branches are in [the official transaction constructor](https://github.com/BoltzExchange/boltz-client/blob/1eab2ade0d817af0091051f0cc9e014124d93e36/pkg/boltz/transaction.go) and [daemon timeout selection](https://github.com/BoltzExchange/boltz-client/blob/1eab2ade0d817af0091051f0cc9e014124d93e36/internal/nursery/listener.go).

## WebSocket behavior

Clients connect to `/v2/ws` and send:

```json
{"op":"subscribe","channel":"swap.update","args":["swap-id"]}
```

The proxy acknowledges the subscription before sending current status snapshots:

```json
{"event":"subscribe","channel":"swap.update","args":["swap-id"]}
{"event":"update","channel":"swap.update","args":[{"id":"swap-id","status":"swap.created"}]}
```

The official SDK requires an acknowledgement within five seconds. Subsequent updates use the same update envelope. Status transaction details, when present, use `transaction.id` and `transaction.hex`.

## Running conformance tests

From `tests/conformance`, run:

```sh
go test -v ./...
npm ci --ignore-scripts
npm run test:vectors
```

Go `1.26.2` and a C compiler are required by the pinned SDK dependencies. Go's automatic toolchain download can provide the required Go version. Node `20.19` or newer is required for vector regeneration.

The local Go tests also run an HTTP server that refuses cooperative signing and verify the unchanged SDK automatically falls back. They confirm that the default cooperative request contains the preimage.

The local Go tests deserialize the independent reference vectors through the official SDK, check both trees and addresses, compare control blocks, construct unilateral claim and refund transactions, and execute the resulting witnesses in Bitcoin's script interpreter. They also verify that invalid preimages and premature refund locktimes fail. The Node test regenerates both vectors using the pinned reference library and compares every stored value.

Start the Rust fixture server from the project root:

```sh
cargo run --example fixture_server -- 127.0.0.1:9001
```

In another terminal, run the SDK tests from `tests/conformance` with its URL:

```sh
PUBKY_BOLTZ_TEST_URL=http://127.0.0.1:9001 go test -run TestProxyOfficialSDK -v
```

This test calls the actual proxy through the unmodified SDK, validates both creation responses and the reverse BOLT11 invoice, checks version, fees, height, discovery, idempotent replay, broadcast responses and WebSocket snapshots, rejects unsupported assets and chain swaps, and proves the SDK's automatic script-path reverse-claim fallback. Without this environment variable, that HTTP test is explicitly skipped.

The fixture server is deterministic test infrastructure. It does not perform Lightning payments, broadcast transactions, or prove a complete live swap. Production readiness additionally requires funded regtest coverage across provider negotiation, Lightning hold settlement, onchain monitoring, proxy restart recovery, and refund recovery.
