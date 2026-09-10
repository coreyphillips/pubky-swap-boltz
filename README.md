# pubky-swap-boltz

A Rust library and localhost proxy that lets the supported Boltz SDK negotiate Bitcoin swaps with a Pubky Swap provider.

```text
Boltz SDK -> localhost REST / WebSocket -> encrypted Pubky messages -> Pubky Swap provider
```

The proxy owns a Pubky messaging identity and public swap records. The original client owns its claim/refund private keys and constructs its own recovery transactions. The provider continues to run its existing funding, Lightning settlement, and recovery drivers. No Boltz HTTP routes are added to the provider.

This is an experimental, Bitcoin-only compatibility profile for the official Go package `github.com/BoltzExchange/boltz-client/v2/pkg/boltz` at **v2.12.5**, with Taproot reference vectors from **boltz-core 5.0.0**. It does not claim support for the complete Boltz daemon or every application using Boltz. Read the [compatibility contract](docs/compatibility.md) before integrating.

## What it does

- BTC submarine swaps with an existing BOLT11 invoice and client refund public key.
- BTC reverse swaps with a client payment hash and claim public key.
- Independent validation of quotes, invoices, Taproot trees, addresses, amounts, keys, and timeouts.
- Durable request replay, stable IDs, and recovery after an interrupted creation response.
- Boltz pair discovery, status lookup, transaction helpers, and WebSocket subscriptions.
- Confirmation and remaining-time checks before giving a reverse client a claim-triggering update.
- Deterministic errors for unsupported assets, chain swaps, and cooperative signing.

The SDK can fall back from cooperative reverse signing to a unilateral Taproot claim. Timed refunds remain available through the refund leaf. Cooperative fee savings and immediate cooperative refunds are not part of this version.

## Run locally

You need Rust, a compatible LND-backed Pubky Swap provider, an existing Pubky identity, and an Electrum server on the same Bitcoin network. Start with regtest. Mainnet operation has not been validated by this project's test suite.

The upstream support is in [pubky-swap PR #46](https://github.com/coreyphillips/pubky-swap/pull/46). This project's manifest pins commit `3b134ec48496d6f02bd43bd7b778d28a1992a630`. Run a provider containing that change. Provider build and node configuration are covered by its [contributing guide](https://github.com/coreyphillips/pubky-swap/blob/3b134ec48496d6f02bd43bd7b778d28a1992a630/CONTRIBUTING.md).

```sh
cp config.example.toml config.toml
cargo build --locked --release
./target/release/pubky-swap-boltz-proxy --config config.toml
```

Set the provider Pubky and identity file paths in the configuration. The provider must already follow the proxy's Pubky identity so it polls its messages. This version does not automatically register a new identity or ring the optional iroh discovery doorbell.

Point the supported SDK's API URL at `http://127.0.0.1:9001`. WebSocket connections use `ws://127.0.0.1:9001/v2/ws`.

Keep the state directory across restarts. It is bound to one Pubky identity, provider, and network. A second process cannot open the same directory. Switching providers or networks requires a separate directory and does not move existing swaps.

## Library

The library exposes `Bridge`, `Provider`, `Chain`, and `Store`. An application supplies its Pubky and chain adapters, constructs a `Bridge`, and calls the negotiation methods directly or mounts `http::router`. The HTTP layer does not contain Bitcoin signing code. See the [fixture server](examples/fixture_server.rs) for composition and the [integration tests](tests/bridge.rs) for failure and recovery examples.

The production `PubkyProvider` runs a single bounded inbox on a dedicated runtime. Correlated replies cannot be consumed by competing HTTP requests. Creation is serialized per local identity; a concurrent creation can receive HTTP 429 and should retry the same request.

## Recovery and privacy

Creation saves the exact native request before sending it. If the reply is lost, retry the identical JSON request. The proxy reuses the native quote and swap mapping instead of creating a second liability. An optional `Idempotency-Key` header binds the key to that request. Reusing a payment hash for different parameters is rejected.

If the provider restarted before admitting a request and no longer has its quote, the proxy reports that no durable admission exists. It does not automatically reprice or replace that request. Start a new swap with a new invoice or payment hash. An already admitted swap is recovered through the provider's persisted acceptance.

Pending hold-invoice recovery is implemented and tested with LND. Other Lightning backends that do not implement that lookup are outside this release's recovery profile.

SQLite uses a write-ahead log and full synchronization. The directory is private to its local user and contains invoices and public swap information. It contains no client branch private keys. Back up the original client's recovery material separately; backing up the proxy cannot replace that material.

A default cooperative reverse claim attempt from the pinned SDK includes its preimage in the HTTP body. The proxy rejects this route without polling, parsing, storing, or forwarding that body. Select the SDK's unilateral path before calling it if the preimage must never reach the proxy process at all.

The binary binds only to loopback. Browser origins and unexpected Host headers are rejected. There is no public hosted mode, CORS mode, or authentication multiplexing layer. Run one identity and state directory per end user.

## Test

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```

The [conformance guide](docs/compatibility.md#running-conformance-tests) runs the pinned official SDK against the real HTTP router and checks Taproot script spends. The upstream provider also has real two-node Lightning regtest tests for Taproot submarine completion, reverse completion, and timeout refunds. These are separate layers of evidence. The HTTP fixture does not prove that encrypted Pubky transport completed a funded swap.

See [protocol and endpoint notes](docs/operations.md) for the exact lifecycle mapping and operational limits.

## License

MIT.
