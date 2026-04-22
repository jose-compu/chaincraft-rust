# Chaincraft Rust Protocol Specification v2

This document defines the Rust `0.4.x` protocol contract.

You implement protocol logic only. Chaincraft handles UDP networking, gossip,
deduplication, storage, peer management, and pipeline orchestration.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                           ChaincraftNode                         │
│                                                                  │
│  inbound datagram                                                 │
│     ├─ control path (direct): REQUEST_DIGEST / REQUEST_MESSAGES  │
│     └─ gossip path: SharedMessage                                │
│           ├─ dedupe check by hash                                │
│           ├─ process_message(msg)                                │
│           │    1) validate all objects (all-or-nothing)          │
│           │    2) linear apply pipeline                          │
│           │       add_message(msg, frontier_state?)              │
│           │       -> Option<StateMemento>                        │
│           │       -> emitted/fallback memento passed downstream  │
│           ├─ persist only on successful pipeline                 │
│           └─ rebroadcast                                          │
└──────────────────────────────────────────────────────────────────┘
```

## Core Abstractions

### `StateMemento`

Pipeline snapshot propagated object-to-object in one message pass.

```rust
use chaincraft::{StateMemento, normalize_state_memento};

let m: StateMemento = normalize_state_memento("tip_digest", Some(vec![
    "tip_digest".to_string()
]));
```

Fields:
- `canonical_digest`
- `frontier_digests`
- `revision`
- `metadata` (string map for reorg/catch-up hints)

### `ApplicationObject`

`ApplicationObject` is the protocol contract.

```rust
#[async_trait]
pub trait ApplicationObject {
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool>;
    async fn add_message(
        &mut self,
        message: SharedMessage,
        frontier_state: Option<StateMemento>,
    ) -> Result<Option<StateMemento>>;
    async fn emit_state_memento(&self) -> Result<StateMemento>;
    fn get_state_digests(&self) -> Vec<String>;
    // ... digest/sync/state/reset methods ...
}
```

Rules:
- `is_valid` must be deterministic and side-effect free.
- `add_message` mutates state and can emit a new memento.
- If `add_message` emits `None`, the runtime uses `emit_state_memento()`.

### `SharedMessage`

Transport envelope exchanged over network:
- `message_type`, `data`, `timestamp`, `hash`, `signature`, etc.

## Pipeline Semantics (v2)

`ApplicationObjectRegistry::process_message`:

1. Validate message against **all** registered objects.
2. If any object rejects, abort pipeline (message rejected).
3. If all valid, process objects sequentially in registration order.
4. Pass `frontier_state` memento from one object to the next.

This gives deterministic multi-object state transitions and supports
reorg/catch-up coordination between related objects.

## Storage/Broadcast Ordering

For both local and inbound gossip messages:
- Run validation + apply pipeline first.
- Persist and broadcast only if pipeline succeeds.

This avoids committing invalid state transitions to storage.

## Direct P2P / Control Path

Non-gossip control messages (`REQUEST_DIGEST`, `REQUEST_MESSAGES_SINCE`,
responses) are direct UDP request/response messages.

They are not regular gossip fanout objects, but replayed `SharedMessage`s from
sync responses are still routed through the same v2 pipeline.

## Merkelized vs Non-Merkelized

Use merkelized objects when canonical/frontier digests matter (chain, DAG,
ledger snapshots). Use non-merkelized objects for eventually-consistent queues
or caches where digest proofs are unnecessary.

## Rust Core Objects

Core object families:
- merkelized: `MerkelizedObject`, `UTXOLedger`, `BalanceLedger`, `Blockchain`,
  `DAGObject`, `TransactionChain`
- non-merkelized: `NonMerkelizedObject`, `CacheObject`, `Mempool`,
  `DocumentCache`

Aliases:
- `MerkleizedObject` -> `MerkelizedObject`
- `NativeSharedObject` -> `CoreSharedObject`

## Examples (v2 contract)

Protocol modules under `src/examples/` implement the v2 signature:
- `chatroom`, `ecdsa_ledger`, `randomness_beacon`
- `slush`, `snowflake`, `snowball`
- `tendermint`
- `blockchain` (memento-aware chain example)

Runnable examples under `examples/` use typed wrappers and node lifecycle:
- `chatroom_example`
- `ecdsa_ledger_example`
- `randomness_beacon_example`
- `slush_example`
- `snowflake_example`
- `snowball_example`
- `blockchain_example`

## Minimal Usage

```rust
use chaincraft::{ChaincraftNode, PeerId, storage::MemoryStorage, BalanceLedger};
use std::sync::Arc;

let mut node = ChaincraftNode::new(PeerId::new(), Arc::new(MemoryStorage::new()));
node.add_shared_object(Box::new(BalanceLedger::new())).await?;
node.start().await?;
node.create_shared_message_with_data(serde_json::json!({
    "type": "MY_EVENT",
    "payload": {"x": 42}
})).await?;
```

## Protocol Author Checklist

- Keep `is_valid` strict and side-effect free.
- Return stable mementos from `add_message` when your object is canonical owner.
- Implement `get_state_digests` for multi-head structures (DAG/forks).
- Do not reimplement sockets, gossip fanout, or node threading.
