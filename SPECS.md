# Chaincraft Rust Protocol Specification v1

This document describes how to implement a protocol in `chaincraft-rust`.

You implement protocol logic; Chaincraft handles networking, gossip, storage,
peer management, and runtime concurrency.

## Core Abstractions

### SharedMessage

`SharedMessage` is the transport envelope for JSON data exchanged between nodes.

```rust
use chaincraft::shared::{MessageType, SharedMessage};

let msg = SharedMessage::new(
    MessageType::Custom("MY_VOTE".to_string()),
    serde_json::json!({"value": 42}),
);
```

### ApplicationObject

Protocols are implemented as `ApplicationObject` objects.

Key methods:

- `is_valid(&self, message)` for validation
- `add_message(&mut self, message)` for state transition
- `is_merkleized()` and digest methods for sync strategy
- `gossip_messages()` / `get_messages_since_digest()` for sync deltas

### ChaincraftNode

`ChaincraftNode` is the runtime:

- receives messages
- stores and broadcasts accepted messages
- dispatches each message to registered application objects

## Message Flow

For incoming or locally-created gossip messages:

1. Build or parse `SharedMessage`.
2. Pass through registered application objects.
3. Store in node storage.
4. Broadcast to peers.

Each object should keep protocol routing inside `add_message` and call private
helpers for concrete state transitions.

## Merkelized vs Non-Merkelized Objects

Use merkelized objects when digest-based synchronization matters (chains, DAGs,
state ledgers).

Use non-merkelized objects for eventually-consistent, gossip-only state where
digest state proofs are unnecessary (cache/mempool-like queues).

## Rust Core Objects

The Rust crate exposes Python-aligned core objects:

- Merkelized:
  - `MerkelizedObject`
  - `UTXOLedger`
  - `BalanceLedger`
  - `Blockchain`
  - `DAGObject`
  - `TransactionChain`
- Non-merkelized:
  - `NonMerkelizedObject`
  - `CacheObject`
  - `Mempool`
  - `DocumentCache`

Compatibility aliases:

- `MerkleizedObject` -> `MerkelizedObject`
- `NativeSharedObject` -> `CoreSharedObject`

## Object Registration

```rust
use chaincraft::{
    network::PeerId,
    storage::MemoryStorage,
    BalanceLedger, ChaincraftNode,
};
use std::sync::Arc;

let storage = Arc::new(MemoryStorage::new());
let mut node = ChaincraftNode::new(PeerId::new(), storage);
node.add_shared_object(Box::new(BalanceLedger::new())).await?;
```

## Guidance

- Keep validation logic strict in `is_valid`.
- Keep state mutation in `add_message`.
- Use digest methods only for merkelized objects.
- Avoid direct network/thread management in protocol objects.
