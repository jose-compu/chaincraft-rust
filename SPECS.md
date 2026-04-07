# Chaincraft Rust Protocol Specification v1

This document describes how to implement protocols with `chaincraft-rust`.

You write protocol/application logic; Chaincraft runtime handles UDP networking,
message propagation, deduplication, storage, peer tracking, and background tasks.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                            ChaincraftNode                            │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ start_networking()                                             │  │
│  │ - UDP receive loop                                             │  │
│  │ - gossip rebroadcast loop                                      │  │
│  │ - direct control/P2P path (non-gossip sync and requests)       │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  Incoming UDP datagram                                               │
│   ├── SharedMessage gossip path                                      │
│   │    ├── dedupe by hash + store                                    │
│   │    ├── app_objects.process_message(msg)                          │
│   │    │    ├── object.is_valid(msg)                                 │
│   │    │    └── if valid => object.add_message(msg)                  │
│   │    └── rebroadcast to peers                                      │
│   │                                                                  │
│   └── Direct control/P2P path (no gossip fanout)                     │
│        └── REQUEST_DIGEST / REQUEST_MESSAGES_SINCE / responses       │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Core Abstractions

### `SharedMessage`

Transport envelope exchanged between nodes as JSON.

Key fields:

- `id: SharedObjectId`
- `message_type: MessageType`
- `target_id: Option<SharedObjectId>`
- `data: serde_json::Value`
- `timestamp`
- `signature: Option<Vec<u8>>`
- `hash`

Create messages with:

```rust
use chaincraft::shared::{MessageType, SharedMessage};

let msg = SharedMessage::new(
    MessageType::Custom("MY_VOTE".to_string()),
    serde_json::json!({"round": 7, "vote": "yes"}),
);
```

### `ApplicationObject`

Protocols are implemented as `ApplicationObject` implementations.

Main methods:

- `is_valid(&self, message) -> Result<bool>`
- `add_message(&mut self, message) -> Result<()>`
- `is_merkleized() -> bool`
- digest methods (`get_latest_digest`, `has_digest`, `is_valid_digest`, `add_digest`)
- sync methods (`gossip_messages`, `get_messages_since_digest`)
- lifecycle/state (`get_state`, `reset`)

### `ChaincraftNode`

Runtime entrypoint for networking and protocol dispatch.

Important methods:

- `start()` / `close()`
- `connect_to_peer("host:port")`
- `add_shared_object(Box<dyn ApplicationObject>)`
- `create_shared_message_with_data(serde_json::Value)`
- `create_shared_message(String)` (compatibility helper)

## Message Types and P2P Control Messages

`MessageType` supports both application messages and protocol control plane messages.

Built-in control types include:

- `PEER_DISCOVERY`, `REQUEST_LOCAL_PEERS`, `LOCAL_PEERS`
- `REQUEST_SHARED_OBJECT_UPDATE`, `SHARED_OBJECT_UPDATE`
- `REQUEST_DIGEST`, `DIGEST_RESPONSE`
- `REQUEST_MESSAGES_SINCE`, `MESSAGES_RESPONSE`
- `GET`, `SET`, `DELETE`, `RESPONSE`, `NOTIFICATION`, `HEARTBEAT`, `ERROR`

For protocol-specific traffic, use `MessageType::Custom("...")`.

Example data payload with explicit type:

```rust
let data = serde_json::json!({
    "type": "SLUSH_VOTE",
    "round": 3,
    "color": "red"
});
node.create_shared_message_with_data(data).await?;
```

## Message Flow (Gossip/Data Path)

### Outbound (local create)

`create_shared_message_with_data(data)` performs:

1. Resolve `MessageType` from `data["type"]` (or default custom type).
2. Build `SharedMessage`.
3. Store message in node storage.
4. Run `ApplicationObjectRegistry::process_message`.
5. Broadcast JSON payload over UDP to known peers.

### Inbound (from peer datagram)

Receive loop performs:

1. Parse control/digest messages first.
2. Else parse datagram as `SharedMessage`.
3. Deduplicate by `hash` (skip if already stored).
4. Store in node storage.
5. Dispatch via `process_message`.
6. Re-broadcast to peers for propagation.

## Request/Response and Sync Path

Rust runtime includes a control-plane request/response path over UDP JSON:

- `REQUEST_DIGEST` -> `DIGEST_RESPONSE`
- `REQUEST_MESSAGES_SINCE` -> `MESSAGES_RESPONSE`

When digests differ, peers request missing messages and apply them through the
same `process_message` path used by normal gossip.

This is the Rust equivalent of direct synchronization traffic: messages are
point-to-point UDP exchanges but still represented in JSON and integrated with
object-level digest methods.

## Validation and Processing Semantics

`ApplicationObjectRegistry::process_message` evaluates each registered object
sequentially:

1. `object.is_valid(&message)`
2. If valid for that object, call `object.add_message(message.clone())`
3. Continue to next object

Important: processing is per-object. A rejection in one object does not prevent
other objects from processing if they validate the same message.

## Merkelized vs Non-Merkelized Objects

Use merkelized objects when digest-based state sync matters:

- chains
- DAGs
- ledgers/state snapshots

Use non-merkelized objects when gossip plus hash deduplication is enough:

- mempool queues
- transient caches
- loosely ordered event streams

For non-merkelized objects, keep digest methods simple/minimal and return empty
sync payloads as appropriate.

## Rust Core Objects

The crate exposes Python-aligned core objects:

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

## Implementing a Protocol

### 1) Define an `ApplicationObject`

- Keep structural/state checks in `is_valid`.
- Keep mutations in `add_message`.
- Route by `message.message_type` or `message.data["type"]` inside `add_message`.

### 2) Register object(s) on a node

```rust
use chaincraft::{network::PeerId, storage::MemoryStorage, BalanceLedger, ChaincraftNode};
use std::sync::Arc;

let storage = Arc::new(MemoryStorage::new());
let mut node = ChaincraftNode::new(PeerId::new(), storage);
node.add_shared_object(Box::new(BalanceLedger::new())).await?;
```

### 3) Start networking and connect peers

```rust
node.start().await?;
node.connect_to_peer("127.0.0.1:9001").await?;
```

### 4) Publish protocol messages

```rust
node.create_shared_message_with_data(serde_json::json!({
    "type": "MY_PROTOCOL_EVENT",
    "payload": {"x": 42}
})).await?;
```

## Threading and Concurrency Guidance

- Do not manage sockets or manual networking in protocol objects.
- Do not create parallel protocol runtimes for message handling.
- Keep object logic deterministic and idempotent where possible.
- If external tasks touch shared protocol state, protect internal invariants.

## What Protocol Authors Should Not Reimplement

- UDP gossip fanout
- message hash deduplication
- peer discovery bookkeeping
- digest-sync transport exchange
- node lifecycle orchestration

Use the provided node/runtime APIs and focus on protocol-specific state machines.
