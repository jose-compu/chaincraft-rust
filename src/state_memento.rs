use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Immutable snapshot propagated between ApplicationObjects in one pipeline pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StateMemento {
    pub canonical_digest: String,
    pub frontier_digests: Vec<String>,
    pub revision: u64,
    pub metadata: BTreeMap<String, Value>,
}

impl StateMemento {
    /// Detect canonical replacement between two snapshots.
    pub fn indicates_reorg_against(&self, previous: Option<&StateMemento>) -> bool {
        let Some(previous) = previous else {
            return false;
        };
        if previous.canonical_digest.is_empty() || self.canonical_digest.is_empty() {
            return false;
        }
        if self.canonical_digest == previous.canonical_digest {
            return false;
        }
        !self
            .frontier_digests
            .iter()
            .any(|digest| digest == &previous.canonical_digest)
    }
}

/// Create a normalized memento where frontier digests are unique and sorted.
pub fn normalize_state_memento(
    canonical_digest: &str,
    frontier_digests: Option<Vec<String>>,
) -> StateMemento {
    let mut digests = frontier_digests.unwrap_or_default();
    if !canonical_digest.is_empty() && !digests.iter().any(|d| d == canonical_digest) {
        digests.push(canonical_digest.to_string());
    }
    digests.retain(|d| !d.is_empty());
    digests.sort();
    digests.dedup();

    StateMemento {
        canonical_digest: canonical_digest.to_string(),
        frontier_digests: digests,
        revision: 0,
        metadata: BTreeMap::new(),
    }
}
