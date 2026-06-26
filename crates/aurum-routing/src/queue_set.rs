use std::collections::BTreeSet;

use smallvec::SmallVec;
use aurum_types::{QueueId, QueueSetId, ShardId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct QueueTarget {
    pub shard_id: ShardId,
    pub queue_id: QueueId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueSetEntry {
    Empty,
    One(QueueTarget),
    Small(SmallQueueSet),
    ShardGrouped(ShardGroupedQueueSet),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SmallQueueSet {
    pub len: u8,
    pub targets: [QueueTarget; 4],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardQueueGroupOwned {
    pub shard_id: ShardId,
    pub queues: SmallVec<[QueueId; 4]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardGroupedQueueSet {
    pub groups: SmallVec<[ShardQueueGroupOwned; 4]>,
}

#[derive(Debug, Default)]
pub struct QueueSetStorage {
    sets: Vec<QueueSetEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueSetRef<'a> {
    Empty,
    One(QueueTarget),
    Small(&'a SmallQueueSet),
    ShardGrouped(&'a ShardGroupedQueueSet),
}

impl QueueSetStorage {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, entry: QueueSetEntry) -> QueueSetId {
        let id = QueueSetId(self.sets.len() as u32);
        self.sets.push(entry);
        id
    }

    #[must_use]
    pub fn get(&self, id: QueueSetId) -> Option<&QueueSetEntry> {
        self.sets.get(id.0 as usize)
    }

    #[must_use]
    pub fn get_ref<'a>(&'a self, id: QueueSetId) -> QueueSetRef<'a> {
        match self.get(id) {
            Some(QueueSetEntry::Empty) | None => QueueSetRef::Empty,
            Some(QueueSetEntry::One(t)) => QueueSetRef::One(*t),
            Some(QueueSetEntry::Small(s)) => QueueSetRef::Small(s),
            Some(QueueSetEntry::ShardGrouped(g)) => QueueSetRef::ShardGrouped(g),
        }
    }
}

pub struct QueueSetBuilder;

impl QueueSetBuilder {
    pub fn build(targets: &[QueueTarget]) -> QueueSetEntry {
        if targets.is_empty() {
            return QueueSetEntry::Empty;
        }
        if targets.len() == 1 {
            return QueueSetEntry::One(targets[0]);
        }

        let mut shard_groups: BTreeSet<ShardId> = BTreeSet::new();
        for t in targets {
            shard_groups.insert(t.shard_id);
        }

        if targets.len() <= 4 && shard_groups.len() == 1 {
            let mut arr = [QueueTarget {
                shard_id: ShardId(0),
                queue_id: QueueId(0),
            }; 4];
            for (i, t) in targets.iter().enumerate() {
                arr[i] = *t;
            }
            return QueueSetEntry::Small(SmallQueueSet {
                len: targets.len() as u8,
                targets: arr,
            });
        }

        let mut groups: SmallVec<[ShardQueueGroupOwned; 4]> = SmallVec::new();
        for shard_id in shard_groups {
            let mut queues: SmallVec<[QueueId; 4]> = SmallVec::new();
            for t in targets {
                if t.shard_id == shard_id {
                    if !queues.contains(&t.queue_id) {
                        queues.push(t.queue_id);
                    }
                }
            }
            groups.push(ShardQueueGroupOwned { shard_id, queues });
        }
        QueueSetEntry::ShardGrouped(ShardGroupedQueueSet { groups })
    }
}

impl QueueSetRef<'_> {
    pub fn for_each_target(&self, mut f: impl FnMut(QueueTarget)) {
        match self {
            Self::Empty => {}
            Self::One(t) => f(*t),
            Self::Small(s) => {
                for t in &s.targets[..usize::from(s.len)] {
                    f(*t);
                }
            }
            Self::ShardGrouped(g) => {
                for group in &g.groups {
                    for &queue_id in &group.queues {
                        f(QueueTarget {
                            shard_id: group.shard_id,
                            queue_id,
                        });
                    }
                }
            }
        }
    }

    #[must_use]
    pub fn target_count(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_) => 1,
            Self::Small(s) => usize::from(s.len),
            Self::ShardGrouped(g) => g.groups.iter().map(|grp| grp.queues.len()).sum(),
        }
    }

    #[must_use]
    pub fn targets_vec(&self) -> Vec<QueueTarget> {
        let mut out = Vec::new();
        self.for_each_target(|t| out.push(t));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(q: u32) -> QueueTarget {
        QueueTarget {
            shard_id: ShardId(0),
            queue_id: QueueId(q),
        }
    }

    #[test]
    fn empty_targets() {
        assert!(matches!(QueueSetBuilder::build(&[]), QueueSetEntry::Empty));
    }

    #[test]
    fn one_target() {
        let entry = QueueSetBuilder::build(&[target(1)]);
        assert!(matches!(entry, QueueSetEntry::One(t) if t.queue_id == QueueId(1)));
    }

    #[test]
    fn small_same_shard() {
        let entry = QueueSetBuilder::build(&[target(1), target(2)]);
        assert!(matches!(entry, QueueSetEntry::Small(s) if s.len == 2));
    }

    #[test]
    fn shard_grouped_multi_shard() {
        let entry = QueueSetBuilder::build(&[
            QueueTarget { shard_id: ShardId(0), queue_id: QueueId(1) },
            QueueTarget { shard_id: ShardId(1), queue_id: QueueId(2) },
        ]);
        assert!(matches!(entry, QueueSetEntry::ShardGrouped(_)));
    }
}
