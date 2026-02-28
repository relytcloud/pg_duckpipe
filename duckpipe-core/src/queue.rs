//! Per-table change queues for decoupling WAL decoding from change application.

use std::collections::VecDeque;

use crate::types::Change;

/// Per-table change queue with batch metadata needed for SQL generation.
#[derive(Debug)]
pub struct TableQueue {
    pub target_key: String,
    pub mapping_id: i32,
    pub attnames: Vec<String>,
    pub key_attrs: Vec<usize>,
    pub atttypes: Vec<u32>,
    changes: VecDeque<Change>,
    /// Highest LSN of any change in this queue.
    pub last_lsn: u64,
}

impl TableQueue {
    pub fn new(
        target_key: String,
        mapping_id: i32,
        attnames: Vec<String>,
        key_attrs: Vec<usize>,
        atttypes: Vec<u32>,
    ) -> Self {
        Self {
            target_key,
            mapping_id,
            attnames,
            key_attrs,
            atttypes,
            changes: VecDeque::new(),
            last_lsn: 0,
        }
    }

    /// Append a change to the queue.
    pub fn push(&mut self, change: Change) {
        if change.lsn > self.last_lsn {
            self.last_lsn = change.lsn;
        }
        self.changes.push_back(change);
    }

    /// Drain all changes from the queue, returning them as a Vec.
    pub fn drain(&mut self) -> Vec<Change> {
        self.changes.drain(..).collect()
    }

    /// Number of changes currently queued.
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}
