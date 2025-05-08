use std::cell::Cell;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;
use std::collections::LinkedList;

use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, KeyValueIterator};
use crate::types::RowEntry;
use crate::utils::WatchableOnceCell;

use crate::mem_table::KVTableInternalKey;
use crate::mem_table::KVTableInternalKeyRange;
use crate::mem_table::MemTableIterator;
use crate::mem_table::MemTableIteratorInner;
use crate::mem_table::MemTableIterator;
use crate::mem_table::MemTableIterator;

pub(crate) struct KVArray {
  map: Arc<Vec<(KVTableInternalKey, RowEntry)>>,
  durable: WatchableOnceCell<Result<(), SlateDBError>>,
  entries_size_in_bytes: AtomicUsize,
  /// this corresponds to the timestamp of the most recent
  /// modifying operation on this KVTable (insertion or deletion)
  last_tick: AtomicI64,
  /// the sequence number of the most recent operation on this KVTable
  last_seq: AtomicU64,
}

impl SegmentIterator {
  pub(crate) fn next_entry_sync(&mut self) -> Option<RowEntry> {
      let ans = self.borrow_item().clone();
      let next_entry = match self.borrow_ordering() {
          IterationOrder::Ascending => self.with_inner_mut(|inner| inner.next()),
          IterationOrder::Descending => self.with_inner_mut(|inner| inner.next_back()),
      };

      let cloned_entry = next_entry.map(|entry| entry.value().clone());
      self.with_item_mut(|item| *item = cloned_entry);

      ans
  }
}

#[self_referencing]
pub(crate) struct SegmentIteratorInner<T: RangeBounds<KVArrayInternalKey>> {
    map: Arc<Vec<(KVTableInternalKey, RowEntry)>>,
    /// `inner` is the Iterator impl of SkipMap, which is the underlying data structure of MemTable.
    #[borrows(map)]
    #[not_covariant]
    inner: Range<'this, KVTableInternalKey, T, KVTableInternalKey, RowEntry>,
    ordering: IterationOrder,
    item: Option<RowEntry>,
}
pub(crate) type SegmentIterator = SegmentIteratorInner<KVArrayInternalKeyRange>;

#[async_trait]
impl KeyValueIterator for SegmentIterator {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.next_entry_sync())
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        loop {
            let front = self.borrow_item().clone();
            if front.is_some_and(|record| record.key < next_key) {
                self.next_entry_sync();
            } else {
                return Ok(());
            }
        }
    }
}

impl KVArray {
  pub(crate) fn new() -> Self {
      Self {
          map: Arc::new(Vec::new()),
          entries_size_in_bytes: AtomicUsize::new(0),
          durable: WatchableOnceCell::new(),
          last_tick: AtomicI64::new(i64::MIN),
          last_seq: AtomicU64::new(0),
      }
  }

  pub(crate) fn metadata(&self) -> KVTableMetadata {
      let entry_num = self.map.len();
      let entries_size_in_bytes = self.entries_size_in_bytes.load(Ordering::Relaxed);
      let last_tick = self.last_tick.load(SeqCst);
      let last_seq = self.last_seq.load(SeqCst);
      KVTableMetadata {
          entry_num,
          entries_size_in_bytes,
          last_tick,
          last_seq,
      }
  }

  pub(crate) fn is_empty(&self) -> bool {
      self.map.is_empty()
  }

  pub(crate) fn last_tick(&self) -> i64 {
      self.last_tick.load(SeqCst)
  }

  pub(crate) fn last_seq(&self) -> Option<u64> {
      if self.is_empty() {
          None
      } else {
          let last_seq = self.last_seq.load(SeqCst);
          Some(last_seq)
      }
  }

  /// Get the value for a given key.
  /// Returns None if the key is not in the memtable at all,
  /// Some(None) if the key is in the memtable but has a tombstone value,
  /// Some(Some(value)) if the key is in the memtable with a non-tombstone value.
  pub(crate) fn get(&self, key: &[u8], max_seq: Option<u64>) -> Option<RowEntry> {
      let user_key = Bytes::from(key.to_vec());
      let range = KVTableInternalKeyRange::from(BytesRange::new(
          Bound::Included(user_key.clone()),
          Bound::Included(user_key),
      ));
      self.map
          .range(range)
          .find(|entry| {
              if let Some(max_seq) = max_seq {
                  entry.key().seq <= max_seq
              } else {
                  true
              }
          })
          .map(|entry| entry.value().clone())
  }

  pub(crate) fn iter(&self) -> MemTableIterator {
      self.range_ascending(..)
  }

  pub(crate) fn range_ascending<T: RangeBounds<Bytes>>(&self, range: T) -> MemTableIterator {
      self.range(range, IterationOrder::Ascending)
  }

  pub(crate) fn range<T: RangeBounds<Bytes>>(
      &self,
      range: T,
      ordering: IterationOrder,
  ) -> MemTableIterator {
      let internal_range = KVTableInternalKeyRange::from(range);
      let mut iterator = MemTableIteratorInnerBuilder {
          map: self.map.clone(),
          inner_builder: |map| map.range(internal_range),
          ordering,
          item: None,
      }
      .build();
      iterator.next_entry_sync();
      iterator
  }

  pub(crate) async fn await_durable(&self) -> Result<(), SlateDBError> {
      self.durable.reader().await_value().await
  }

  pub(crate) fn notify_durable(&self, result: Result<(), SlateDBError>) {
      self.durable.write(result);
  }
}

#[async_trait]
impl KeyValueIterator for MemTableIterator {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.next_entry_sync())
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        loop {
            let front = self.borrow_item().clone();
            if front.is_some_and(|record| record.key < next_key) {
                self.next_entry_sync();
            } else {
                return Ok(());
            }
        }
    }
}
