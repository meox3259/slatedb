use std::cell::Cell;
use std::intrinsics::mir::PtrMetadata;
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
use std::collections::{LinkedList, VecDeque};
use std::sync::Mutex;

use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, KeyValueIterator};
use crate::types::RowEntry;
use crate::utils::WatchableOnceCell;

use crate::mem_table::KVTableInternalKey;
use crate::mem_table::KVTableInternalKeyRange;
use crate::mem_table::MemTableIterator;
use crate::merge_iterator::MergeIterator;

pub(crate) struct KVArrayInternalKey {
    user_key: Bytes,
    seq: u64,
}

impl KVArrayInternalKey {
    pub fn new(user_key: Bytes, seq: u64) -> Self {
        Self { user_key, seq }
    }
}

impl Ord for KVArrayInternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then(self.seq.cmp(&other.seq).reverse())
    }
}

impl PartialOrd for KVArrayInternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

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
    inner: Range<'this, KVArrayInternalKey, T, KVArrayInternalKey, RowEntry>,
    ordering: IterationOrder,
    item: Option<RowEntry>,
}
pub(crate) type SegmentIterator = SegmentIteratorInner<KVArrayInternalKeyRange>;

pub(crate) struct KVArrayInternalKeyRange {
    start_bound: Bound<KVArrayInternalKey>,
    end_bound: Bound<KVArrayInternalKey>,
}

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

pub(crate) struct KVArrayMetadata {
    pub(crate) entry_num: usize,
    pub(crate) entries_size_in_bytes: usize,
    /// this corresponds to the timestamp of the most recent
    /// modifying operation on this KVTable (insertion or deletion)
    #[allow(dead_code)]
    pub(crate) last_tick: i64,
    /// the sequence number of the most recent operation on this KVTable
    #[allow(dead_code)]
    pub(crate) last_seq: u64,
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

    pub(crate) fn metadata(&self) -> KVArrayMetadata {
        let entry_num = self.map.len();
        let entries_size_in_bytes = self.entries_size_in_bytes.load(Ordering::Relaxed);
        let last_tick = self.last_tick.load(SeqCst);
        let last_seq = self.last_seq.load(SeqCst);
        KVArrayMetadata {
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
          .binary_search_by(|entry| entry.key().cmp(&range))
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
      let mut iterator = SegmentIteratorInnerBuilder {
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
struct ImmutableSegment {
    last_wal_id: u64,
    table: Arc<KVArray>,
    flushed: WatchableOnceCell<Result<(), SlateDBError>>,
}

impl ImmutableSegment {
    pub(crate) async fn new(iter: MergeIterator) -> Self {
        let table = Arc::new(KVArray::new());
        while let Some(entry) = iter.next_entry().await? {
            table.push(entry.key, entry.value);
        }
        Self { 
            last_wal_id: 0,
            table,
            flushed: WatchableOnceCell::new(),
        }
    }

    pub(crate) fn new(table: WritableKVTable, last_wal_id: u64) -> Self {
        Self {
            last_wal_id,
            table: table.table,
            flushed: WatchableOnceCell::new(),
        }
    }

    pub(crate) fn table(&self) -> Arc<KVArray> {
        self.table.clone()
    }

    pub(crate) fn last_wal_id(&self) -> u64 {
        self.last_wal_id
    }

    pub(crate) async fn await_flush_to_l0(&self) -> Result<(), SlateDBError> {
        self.flushed.reader().await_value().await
    }

    pub(crate) fn notify_flush_to_l0(&self, result: Result<(), SlateDBError>) {
        self.flushed.write(result);
    }
}

struct PipeLine {
    pipeline: Arc<Mutex<VecDeque<ImmutableSegment>>>,
    read_only: Arc<Mutex<VecDeque<ImmutableSegment>>>,
    version: AtomicUsize,
}

enum CompactionAction {
    Merge,
    Compact,
}

impl PipeLine {
    fn new() -> Self {
        Self { 
            pipeline: Arc::new(Mutex::new(VecDeque::new())), 
            read_only: Arc::new(Mutex::new(VecDeque::new())),
            version: AtomicUsize::new(0),
        }
    }

    fn compact(&mut self) {
        let head = self.pipeline.pop_front().unwrap();
        let tail = self.pipeline.pop_back().unwrap();
        let compacted = head.compact(tail);
        self.pushHead(compacted);
    }

    fn doCompaction(&mut self, action: CompactionAction) -> Result<(), SlateDBError> {
        match action {
            CompactionAction::Compact => {
                let mut result = self.createSubsitution();
                self.swapSegments(result)
            }

            _ => { // 使用 '_' 匹配所有其他情况 (类似 C++ 的 default)
                println!("The number is something else");
            }
        }
    }

    fn createSubsitution(&mut self) -> Result<(), SlateDBError> {
        let head = self.pipeline.pop_front().unwrap();
        let tail = self.pipeline.pop_back().unwrap();
        let compacted = head.compact(tail);

        self.pushHead(compacted);
        Ok(())
    }

    fn removeSuffixSegments(&mut self, segments: LinkedList<ImmutableSegment>) -> Result<(), SlateDBError> {
        if segments.size() > self.pipeline.size() {
            return Err(SlateDBError::new("segments.size() > self.pipeline.size()"));
        }
        let mut pipeline = self.pipeline.lock();
        while suffix.size() > 0 {
            let suffix_segment = suffix.pop_front().unwrap();
            let pipeline_segment = pipeline.pop_front().unwrap();
            if suffix_segment != pipeline_segment {
                return Err(SlateDBError::new("segments.size() > self.pipeline.size()"));
            }
        }
        return Ok(());
    }

    fn pushHead(&mut self, segment: MutableSegment) {
        self.pipeline.push_front(segment);
        // 这里需要锁住
        self.read_only = self.pipeline.lock().clone();
    }

    fn pushTail(&mut self, segment: ImmutableSegment) {
        self.pipeline.push_back(segment);
        // 这里需要锁住
        self.read_only = self.pipeline.lock().clone();
    }
}

impl PipeLine {
    fn flattenOneSegment(&mut self) -> Result<(), SlateDBError> {
        let mut pipeline = self.pipeline.lock();
        let mut index = 0;  
        for segment in pipeline.iter_mut() {
            index += 1;
            if segment.canBeFlattened() {
                if segment.empty() {
                    continue;
                }
            }
            let new_segment = ImmutableSegment::new(segment.table(), segment.last_wal_id());
            pipeline[index] = new_segment;
            self.version.fetch_add(1, Ordering::Release);
            break;
        }
        Ok(())
    }
}