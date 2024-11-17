use itertools::Itertools;
use std::ops::Bound;
use std::ops::RangeBounds;
use tokio::sync::RwLock;

use crate::models::*;

impl PartialEq for StreamPoint {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.stream_id == other.stream_id
    }
}

impl Eq for StreamPoint {}
impl Ord for StreamPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then(self.stream_id.cmp(&other.stream_id))
    }
}

impl PartialOrd for StreamPoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl ResizableMmapMut {
    fn align_to<T>(&self, len: usize) -> &[T] {
        unsafe {
            let (_, res, _) = self.mmap[..len].align_to();
            res
        }
    }
    fn align_to_range<T>(&self, range: impl RangeBounds<usize>, len: usize) -> &[T] {
        let remapped_start = match range.start_bound() {
            Bound::Included(x) => (x + 1) * size_of::<T>(),
            Bound::Excluded(x) => x * size_of::<T>(),
            Bound::Unbounded => 0,
        };
        let remapped_end = match range.end_bound() {
            Bound::Included(x) => (x + 1) * size_of::<T>(),
            Bound::Excluded(x) => x * size_of::<T>(),
            Bound::Unbounded => len,
        };
        let remapped_range = remapped_start..remapped_end;
        unsafe {
            let (_, res, _) = self.mmap[remapped_range].align_to();
            res
        }
    }

    fn align_to_mut<T>(&mut self, len: usize) -> &mut [T] {
        unsafe {
            let (_, res, _) = self.mmap[..len].align_to_mut();
            res
        }
    }
}

impl WritableTimePartition {
    fn stream(&self) -> StreamPointSlice {
        StreamPointSlice {
            timestamp: self.timestamps_mmap.align_to(self.len),
            stream_id: self.streams_mmap.align_to(self.len),
            value: self.values_mmap.align_to(self.len),
        }
    }
    fn stream_mut(&mut self) -> StreamPointSliceMut {
        StreamPointSliceMut {
            timestamp: self.timestamps_mmap.align_to_mut(self.len),
            stream_id: self.streams_mmap.align_to_mut(self.len),
            value: self.values_mmap.align_to_mut(self.len),
        }
    }
    fn stream_range<R: RangeBounds<usize> + Clone>(&self, range: R) -> StreamPointSlice {
        StreamPointSlice {
            timestamp: self.timestamps_mmap.align_to_range(range.clone(), self.len),
            stream_id: self.streams_mmap.align_to_range(range.clone(), self.len),
            value: self.values_mmap.align_to_range(range, self.len),
        }
    }
}
pub async fn import_stream(partition: RwLock<WritableTimePartition>, mut stream: StreamPointVec) {
    let last_idx: usize;
    let first_idx: usize;
    {
        let start_ts = *stream.timestamp.first().unwrap();
        let end_ts = *stream.timestamp.last().unwrap();

        let readable_partition = partition.read().await;
        let readable_stream = readable_partition.stream();

        first_idx = readable_stream
            .timestamp
            .iter()
            .cloned()
            .rposition(|x| x <= start_ts)
            .unwrap_or(0);
        last_idx = readable_stream
            .timestamp
            .iter()
            .cloned()
            .rposition(|x| x < end_ts)
            .unwrap_or(0);
        assert!(first_idx <= last_idx);

        if first_idx < last_idx {
            let range = first_idx..=last_idx;
            let readable_stream_merge_pts = readable_stream.index(range);
            let owned_merge_pts = readable_stream_merge_pts.iter().map(|x| x.to_owned());
            stream = stream
                .into_iter()
                .map(|x| x.to_owned())
                .merge(owned_merge_pts)
                .collect();
        }
    }

    {
        let mut writable_partition = partition.write().await;
        let r_offset_count = writable_partition.len - last_idx - 1;
        let prev_len = writable_partition.len;
        writable_partition.len += stream.len();

        let dest = writable_partition.len - r_offset_count;
        let writable_streams = writable_partition.stream_mut();

        if r_offset_count > 0 {
            // need to memmove the pts to the right of the insertion block
            let src = last_idx..prev_len;
            writable_streams.timestamp.copy_within(src.clone(), dest);
            writable_streams.stream_id.copy_within(src.clone(), dest);
            writable_streams.value.copy_within(src, dest);
        }

        let src = first_idx..first_idx + stream.len();
        writable_streams.timestamp[src.clone()].copy_from_slice(&stream.timestamp);
        writable_streams.stream_id[src.clone()].copy_from_slice(&stream.stream_id);
        writable_streams.value[src].copy_from_slice(&stream.value);
    }
}
