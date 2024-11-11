use itertools::{izip, Itertools};
use std::ops::Bound;
use std::ops::RangeBounds;

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
    fn timestamps(&self) -> &[i64] {
        self.timestamps_mmap.align_to(self.len)
    }
    fn timestamps_range(&self, range: impl RangeBounds<usize>) -> &[i64] {
        self.timestamps_mmap.align_to_range(range, self.len)
    }
    fn stream_ids(&self) -> &[u64] {
        self.streams_mmap.align_to(self.len)
    }

    fn stream_ids_range(&self, range: impl RangeBounds<usize>) -> &[u64] {
        self.streams_mmap.align_to_range(range, self.len)
    }
    fn values_range(&self, range: impl RangeBounds<usize>) -> &[f32] {
        self.values_mmap.align_to_range(range, self.len)
    }
    fn values(&self) -> &[f32] {
        self.values_mmap.align_to(self.len)
    }
    fn timestamps_mut(&mut self) -> &mut [i64] {
        self.timestamps_mmap.align_to_mut(self.len)
    }
    fn stream_ids_mut(&mut self) -> &mut [u64] {
        self.streams_mmap.align_to_mut(self.len)
    }
    fn values_mut(&mut self) -> &mut [f32] {
        self.values_mmap.align_to_mut(self.len)
    }

    pub async fn import_stream(&mut self, mut stream: ImportStream) {
        assert!(stream.pts.len() > 0);

        stream.pts.sort_unstable();

        let start_ts = stream.pts.first().unwrap().timestamp;
        let end_ts = stream.pts.last().unwrap().timestamp;

        let first_idx: usize;
        let last_idx: usize;

        {
            let wal_timestamps = self.timestamps();
            first_idx = wal_timestamps
                .iter()
                .cloned()
                .rposition(|x| x <= start_ts)
                .unwrap_or(0);
            last_idx = wal_timestamps
                .iter()
                .cloned()
                .rposition(|x| x < end_ts)
                .unwrap_or(0);
        }
        assert!(first_idx <= last_idx);

        let r_offset_count = self.len - last_idx - 1;
        let prev_len = self.len;
        self.len += stream.pts.len();

        if r_offset_count > 0 {
            // need to memmove the pts to the right of the insertion block
            let dest = self.len - r_offset_count;
            let src = last_idx..prev_len;
            {
                let timestamps = self.timestamps_mut();
                timestamps.copy_within(src.clone(), dest);
            }
            {
                let stream_ids = self.stream_ids_mut();
                stream_ids.copy_within(src.clone(), dest);
            }
            {
                let values = self.values_mut();
                values.copy_within(src, dest);
            }
        }

        if first_idx < last_idx {
            let range = first_idx..=last_idx;
            let timestamps = self.timestamps_range(range.clone());
            let stream_ids = self.stream_ids_range(range.clone());
            let values = self.values_range(range);
            let merge_pts = izip!(timestamps, stream_ids, values);
            let as_stream_points = merge_pts.map(|(&timestamp, &stream_id, &value)| StreamPoint {
                timestamp,
                stream_id,
                value,
            });
            stream.pts = stream.pts.into_iter().merge(as_stream_points).collect();
        }

        let src = first_idx..first_idx + stream.pts.len();

        {
            let timestamps_from_stream: Vec<i64> = stream.pts.iter().map(|x| x.timestamp).collect();
            let timestamps = self.timestamps_mut();
            timestamps[src.clone()].copy_from_slice(&timestamps_from_stream)
        }
        {
            let stream_ids_from_stream: Vec<u64> = stream.pts.iter().map(|x| x.stream_id).collect();
            let stream_ids = self.stream_ids_mut();
            stream_ids[src.clone()].copy_from_slice(&stream_ids_from_stream)
        }
        {
            let values_from_stream: Vec<f32> = stream.pts.iter().map(|x| x.value).collect();
            let values = self.values_mut();
            values[src].copy_from_slice(&values_from_stream)
        }
    }
}