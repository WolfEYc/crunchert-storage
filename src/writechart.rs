use dashmap::DashMap;
use itertools::Itertools;
use memmap2::MmapMut;
use pco::standalone::simpler_compress;
use std::{collections::HashMap, fs, io, sync::Arc};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, RwLock},
};

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

impl<T> ResizableMmapMut<T> {
    fn align_to(&self, len: usize) -> &[T] {
        unsafe {
            let (_, res, _) = self.mmap[..len].align_to();
            res
        }
    }
    fn align_to_mut(&mut self, len: usize) -> &mut [T] {
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
    #[inline]
    pub fn cap(&self) -> usize {
        self.timestamps_mmap.cap
    }
    #[inline]
    pub fn pts_free(&self) -> usize {
        self.cap() - self.len
    }
    #[inline]
    pub fn pct_full(&self) -> f32 {
        self.len as f32 / self.timestamps_mmap.cap as f32
    }
    pub async fn freeze(
        &self,
        config: &StorageConfig,
    ) -> Result<ReadOnlyTimePartitionFileHeader, io::Error> {
        let mut compressed_pts = Vec::new();
        let mut disk_streams = Vec::new();
        {
            let mut by_stream_id = HashMap::<u64, HotStream>::new();
            for x in self.stream() {
                by_stream_id
                    .entry(*x.stream_id)
                    .and_modify(|hs| {
                        hs.unix_seconds.push(*x.timestamp);
                        hs.values.push(*x.value);
                    })
                    .or_insert_with(|| HotStream {
                        unix_seconds: vec![*x.timestamp],
                        values: vec![*x.value],
                    });
            }

            for (stream_id, hs) in by_stream_id {
                let unix_s_byte_start = compressed_pts.len();
                {
                    let compressed_timestamps =
                        simpler_compress(&hs.unix_seconds, config.compression_level).unwrap();
                    compressed_pts.extend_from_slice(&compressed_timestamps);
                }
                let unix_s_byte_stop = compressed_pts.len();
                {
                    let compressed_values =
                        simpler_compress(&hs.values, config.compression_level).unwrap();
                    compressed_pts.extend_from_slice(&compressed_values);
                }
                let values_byte_stop = compressed_pts.len();
                disk_streams.push(ReadOnlyDiskStreamFileHeader {
                    stream_id,
                    unix_s_byte_start,
                    unix_s_byte_stop,
                    values_byte_stop,
                })
            }
        }

        let file_path = config
            .data_storage_dir
            .join(self.start_unix_s.to_string())
            .join("_readonly");
        {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file_path)
                .await?;

            file.write_all(&compressed_pts).await?;
            file.flush().await?;
        }
        let file_header = ReadOnlyTimePartitionFileHeader {
            start_unix_s: self.start_unix_s,
            file_path,
            disk_streams,
        };

        Ok(file_header)
    }
}

pub async fn write_stream(
    partition: Arc<RwLock<WritableTimePartition>>,
    mut stream: StreamPointVec,
) {
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
        if stream.len() > writable_partition.pts_free() {
            return;
        }
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
