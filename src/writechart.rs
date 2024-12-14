use itertools::Itertools;
use pco::standalone::simpler_compress;
use std::{collections::HashMap, io};
use tokio::io::AsyncWriteExt;

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
    fn align_to_mut(&mut self) -> &mut [T] {
        unsafe {
            let (_, res, _) = self.mmap.align_to_mut();
            res
        }
    }
}

impl WritableTimePartition {
    pub async fn freeze(
        &self,
        config: &StorageConfig,
    ) -> Result<ReadOnlyTimePartitionFileHeader, io::Error> {
        let mut compressed_pts = Vec::new();
        let mut disk_streams = Vec::new();

        let by_stream_id = {
            let mut by_stream_id: HashMap<u64, DatapointVec> = HashMap::new();
            for x in self.stream() {
                by_stream_id
                    .entry(*x.stream_id)
                    .and_modify(|e| {
                        e.push(Datapoint {
                            unix_s: *x.timestamp,
                            value: *x.value,
                        })
                    })
                    .or_insert_with(|| DatapointVec {
                        unix_s: vec![*x.timestamp],
                        value: vec![*x.value],
                    });
            }
            by_stream_id
        };

        for (stream_id, pts) in by_stream_id {
            let unix_s_byte_start = compressed_pts.len();
            {
                let compressed_timestamps =
                    simpler_compress(&pts.unix_s, config.compression_level).unwrap();
                compressed_pts.extend_from_slice(&compressed_timestamps);
            }
            let unix_s_byte_stop = compressed_pts.len();
            {
                let compressed_values =
                    simpler_compress(&pts.value, config.compression_level).unwrap();
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

        let file_path = config
            .data_storage_dir
            .join("readonly")
            .join(self.start_unix_s.to_string());
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

    pub async fn write_stream(&mut self, mut stream: StreamPointVec) {
        let first_pt = *stream.timestamp.first().unwrap();
        let partition_stream = self.stream();
        let pp = partition_stream
            .timestamp
            .partition_point(|&x| x < first_pt);

        let partition_stream = partition_stream.index(pp..);

        stream = stream
            .into_iter()
            .merge_join_by(partition_stream, |i, j| {
                i.timestamp
                    .cmp(&j.timestamp)
                    .then_with(|| i.stream_id.cmp(&j.stream_id))
            })
            .map(|x| match x {
                itertools::EitherOrBoth::Both(_, r) => r,
                itertools::EitherOrBoth::Left(l) => l,
                itertools::EitherOrBoth::Right(r) => r,
            })
            .map(|x| x.to_owned())
            .collect();

        let new_len = self.len + stream.len();
        let src = pp..new_len;
        let timestamps = self.timestamps_mmap.align_to_mut();
        timestamps[src.clone()].copy_from_slice(&stream.timestamp);
        let stream_ids = self.streams_mmap.align_to_mut();
        stream_ids[src.clone()].copy_from_slice(&stream.stream_id);
        let values = self.values_mmap.align_to_mut();
        values[src.clone()].copy_from_slice(&stream.value);
        self.len = new_len;
    }
}
