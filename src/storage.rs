use crate::constants::*;
use crate::errors::*;
use crate::models::*;
use crate::readchart::writable_time_partition_get_agg_chart;
use chrono::Utc;
use dashmap::DashMap;
use itertools::Itertools;
use memmap2::{Mmap, MmapMut};
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;

impl ResizableMmapMut {
    fn new(path: &Path) -> Result<Self, io::Error> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        mmap.advise(memmap2::Advice::Sequential)?;

        Ok(Self { mmap, file })
    }
}

impl TryFrom<&ReadOnlyTimePartitionFileHeader> for ReadOnlyTimePartition {
    type Error = io::Error;
    fn try_from(value: &ReadOnlyTimePartitionFileHeader) -> Result<Self, Self::Error> {
        let hash_map_iter = value.disk_streams.iter().cloned().map(|x| {
            (
                x.stream_id,
                ReadOnlyStream {
                    disk_header: x,
                    hot_stream: RwLock::new(None),
                    last_accessed: Mutex::new(None),
                },
            )
        });

        let file = fs::File::open(&value.file_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let streams = DashMap::from_iter(hash_map_iter);
        let start_unix_s = value.start_unix_s;
        Ok(Self {
            start_unix_s,
            streams,
            mmap,
        })
    }
}

impl TryFrom<&WritableTimePartitionFileHeader> for WritableTimePartition {
    type Error = io::Error;
    fn try_from(value: &WritableTimePartitionFileHeader) -> Result<Self, Self::Error> {
        let timestamps_mmap = ResizableMmapMut::new(value.timestamps_file_path.as_path())?;

        let streams_mmap = ResizableMmapMut::new(value.stream_ids_file_path.as_path())?;
        let values_mmap = ResizableMmapMut::new(value.values_file_path.as_path())?;

        let start_unix_s = value.start_unix_s;
        let streams = DashMap::new();

        Ok(Self {
            start_unix_s,
            timestamps_mmap,
            streams_mmap,
            values_mmap,
            streams,
            len: value.len,
        })
    }
}
impl PartitionsFileHeader {
    fn new(config: &StorageConfig) -> Self {
        Self {
            read_only_time_partitions: Vec::new(),
            writable_time_partitions: vec![WritableTimePartitionFileHeader::new(config)],
        }
    }

    fn from_file(path: &Path) -> Result<Self, StorageCreationError> {
        let bytes = std::fs::read(path)?;
        Ok(postcard::from_bytes(&bytes)?)
    }

    fn thaw(
        &self,
    ) -> Result<
        (
            Vec<Arc<ReadOnlyTimePartition>>,
            Vec<Arc<RwLock<WritableTimePartition>>>,
        ),
        io::Error,
    > {
        let readonly_partitions = self
            .read_only_time_partitions
            .iter()
            .map(ReadOnlyTimePartition::try_from)
            .process_results(|iter| iter.map(Arc::new).collect())?;

        let writable_partitions = self
            .writable_time_partitions
            .iter()
            .map(WritableTimePartition::try_from)
            .process_results(|iter| iter.map(RwLock::new).map(Arc::new).collect())?;

        Ok((readonly_partitions, writable_partitions))
    }
}

impl WritableTimePartitionFileHeader {
    fn new(config: &StorageConfig) -> Self {
        let start_unix_s = Utc::now().timestamp();
        let file_path = config.data_storage_dir.join(start_unix_s.to_string());
        let timestamps_file_path = file_path.join("timestamps");
        let stream_ids_file_path = file_path.join("stream_ids");
        let values_file_path = file_path.join("values");
        Self {
            len: 0,
            start_unix_s,
            timestamps_file_path,
            stream_ids_file_path,
            values_file_path,
        }
    }
}
impl Storage {
    async fn get_read_only_partitions_in_range(
        &self,
        start_unix_s: i64,
        stop_unix_s: i64,
    ) -> Vec<Arc<ReadOnlyTimePartition>> {
        let mut partition_end = Utc::now().timestamp();
        let mut partitions_in_range = Vec::new();
        let partitions = self.readonly_partitions.read().await;
        for partition in partitions.iter() {
            if start_unix_s > partition_end {
                return partitions_in_range;
            }
            partition_end = partition.start_unix_s;
            if stop_unix_s < partition.start_unix_s {
                continue;
            }
            partitions_in_range.push(partition.clone());
        }
        return partitions_in_range;
    }

    async fn get_writable_partitions_in_range(
        &self,
        start_unix_s: i64,
        stop_unix_s: i64,
    ) -> Vec<Arc<RwLock<WritableTimePartition>>> {
        let mut partition_end = Utc::now().timestamp();
        let mut partitions_in_range = Vec::new();
        let partitions = self.writable_partitions.read().await;

        for partition in partitions.iter() {
            let readable_partition = partition.read().await;
            if start_unix_s > partition_end {
                return partitions_in_range;
            }
            partition_end = readable_partition.start_unix_s;
            if stop_unix_s < readable_partition.start_unix_s {
                continue;
            }
            partitions_in_range.push(partition.clone());
        }
        return partitions_in_range;
    }

    pub async fn get_agg_chart(&self, req: ChartRequest, agg: Aggregation) -> Vec<Datapoint> {
        let read_only_time_partitions = self
            .get_read_only_partitions_in_range(req.start_unix_s, req.stop_unix_s)
            .await;
        let writable_time_partitions = self
            .get_writable_partitions_in_range(req.start_unix_s, req.stop_unix_s)
            .await;
        let arc_req = Arc::new(req);
        let read_only_datapoint_jobs = read_only_time_partitions.into_iter().map(|x| {
            x.read_only_time_partition_get_agg_chart(arc_req.clone(), agg, self.num_threads)
        });
        let writable_datapoint_jobs = writable_time_partitions.into_iter().map(|x| {
            writable_time_partition_get_agg_chart(x, arc_req.clone(), agg, self.num_threads)
        });

        let read_only_datapoints_nested = JoinSet::from_iter(read_only_datapoint_jobs).join_all();
        let writeable_datapoints_nested = JoinSet::from_iter(writable_datapoint_jobs).join_all();
        let all_datapoints_nested =
            JoinSet::from_iter([writeable_datapoints_nested, read_only_datapoints_nested])
                .join_all()
                .await;

        let datapoints_flattened = all_datapoints_nested
            .into_iter()
            .flatten()
            .flatten()
            .collect();
        return datapoints_flattened;
    }

    pub fn new(config: StorageConfig, num_threads: usize) -> Result<Self, StorageCreationError> {
        let config = config.validate()?;
        let partitions_file_path = config
            .data_storage_dir
            .join(PARTITIONS_FILE_HEADER_FILENAME);

        let partitions_file_header = if partitions_file_path.exists() {
            PartitionsFileHeader::from_file(&partitions_file_path)?
        } else {
            PartitionsFileHeader::new(&config)
        };

        let (readonly_partitions, writeable_partitions) = partitions_file_header.thaw()?;
        let readonly_partitions = RwLock::new(readonly_partitions);
        let writable_partitions = RwLock::new(writeable_partitions);

        Ok(Self {
            config,
            partitions_file_header,
            readonly_partitions,
            writable_partitions,
            num_threads,
        })
    }

    pub async fn import_stream(&self, mut stream: ImportStream) -> Result<(), ImportStreamError> {
        stream.pts.sort_unstable();
        let first_ts = stream.pts.first().unwrap().timestamp;
        let last_ts = stream.pts.last().unwrap().timestamp;

        let writeable_partitions = self
            .get_writable_partitions_in_range(first_ts, last_ts)
            .await;

        if writeable_partitions.len() == 0 {
            todo!();
        }

        //TODO split the points better and dont cause a off by 1 error

        writeable_partitions
            .first()
            .unwrap()
            .write()
            .await
            .import_stream(stream.pts)
            .await;

        if writeable_partitions.len() == 1 {
            return Ok(());
        }

        for i in 1..writeable_partitions.len() {
            let prev = writeable_partitions[i - 1].read().await;
            let curr = writeable_partitions[i].read().await;

            let start_idx = stream
                .pts
                .binary_search_by_key(&prev.start_unix_s, |x| x.timestamp);
            let end_idx = stream
                .pts
                .binary_search_by_key(&curr.start_unix_s, |x| x.timestamp);
        }

        todo!()
    }
}
