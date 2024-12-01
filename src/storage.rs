use crate::constants::*;
use crate::errors::*;
use crate::models::*;
use crate::readchart::writable_time_partition_get_agg_chart;
use crate::writechart::write_stream;
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

impl<T> ResizableMmapMut<T> {
    fn new(path: &Path, cap: usize) -> Result<Self, io::Error> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let cap_in_bytes = cap * size_of::<T>();
        file.set_len(cap_in_bytes as u64)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        mmap.advise(memmap2::Advice::Sequential)?;

        Ok(Self {
            mmap,
            file,
            cap,
            item: std::marker::PhantomData::<T>,
        })
    }
}

impl ReadOnlyTimePartitionFileHeader {
    fn materialize(self: &Self) -> Result<ReadOnlyTimePartition, io::Error> {
        let hash_map_iter = self.disk_streams.iter().cloned().map(|x| {
            (
                x.stream_id,
                ReadOnlyStream {
                    disk_header: x,
                    hot_stream: RwLock::new(None),
                    last_accessed: Mutex::new(None),
                },
            )
        });

        let file = fs::File::open(&self.file_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let streams = DashMap::from_iter(hash_map_iter);
        let start_unix_s = self.start_unix_s;
        Ok(ReadOnlyTimePartition {
            start_unix_s,
            streams,
            mmap,
        })
    }
}
impl WritableTimePartitionFileHeader {
    fn new(config: &StorageConfig) -> Self {
        let start_unix_s = Utc::now().timestamp();
        let file_path = config.data_storage_dir.join(start_unix_s.to_string());
        let timestamps_file_path = file_path.join("timestamps");
        let stream_ids_file_path = file_path.join("stream_ids");
        let values_file_path = file_path.join("values");
        let cap = config.writable_partition_size;

        Self {
            len: 0,
            cap,
            start_unix_s,
            timestamps_file_path,
            stream_ids_file_path,
            values_file_path,
        }
    }
    fn materialize(
        self: &WritableTimePartitionFileHeader,
    ) -> Result<WritableTimePartition, io::Error> {
        let timestamps_mmap = ResizableMmapMut::new(self.timestamps_file_path.as_path(), self.cap)?;

        let streams_mmap = ResizableMmapMut::new(self.stream_ids_file_path.as_path(), self.cap)?;
        let values_mmap = ResizableMmapMut::new(self.values_file_path.as_path(), self.cap)?;

        let start_unix_s = self.start_unix_s;
        let streams = DashMap::new();

        Ok(WritableTimePartition {
            start_unix_s,
            timestamps_mmap,
            streams_mmap,
            values_mmap,
            streams,
            len: self.len,
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
            .map(ReadOnlyTimePartitionFileHeader::materialize)
            .process_results(|iter| iter.map(Arc::new).collect())?;

        let writable_partitions = self
            .writable_time_partitions
            .iter()
            .map(WritableTimePartitionFileHeader::materialize)
            .process_results(|iter| iter.map(RwLock::new).map(Arc::new).collect())?;

        Ok((readonly_partitions, writable_partitions))
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
        let partitions_file_header = RwLock::new(partitions_file_header);

        Ok(Self {
            config,
            partitions_file_header,
            readonly_partitions,
            writable_partitions,
            num_threads,
        })
    }

    async fn write_streams(&self, mut stream: ImportStream) {
        assert!(!stream.pts.is_empty());

        stream.pts.as_mut_slice().sort_by_key(|x| *x.timestamp);
        let writable_partitions = self.writable_partitions.read().await;

        assert!(writable_partitions.len() != 0);

        let mut import_joinset = JoinSet::new();
        for partition in writable_partitions.iter().rev() {
            let start_unix_s = partition.read().await.start_unix_s;

            let start_idx = stream
                .pts
                .timestamp
                .binary_search(&start_unix_s)
                .unwrap_or_else(|x| x);

            if start_idx == stream.pts.len() {
                break;
            }

            let stream_pts = stream.pts.split_off(start_idx);
            import_joinset.spawn(write_stream(partition.clone(), stream_pts));
            if stream.pts.is_empty() {
                break;
            }
        }
        import_joinset.join_all().await;
    }
    async fn start_next_stream(&self) -> Result<(), io::Error> {
        let new_writable_partition_file_header = WritableTimePartitionFileHeader::new(&self.config);
        let writable_partition = new_writable_partition_file_header.materialize()?;
        {
            let mut writable_partitions_file_header = self.partitions_file_header.write().await;
            writable_partitions_file_header
                .writable_time_partitions
                .push(new_writable_partition_file_header);
        }
        {
            let mut writable_writable_partitions = self.writable_partitions.write().await;
            writable_writable_partitions.push(Arc::new(RwLock::new(writable_partition)));
        }
        Ok(())
    }
    async fn need_new_writable_partition(&self) -> bool {
        let readable_writable_partitions = self.writable_partitions.read().await;
        let last_readable_writable_partition =
            readable_writable_partitions.last().unwrap().read().await;
        last_readable_writable_partition.pct_full() > self.config.writable_partition_ideal_pct_full
    }

    async fn need_to_move_earliest_writable_to_readonly_partition(&self) -> bool {
        let readable_writable_partitions = self.writable_partitions.read().await;
        if readable_writable_partitions.len() <= self.config.min_writable_partitions {
            return false;
        }
        let first_readable_writable_partition =
            readable_writable_partitions.first().unwrap().read().await;

        let now = Utc::now().timestamp();
        let age = (now - first_readable_writable_partition.start_unix_s).unsigned_abs();

        age > self.config.writable_duration_s
    }

    pub async fn import_streams(&self, stream: ImportStream) -> Result<(), io::Error> {
        self.write_streams(stream).await;

        if self.need_new_writable_partition().await {
            self.start_next_stream().await?;
        }

        if self
            .need_to_move_earliest_writable_to_readonly_partition()
            .await
        {

            //TODO move earliest writable partition to readable partition
        }

        Ok(())
    }
}
