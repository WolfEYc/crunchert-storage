use crate::errors::*;
use crate::models::*;
use crate::readchart::get_writable_chart_aggregated;
use crate::writechart::write_stream;
use chrono::Utc;
use itertools::Itertools;
use memmap2::{Mmap, MmapMut};
use std::collections::VecDeque;
use std::io;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;

impl<T> ResizableMmapMut<T> {
    async fn new(path: &Path, cap: usize) -> Result<Self, io::Error> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;
        let cap_in_bytes = cap * size_of::<T>();
        file.set_len(cap_in_bytes as u64).await?;
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
    async fn materialize(self) -> Result<ReadOnlyTimePartition, io::Error> {
        let streams = self
            .disk_streams
            .into_iter()
            .map(|x| (x.stream_id, x))
            .collect();

        let file = tokio::fs::File::open(&self.file_path).await?;
        let mmap = unsafe { Mmap::map(&file)? };
        let start_unix_s = self.start_unix_s;
        Ok(ReadOnlyTimePartition {
            file,
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
        let cap = config.writable_partition_bytes / size_of::<StreamPoint>();

        Self {
            len: 0,
            cap,
            start_unix_s,
            timestamps_file_path,
            stream_ids_file_path,
            values_file_path,
        }
    }
    async fn materialize(self) -> Result<WritableTimePartition, io::Error> {
        let timestamps_mmap_job =
            ResizableMmapMut::new(self.timestamps_file_path.as_path(), self.cap);

        let streams_mmap_job = ResizableMmapMut::new(self.stream_ids_file_path.as_path(), self.cap);
        let values_mmap_job = ResizableMmapMut::new(self.values_file_path.as_path(), self.cap);

        let (timestamps_mmap, streams_mmap, values_mmap) =
            tokio::join!(timestamps_mmap_job, streams_mmap_job, values_mmap_job);

        let start_unix_s = self.start_unix_s;

        Ok(WritableTimePartition {
            start_unix_s,
            timestamps_mmap: timestamps_mmap?,
            streams_mmap: streams_mmap?,
            values_mmap: values_mmap?,
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

    async fn from_file(path: &Path) -> Result<Self, io::Error> {
        let bytes = tokio::fs::read(path).await?;
        Ok(postcard::from_bytes(&bytes).unwrap())
    }
    async fn to_file(&self, path: &Path) -> Result<(), io::Error> {
        let bytes = postcard::to_allocvec(self).unwrap();
        tokio::fs::write(path, bytes).await?;
        Ok(())
    }

    async fn thaw(
        &self,
    ) -> Result<
        (
            Vec<Arc<ReadOnlyTimePartition>>,
            VecDeque<Arc<RwLock<WritableTimePartition>>>,
        ),
        io::Error,
    > {
        let readonly_partitions = {
            let readonly_partition_jobs = self
                .read_only_time_partitions
                .iter()
                .cloned()
                .map(ReadOnlyTimePartitionFileHeader::materialize);
            JoinSet::from_iter(readonly_partition_jobs)
                .join_all()
                .await
                .into_iter()
                .process_results(|iter| iter.map(Arc::new).collect())?
        };
        let writable_partitions = {
            let writable_partition_jobs = self
                .writable_time_partitions
                .iter()
                .cloned()
                .map(WritableTimePartitionFileHeader::materialize);
            JoinSet::from_iter(writable_partition_jobs)
                .join_all()
                .await
                .into_iter()
                .process_results(|iter| iter.map(RwLock::new).map(Arc::new).collect())?
        };

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

    pub async fn get_agg_chart(&self, req: ChartRequest, agg: Aggregation) -> DatapointVec {
        let read_only_time_partitions = self
            .get_read_only_partitions_in_range(req.start_unix_s, req.stop_unix_s)
            .await;
        let writable_time_partitions = self
            .get_writable_partitions_in_range(req.start_unix_s, req.stop_unix_s)
            .await;
        let read_only_datapoint_jobs = read_only_time_partitions
            .into_iter()
            .map(|x| x.get_chart_aggregated(req.clone(), agg));
        let writable_datapoint_jobs = writable_time_partitions
            .into_iter()
            .map(|x| get_writable_chart_aggregated(x, req.clone(), agg));

        let read_only_datapoints_nested = JoinSet::from_iter(read_only_datapoint_jobs).join_all();
        let writeable_datapoints_nested = JoinSet::from_iter(writable_datapoint_jobs).join_all();
        let all_datapoints_nested =
            JoinSet::from_iter([writeable_datapoints_nested, read_only_datapoints_nested])
                .join_all()
                .await;

        all_datapoints_nested
            .into_iter()
            .flatten()
            .into_iter()
            .reduce(|mut acc, e| {
                acc.unix_s.extend_from_slice(&e.unix_s);
                acc.value.extend_from_slice(&e.value);
                acc
            })
            .unwrap_or(DatapointVec::default())
    }

    pub async fn new(
        config: StorageConfig,
        num_threads: usize,
    ) -> Result<Self, StorageCreationError> {
        let config = config.validate()?;
        let partitions_file_path = config.partitions_file_path();
        let partitions_file_header = if partitions_file_path.exists() {
            PartitionsFileHeader::from_file(&partitions_file_path).await?
        } else {
            PartitionsFileHeader::new(&config)
        };

        let (readonly_partitions, writeable_partitions) = partitions_file_header.thaw().await?;
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

    async fn write_stream(&self, mut stream: ImportStream) {
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
        let writable_partition = new_writable_partition_file_header
            .clone()
            .materialize()
            .await?;
        {
            let mut writable_partitions_file_header = self.partitions_file_header.write().await;
            writable_partitions_file_header
                .writable_time_partitions
                .push(new_writable_partition_file_header);
        }
        {
            let mut writable_writable_partitions = self.writable_partitions.write().await;
            writable_writable_partitions.push_back(Arc::new(RwLock::new(writable_partition)));
        }
        Ok(())
    }
    async fn need_new_writable_partition(&self) -> bool {
        let readable_writable_partitions = self.writable_partitions.read().await;
        let last_readable_writable_partition =
            readable_writable_partitions.back().unwrap().read().await;
        last_readable_writable_partition.pct_full() > self.config.writable_partition_ideal_pct_full
    }

    async fn writable_freeze_required(&self) -> bool {
        let readable_writable_partitions = self.writable_partitions.read().await;
        if readable_writable_partitions.len() <= self.config.min_writable_partitions {
            return false;
        }
        let first_readable_writable_partition =
            readable_writable_partitions.front().unwrap().read().await;

        let now = Utc::now().timestamp();
        let age = (now - first_readable_writable_partition.start_unix_s).unsigned_abs();

        age > self.config.writable_duration_s
    }

    async fn freeze_oldest_writable(&self) -> Result<(), io::Error> {
        let first_writable_partition_read_guard = {
            let mut writable_writable_partitions = self.writable_partitions.write().await;
            writable_writable_partitions.pop_front().unwrap()
        };
        let frozen_partition = {
            let first_writable_partition = first_writable_partition_read_guard.read().await;
            first_writable_partition.freeze(&self.config).await?
        };
        {
            let readable_frozen_partition_job = frozen_partition.clone().materialize();
            let wrtiable_readonly_partitions_job = self.readonly_partitions.write();
            let (readable_frozen_partition, mut writable_readonly_partitions) = tokio::join!(
                readable_frozen_partition_job,
                wrtiable_readonly_partitions_job
            );
            writable_readonly_partitions.push(Arc::new(readable_frozen_partition?));
        }
        {
            let mut writable_partitions_file_header = self.partitions_file_header.write().await;
            writable_partitions_file_header
                .read_only_time_partitions
                .push(frozen_partition);
            writable_partitions_file_header
                .writable_time_partitions
                .remove(0);

            writable_partitions_file_header
                .to_file(&self.config.partitions_file_path())
                .await?;
        }
        Ok(())
    }

    pub async fn import_stream(&self, stream: ImportStream) -> Result<(), io::Error> {
        self.write_stream(stream).await;

        if self.need_new_writable_partition().await {
            self.start_next_stream().await?;
        }

        if self.writable_freeze_required().await {
            self.freeze_oldest_writable().await?;
        }

        Ok(())
    }
}
