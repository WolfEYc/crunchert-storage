use dashmap::DashMap;
use itertools::Itertools;
use memmap2::Mmap;
use pco::standalone::simple_decompress;
use postcard::from_bytes;

use std::cmp::{max, min};
use std::sync::Arc;
use std::{fmt::Debug, path::PathBuf};
use std::{fs, io, usize};
use tokio::task::JoinSet;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

const PARTITIONS_FILE_HEADER_FILENAME: &str = "CruncheRTPartitionsConfig";
const MIN_PTS_TO_COMPRESS: usize = 8192;
const MIN_STREAMS_PER_THREAD: usize = 1024;

pub struct AggChartRequest {
    pub stream_ids: Vec<u64>,
    pub start_unix_s: i64,
    pub stop_unix_s: i64,
    pub step_s: u32,
    pub aggregation: i32,
}

#[derive(Clone, Copy)]
pub enum Aggregation {
    Sum,
    Avg,
    Min,
    Max,
}

pub struct NonAggChartRequest {
    pub stream_ids: Vec<u64>,
    pub start_unix_s: i64,
    pub stop_unix_s: i64,
    pub step_s: u32,
}

#[derive(Debug, Copy, Clone)]
pub struct Datapoint {
    pub unix_s: i64,
    pub value: f32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StorageConfig {
    pub compression_level: usize,
    pub retention_period_s: usize,
    pub cold_storage_after_s: usize,
    pub data_frequency_s: usize,
    pub stream_cache_ttl_s: usize,
    pub data_storage_dir: PathBuf,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
struct DiskStreamFileHeader {
    stream_id: u64,
    unix_s_byte_start: usize,
    unix_s_byte_stop: usize,
    values_byte_stop: usize,
    compressed: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct TimePartitionFileHeader {
    start_unix_s: i64,
    file_path: PathBuf,
    disk_streams: Vec<DiskStreamFileHeader>,
}

impl TimePartitionFileHeader {
    fn new(config: &StorageConfig) -> Self {
        let now = Utc::now().timestamp();
        let file_path = config.data_storage_dir.join(now.to_string());
        Self {
            start_unix_s: now,
            file_path,
            disk_streams: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct PartitionsFileHeader {
    // sorted descending start_unix_s
    time_partitions: Vec<TimePartitionFileHeader>,
}

#[derive(Default)]
struct HotStream {
    unix_seconds: Vec<i64>,
    values: Vec<f32>,
}

struct Stream {
    disk_header: DiskStreamFileHeader,
    hot_stream: RwLock<Option<HotStream>>,
    last_accessed: Mutex<Option<i64>>,
}

struct TimePartition {
    start_unix_s: i64,
    mmap: Mmap,
    streams: DashMap<u64, Stream>,
}

pub struct Storage {
    config: StorageConfig,
    partitions: RwLock<Vec<Arc<TimePartition>>>,
    partition_file_header: PartitionsFileHeader,
    num_threads: usize,
}

#[derive(Clone, Copy)]
struct ChartReqMetadata {
    start_unix_s: i64,
    stop_unix_s: i64,
    step_s: u32,
    resolution: usize,
}

#[inline]
fn resolution(start_unix_s: i64, stop_unix_s: i64, step_s: u32) -> usize {
    let duration_s = (stop_unix_s - start_unix_s) as u32;
    let resolution = duration_s / step_s;
    return resolution as usize;
}

impl ChartReqMetadata {
    fn from_agg_chart_req(value: &AggChartRequest) -> Self {
        Self {
            start_unix_s: value.start_unix_s,
            stop_unix_s: value.stop_unix_s,
            step_s: value.step_s,
            resolution: resolution(value.start_unix_s, value.stop_unix_s, value.step_s),
        }
    }
    fn from_non_agg_chart_req(value: &NonAggChartRequest) -> Self {
        Self {
            start_unix_s: value.start_unix_s,
            stop_unix_s: value.stop_unix_s,
            step_s: value.step_s,
            resolution: resolution(value.start_unix_s, value.stop_unix_s, value.step_s),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            compression_level: 8,
            retention_period_s: 31104000,  //1y
            cold_storage_after_s: 7776000, //90d
            data_frequency_s: 900,
            stream_cache_ttl_s: 900,
            data_storage_dir: PathBuf::from("/var/lib/wolfeymetrics"),
        }
    }
}

const MIN_COMPRESSION_LEVEL: usize = 4;
const MAX_COMPRESSION_LEVEL: usize = 12;
const MIN_RETENTION_PERIOD_S: usize = 900; //15m
const MIN_COLD_STORAGE_S: usize = 7776000; //90d
const MAX_RETENTION_PERIOD_S: usize = 3156000000; //100y
const MAX_DATA_FREQUENCY_S: usize = 604800; //7d

#[derive(thiserror::Error, Debug)]
pub enum StorageConfigError {
    #[error("COMPRESSION_LEVEL must be >= {MIN_COMPRESSION_LEVEL}")]
    ToLowCompressionLevel,
    #[error("COMPRESSION_LEVEL must be <= {MAX_COMPRESSION_LEVEL}")]
    ToHighCompressionLevel,
    #[error("RETENTION_PERIOD must be >= {MIN_RETENTION_PERIOD_S}")]
    ToLowRetentionPeriod,
    #[error("RETENTION_PERIOD must be <= {MAX_RETENTION_PERIOD_S}")]
    ToHighRetentionPeriod,
    #[error("RETENTION_PERIOD_S must be >= COLD_STORAGE_AFTER_S")]
    ColdStorageCannotBeGreaterThanRetention,
    #[error("COLD_STORAGE_AFTER_S must be >= {MIN_COLD_STORAGE_S} or RETENTION_PERIOD_S")]
    ColdStorageTooLow,
    #[error("DATA_FREQUENCY_S must be <= {MAX_DATA_FREQUENCY_S}")]
    DataFrequencyTooHigh,
}

impl StorageConfig {
    fn validate(self) -> Result<Self, StorageConfigError> {
        if self.compression_level < MIN_COMPRESSION_LEVEL {
            return Err(StorageConfigError::ToLowCompressionLevel);
        }

        if self.compression_level > MAX_COMPRESSION_LEVEL {
            return Err(StorageConfigError::ToHighCompressionLevel);
        }

        if self.retention_period_s < MIN_RETENTION_PERIOD_S {
            return Err(StorageConfigError::ToLowRetentionPeriod);
        }
        if self.retention_period_s > MAX_RETENTION_PERIOD_S {
            return Err(StorageConfigError::ToHighRetentionPeriod);
        }

        if self.retention_period_s < self.cold_storage_after_s {
            return Err(StorageConfigError::ColdStorageCannotBeGreaterThanRetention);
        }

        let min_cold_storage_s = std::cmp::min(MIN_COLD_STORAGE_S, self.retention_period_s);

        if self.cold_storage_after_s < min_cold_storage_s {
            return Err(StorageConfigError::ColdStorageTooLow);
        }

        if self.data_frequency_s > MAX_DATA_FREQUENCY_S {
            return Err(StorageConfigError::DataFrequencyTooHigh);
        }

        Ok(self)
    }
}

impl DiskStreamFileHeader {
    fn read_stream_from_mmap(&self, mmap: &Mmap) -> HotStream {
        let unix_s_bytes = &mmap[self.unix_s_byte_start..self.unix_s_byte_stop];
        let value_bytes = &mmap[self.unix_s_byte_stop..self.values_byte_stop];

        let Ok(unix_s_decompressed) = simple_decompress(unix_s_bytes) else {
            return HotStream::default();
        };
        let Ok(values_decompressed) = simple_decompress(value_bytes) else {
            return HotStream::default();
        };

        HotStream {
            unix_seconds: unix_s_decompressed,
            values: values_decompressed,
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
struct ValueTracker {
    value: f32,
    count: u32,
}

impl ValueTracker {
    #[inline]
    fn agg<F>(self, rhs: Self, agg_fn: &F) -> Self
    where
        F: Fn(f32, f32) -> f32,
    {
        Self {
            value: agg_fn(self.value, rhs.value),
            count: self.count + rhs.count,
        }
    }
    #[inline]
    fn apply<F>(self, rhs: f32, agg_fn: &F) -> Self
    where
        F: Fn(f32, f32) -> f32,
    {
        Self {
            value: agg_fn(self.value, rhs),
            count: self.count + 1,
        }
    }
}
#[inline]
fn agg_to_agg_fn(agg: Aggregation) -> impl Fn(f32, f32) -> f32 {
    match agg {
        Aggregation::Sum => std::ops::Add::add,
        Aggregation::Avg => std::ops::Add::add,
        Aggregation::Min => f32::min,
        Aggregation::Max => f32::max,
    }
}
#[inline]
fn iter_search_ts(req: ChartReqMetadata) -> impl Iterator<Item = i64> {
    (req.start_unix_s..req.stop_unix_s)
        .rev()
        .step_by(req.step_s as usize)
}

impl HotStream {
    fn get_chart_values(&self, req: ChartReqMetadata) -> impl Iterator<Item = Option<f32>> + '_ {
        iter_search_ts(req).map(move |x| {
            let found_ts_idx = self.unix_seconds.binary_search(&x).unwrap_or_else(|x| x);
            let found_ts_idx = min(found_ts_idx, self.unix_seconds.len() - 1);
            let found_ts = self.unix_seconds[found_ts_idx];
            let diff = x.abs_diff(found_ts);
            if diff < req.step_s as u64 {
                let value = self.values[found_ts_idx];
                Some(value)
            } else {
                None
            }
        })
    }
    fn add_stream_to_chart(
        &self,
        req: ChartReqMetadata,
        aggregated_result: Vec<ValueTracker>,
        agg: Aggregation,
    ) -> Vec<ValueTracker> {
        let chart_values = self.get_chart_values(req);
        let agg_fn = agg_to_agg_fn(agg);
        aggregated_result
            .into_iter()
            .zip(chart_values)
            .filter_map(|(x, y)| match y {
                Some(y) => Some((x, y)),
                None => None,
            })
            .map(|(x, y)| x.apply(y, &agg_fn))
            .collect()
    }
}

impl Stream {
    async fn get_chart_aggregated(
        &self,
        req: ChartReqMetadata,
        mmap: &Mmap,
        aggregated_result: Vec<ValueTracker>,
        agg: Aggregation,
    ) -> Vec<ValueTracker> {
        let mut last_accessed_lock = self.last_accessed.lock().await;
        *last_accessed_lock = Some(Utc::now().timestamp());
        drop(last_accessed_lock);

        let hot_stream_option = self.hot_stream.read().await;
        if let Some(ref x) = *hot_stream_option {
            return x.add_stream_to_chart(req, aggregated_result, agg);
        }
        drop(hot_stream_option);

        let mut writable_hot_stream = self.hot_stream.write().await;

        if let Some(ref x) = *writable_hot_stream {
            return x.add_stream_to_chart(req, aggregated_result, agg);
        }

        let hot_stream = self.disk_header.read_stream_from_mmap(mmap);

        let res = hot_stream.add_stream_to_chart(req, aggregated_result, agg);
        *writable_hot_stream = Some(hot_stream);
        res
    }
}

async fn get_chart_aggregated_batched(
    req: Arc<AggChartRequest>,
    meta: ChartReqMetadata,
    agg: Aggregation,
    thread_idx: usize,
    num_threads: usize,
    time_partition: Arc<TimePartition>,
) -> Vec<ValueTracker> {
    let streams_per_thread = req.stream_ids.len() / num_threads;
    let start_idx = streams_per_thread * thread_idx;
    let stop_idx = if thread_idx == num_threads - 1 {
        req.stream_ids.len()
    } else {
        streams_per_thread * (thread_idx + 1)
    };
    // pretty confident with this math, if it goes wrong, then well shit

    let streams = req.stream_ids[start_idx..stop_idx]
        .iter()
        .filter_map(|x| time_partition.streams.get(x));

    let mut aggregated_batch = vec![ValueTracker::default(); meta.resolution];
    for stream in streams {
        aggregated_batch = stream
            .get_chart_aggregated(meta, &time_partition.mmap, aggregated_batch, agg)
            .await;
    }
    return aggregated_batch;
}

#[inline]
fn default_final_agg_fn(x: ValueTracker) -> f32 {
    x.value
}
#[inline]
fn avg_final_agg(x: ValueTracker) -> f32 {
    x.value / x.count as f32
}

async fn time_partition_get_agg_chart(
    time_partition: Arc<TimePartition>,
    req: Arc<AggChartRequest>,
    agg: Aggregation,
    num_threads: usize,
) -> Vec<Datapoint> {
    let meta = ChartReqMetadata::from_agg_chart_req(&req);

    let threads_requested = req.stream_ids.len() / MIN_STREAMS_PER_THREAD;
    let threads_capped = min(threads_requested, num_threads);
    let num_threads = max(threads_capped, 1);

    let batches = (0..num_threads).map(|x| {
        get_chart_aggregated_batched(
            req.clone(),
            meta,
            agg,
            x,
            num_threads,
            time_partition.clone(),
        )
    });
    let agg_fn = agg_to_agg_fn(agg);
    let batched_agg_chart = JoinSet::from_iter(batches).join_all().await;
    let reduced = batched_agg_chart.into_iter().reduce(|acc, x| {
        acc.into_iter()
            .zip(x)
            .map(|(x, y)| x.agg(y, &agg_fn))
            .collect()
    });

    let Some(reduced) = reduced else {
        panic!("tried to aggregate nothing");
    };

    let final_agg = match agg {
        Aggregation::Avg => avg_final_agg,
        _ => default_final_agg_fn,
    };

    reduced
        .into_iter()
        .zip(iter_search_ts(meta))
        .filter(|(x, _)| x.count > 0)
        .map(|(x, ts)| Datapoint {
            unix_s: ts,
            value: final_agg(x),
        })
        .collect()
}

impl TryFrom<&TimePartitionFileHeader> for TimePartition {
    type Error = io::Error;
    fn try_from(value: &TimePartitionFileHeader) -> Result<Self, Self::Error> {
        let hash_map_iter = value.disk_streams.iter().map(|x| {
            (
                x.stream_id,
                Stream {
                    disk_header: x.clone(),
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

impl PartitionsFileHeader {
    fn new(config: &StorageConfig) -> Self {
        Self {
            time_partitions: vec![TimePartitionFileHeader::new(config)],
        }
    }
    fn thaw(&self, config: &StorageConfig) -> Result<Vec<Arc<TimePartition>>, io::Error> {
        let now = Utc::now().timestamp();
        let cutoff = now - config.cold_storage_after_s as i64;
        self.time_partitions
            .iter()
            .filter(|x| x.start_unix_s > cutoff)
            .map(|x| x.try_into())
            .process_results(|iter| iter.map(Arc::new).collect())
    }
}

pub enum StorageCreationError {
    IOError(io::Error),
    ConfigError(StorageConfigError),
    DeserializationError(postcard::Error),
}

impl Storage {
    async fn get_partitions_in_range(
        &self,
        start_unix_s: i64,
        stop_unix_s: i64,
    ) -> Vec<Arc<TimePartition>> {
        let mut partition_end = Utc::now().timestamp();
        let mut partitions_in_range = Vec::new();
        let partitions = self.partitions.read().await;
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

    pub async fn get_agg_chart(&self, req: AggChartRequest, agg: Aggregation) -> Vec<Datapoint> {
        let time_partitions = self
            .get_partitions_in_range(req.start_unix_s, req.stop_unix_s)
            .await;
        let arc_req = Arc::new(req);
        let datapoint_jobs = time_partitions
            .into_iter()
            .map(|x| time_partition_get_agg_chart(x, arc_req.clone(), agg, self.num_threads));
        let datapoints_nested = JoinSet::from_iter(datapoint_jobs).join_all().await;
        let datapoints_flattened = datapoints_nested.into_iter().flatten().collect();
        return datapoints_flattened;
    }

    pub fn new(config: StorageConfig, num_threads: usize) -> Result<Self, StorageCreationError> {
        let config = config
            .validate()
            .map_err(StorageCreationError::ConfigError)?;

        let partitions_file_path = config
            .data_storage_dir
            .join(PARTITIONS_FILE_HEADER_FILENAME);

        let partition_file_header: PartitionsFileHeader = if partitions_file_path.exists() {
            let partitions_file_header_bytes =
                std::fs::read(partitions_file_path).map_err(StorageCreationError::IOError)?;
            from_bytes(&partitions_file_header_bytes)
                .map_err(StorageCreationError::DeserializationError)?
        } else {
            PartitionsFileHeader::new(&config)
        };

        let partitions = partition_file_header
            .thaw(&config)
            .map_err(StorageCreationError::IOError)?;
        let partitions = RwLock::new(partitions);

        Ok(Self {
            config,
            partitions,
            partition_file_header,
            num_threads,
        })
    }
}