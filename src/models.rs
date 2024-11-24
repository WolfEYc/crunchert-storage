use dashmap::DashMap;
use memmap2::{Mmap, MmapMut};
use serde::{Deserialize, Serialize};
use soa_derive::StructOfArray;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::File, marker::PhantomData};
use tokio::sync::{Mutex, RwLock};

#[derive(Default, Debug, Clone, Copy)]
pub struct ValueTracker {
    pub value: f32,
    pub count: u32,
}
#[derive(StructOfArray)]
pub struct StreamPoint {
    pub timestamp: i64,
    pub stream_id: u64,
    pub value: f32,
}

pub struct ImportStream {
    pub pts: StreamPointVec,
}

#[derive(Clone, Copy)]
pub enum Aggregation {
    Sum,
    Avg,
    Min,
    Max,
}

pub struct ChartRequest {
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
    pub data_storage_dir: PathBuf,
    pub compression_level: usize,
    pub retention_period_s: usize,
    pub stream_cache_ttl_s: usize,
    pub data_frequency_s: usize,
    pub writable_partition_size: usize,
    pub writable_partition_ideal_pct_full: f32,
    pub writable_partitions: usize,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct ReadOnlyDiskStreamFileHeader {
    pub stream_id: u64,
    pub unix_s_byte_start: usize,
    pub unix_s_byte_stop: usize,
    pub values_byte_stop: usize,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ReadOnlyTimePartitionFileHeader {
    pub start_unix_s: i64,
    pub file_path: PathBuf,
    pub disk_streams: Vec<ReadOnlyDiskStreamFileHeader>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct WritableTimePartitionFileHeader {
    pub start_unix_s: i64,
    pub len: usize,
    pub cap: usize,
    pub timestamps_file_path: PathBuf,
    pub stream_ids_file_path: PathBuf,
    pub values_file_path: PathBuf,
}

#[derive(Serialize, Deserialize)]
pub struct PartitionsFileHeader {
    // sorted descending start_unix_s
    pub read_only_time_partitions: Vec<ReadOnlyTimePartitionFileHeader>,
    pub writable_time_partitions: Vec<WritableTimePartitionFileHeader>,
}

#[derive(Default)]
pub struct HotStream {
    pub unix_seconds: Vec<i64>,
    pub values: Vec<f32>,
}

pub struct ReadOnlyStream {
    pub disk_header: ReadOnlyDiskStreamFileHeader,
    pub hot_stream: RwLock<Option<HotStream>>,
    pub last_accessed: Mutex<Option<i64>>,
}

pub struct ReadOnlyTimePartition {
    pub start_unix_s: i64,
    pub mmap: Mmap,
    pub streams: DashMap<u64, ReadOnlyStream>,
}

pub struct ResizableMmapMut<T> {
    pub mmap: MmapMut,
    pub file: File,
    pub cap: usize,
    pub item: PhantomData<T>,
}

pub struct WritableTimePartition {
    pub len: usize,
    pub start_unix_s: i64,
    pub timestamps_mmap: ResizableMmapMut<i64>,
    pub streams_mmap: ResizableMmapMut<u64>,
    pub values_mmap: ResizableMmapMut<f32>,
    pub streams: DashMap<u64, RwLock<HotStream>>,
}

pub struct Storage {
    pub config: StorageConfig,
    pub partitions_file_header: RwLock<PartitionsFileHeader>,
    pub readonly_partitions: RwLock<Vec<Arc<ReadOnlyTimePartition>>>,
    pub writable_partitions: RwLock<Vec<Arc<RwLock<WritableTimePartition>>>>,
    pub num_threads: usize,
}

#[derive(Clone, Copy)]
pub struct ChartReqMetadata {
    pub start_unix_s: i64,
    pub stop_unix_s: i64,
    pub step_s: u32,
    pub resolution: usize,
}
