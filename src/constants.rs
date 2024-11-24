pub const PARTITIONS_FILE_HEADER_FILENAME: &str = "CruncheRTPartitionsConfig";
pub const MIN_STREAMS_PER_THREAD: usize = 1024;
pub const MIN_COMPRESSION_LEVEL: usize = 4;
pub const MAX_COMPRESSION_LEVEL: usize = 12;
pub const MIN_RETENTION_PERIOD_S: usize = 900; //15m
pub const MAX_RETENTION_PERIOD_S: usize = 3156000000; //100y
pub const MIN_WRITEABLE_PARTITIONS: usize = 2;
pub const MAX_DATA_FREQUENCY_S: usize = 86400;
pub const MIN_WRITEABLE_PARTITION_SIZE: usize = u16::MAX as usize;
pub const MAX_WRITEABLE_PARTITION_SIZE: usize = u32::MAX as usize;
pub const MIN_WRITABLE_PARTITION_IDEAL_PCT_FULL: f32 = 0.10;
pub const MAX_WRITABLE_PARTITION_IDEAL_PCT_FULL: f32 = 0.95;
