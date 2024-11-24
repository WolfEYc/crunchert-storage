use crate::constants::*;
use crate::errors::StorageConfigError;
use crate::models::StorageConfig;
use std::path::PathBuf;

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            compression_level: 8,
            retention_period_s: 31536000,        //1y
            stream_cache_ttl_s: 900,             //15m
            writable_partition_size: 1296000000, // some made up sh*t
            writable_partitions: 2,
            data_frequency_s: 900, //15m
            writable_partition_ideal_pct_full: 0.75,
            data_storage_dir: PathBuf::from("/var/lib/wolfeymetrics"),
        }
    }
}
impl StorageConfig {
    pub fn validate(self) -> Result<Self, StorageConfigError> {
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

        if self.writable_partitions < MIN_WRITEABLE_PARTITIONS {
            return Err(StorageConfigError::ToLowWritablePartitions);
        }

        if self.writable_partition_size < MIN_WRITEABLE_PARTITION_SIZE {
            return Err(StorageConfigError::ToLowWritablePartitionSize);
        }
        if self.writable_partition_size > MAX_WRITEABLE_PARTITION_SIZE {
            return Err(StorageConfigError::ToHighWritablePartitionSize);
        }
        if self.data_frequency_s > MAX_DATA_FREQUENCY_S {
            return Err(StorageConfigError::ToHighDataFrequency);
        }
        if self.writable_partition_ideal_pct_full < MIN_WRITABLE_PARTITION_IDEAL_PCT_FULL {
            return Err(StorageConfigError::ToLowWritablePartitionIdealPctFull);
        }
        if self.writable_partition_ideal_pct_full > MAX_WRITABLE_PARTITION_IDEAL_PCT_FULL {
            return Err(StorageConfigError::ToHighWritablePartitionSize);
        }

        Ok(self)
    }
}
