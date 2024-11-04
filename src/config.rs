use crate::constants::*;
use crate::errors::StorageConfigError;
use crate::models::StorageConfig;
use std::path::PathBuf;

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            compression_level: 8,
            retention_period_s: 31536000, //1y
            partition_duration_s: 86400,  //1d
            stream_cache_ttl_s: 900,      //15m
            writable_partitions: 2,
            data_frequency_s: 900, //15m
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

        if self.partition_duration_s < MIN_PARTITION_DURATION_S {
            return Err(StorageConfigError::ToLowPartitionDuration);
        }
        if self.partition_duration_s > MAX_PARTITION_DURATION_S {
            return Err(StorageConfigError::ToHighPartitionDuration);
        }
        if self.data_frequency_s > MAX_DATA_FREQUENCY_S {
            return Err(StorageConfigError::ToHighDataFrequency);
        }

        Ok(self)
    }
}
