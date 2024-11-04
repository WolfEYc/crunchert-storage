use crate::constants::*;
use postcard;
use std::io;

#[derive(thiserror::Error, Debug)]
pub enum StorageCreationError {
    #[error("io error")]
    IOError(#[from] io::Error),
    #[error("config error")]
    ConfigError(#[from] StorageConfigError),
    #[error("postcard deserialization error")]
    DeserializationError(#[from] postcard::Error),
}
#[derive(thiserror::Error, Debug)]
pub enum ImportStreamError {
    #[error("io error")]
    IOError(#[from] io::Error),
    #[error("input must be non empty")]
    EmptyInputError,
}

#[derive(thiserror::Error, Debug)]
pub enum StorageConfigError {
    #[error("COMPRESSION_LEVEL must be >= {MIN_COMPRESSION_LEVEL}")]
    ToLowCompressionLevel,
    #[error("COMPRESSION_LEVEL must be <= {MAX_COMPRESSION_LEVEL}")]
    ToHighCompressionLevel,
    #[error("RETENTION_PERIOD_S must be >= {MIN_RETENTION_PERIOD_S}")]
    ToLowRetentionPeriod,
    #[error("RETENTION_PERIOD_S must be <= {MAX_RETENTION_PERIOD_S}")]
    ToHighRetentionPeriod,
    #[error("WRITABLE_PARTITIONS must be >= {MIN_WRITEABLE_PARTITIONS}")]
    ToLowWritablePartitions,
    #[error("WRITABLE_PARTITION_SIZE must be >= {MIN_WRITEABLE_PARTITION_SIZE}")]
    ToLowWritablePartitionSize,
    #[error("WRITABLE_PARTITION_SIZE must be <= {MAX_WRITEABLE_PARTITION_SIZE}")]
    ToHighWritablePartitionSize,
    #[error("DATA_FREQUENCY_S must be <= {MAX_DATA_FREQUENCY_S}")]
    ToHighDataFrequency,
}
