mod config;
mod writer;

pub type BatchingStoreWriterConfig = config::BatchingStoreWriterConfig;
pub type BatchingStoreWriter<W> = writer::BatchingStoreWriter<W>;
