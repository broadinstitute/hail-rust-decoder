//! Cloud storage writer for streaming output to object stores
//!
//! This module provides `CloudWriter`, which implements `std::io::Write` and
//! buffers data before uploading to cloud storage (GCS, S3).
//!
//! ## Architecture
//!
//! For Parquet files (typically <1GB per partition), we buffer the entire file
//! in memory and upload on `finish()`. This is simpler and more reliable than
//! multipart uploads for files of this size.
//!
//! For larger files or streaming use cases, consider using the multipart upload
//! API directly with `object_store`'s `put_multipart`.

use crate::io::adapter::IO_RUNTIME;
use crate::{HailError, Result};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use std::io::Write;
use std::sync::Arc;
use url::Url;

/// A writer that buffers data and uploads to cloud storage on finish
///
/// This writer implements `std::io::Write`, allowing it to be used with
/// `ParquetWriter` and other components expecting synchronous writers.
///
/// # Usage
///
/// ```no_run
/// use hail_decoder::io::CloudWriter;
///
/// let writer = CloudWriter::new("gs://bucket/path/output.parquet")?;
/// // Write data...
/// writer.finish()?; // Uploads to GCS
/// # Ok::<(), hail_decoder::HailError>(())
/// ```
pub struct CloudWriter {
    /// The object store to write to
    store: Arc<dyn ObjectStore>,
    /// The path within the store
    path: ObjPath,
    /// Buffer accumulating written data
    buffer: Vec<u8>,
}

impl CloudWriter {
    /// Create a new CloudWriter for the given cloud URL
    ///
    /// # Arguments
    /// * `url_str` - Cloud storage URL (gs://, s3://)
    ///
    /// # Example
    /// ```no_run
    /// let writer = CloudWriter::new("gs://my-bucket/output.parquet")?;
    /// # Ok::<(), hail_decoder::HailError>(())
    /// ```
    pub fn new(url_str: &str) -> Result<Self> {
        let url = Url::parse(url_str)
            .map_err(|e| HailError::InvalidFormat(format!("Invalid URL: {}", e)))?;

        let (store, path): (Arc<dyn ObjectStore>, ObjPath) = match url.scheme() {
            #[cfg(feature = "gcp")]
            "gs" => {
                let bucket = url.host_str()
                    .ok_or_else(|| HailError::InvalidFormat("Missing bucket in GCS URL".to_string()))?;
                let path = url.path().trim_start_matches('/');

                let gcs = object_store::gcp::GoogleCloudStorageBuilder::new()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| HailError::InvalidFormat(format!("Failed to create GCS client: {}", e)))?;

                (Arc::new(gcs), ObjPath::from(path))
            }
            #[cfg(feature = "aws")]
            "s3" => {
                let bucket = url.host_str()
                    .ok_or_else(|| HailError::InvalidFormat("Missing bucket in S3 URL".to_string()))?;
                let path = url.path().trim_start_matches('/');

                let s3 = object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| HailError::InvalidFormat(format!("Failed to create S3 client: {}", e)))?;

                (Arc::new(s3), ObjPath::from(path))
            }
            scheme => {
                return Err(HailError::InvalidFormat(format!("Unsupported URL scheme for writing: {}", scheme)));
            }
        };

        Ok(CloudWriter {
            store,
            path,
            buffer: Vec::with_capacity(64 * 1024 * 1024), // Pre-allocate 64MB
        })
    }

    /// Create a CloudWriter from an existing ObjectStore and path
    ///
    /// This is useful when you already have a configured ObjectStore instance.
    pub fn from_store(store: Arc<dyn ObjectStore>, path: ObjPath) -> Self {
        CloudWriter {
            store,
            path,
            buffer: Vec::with_capacity(64 * 1024 * 1024),
        }
    }

    /// Finish writing and upload the data to cloud storage
    ///
    /// This must be called after all data has been written. It blocks until
    /// the upload is complete.
    ///
    /// # Returns
    /// The number of bytes uploaded.
    pub fn finish(self) -> Result<usize> {
        let size = self.buffer.len();
        let store = self.store;
        let path = self.path;
        let data = self.buffer;

        tracing::trace!("CloudWriter: uploading {} bytes to {:?}", size, path);
        let start_time = std::time::Instant::now();

        IO_RUNTIME.block_on(async move {
            store.put(&path, data.into()).await
        }).map_err(|e| HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to upload to cloud storage: {}", e),
        )))?;

        tracing::trace!("CloudWriter: upload completed in {:?}", start_time.elapsed());
        Ok(size)
    }

    /// Get the current buffer size (bytes written so far)
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if no data has been written
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

impl Write for CloudWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // No-op: we flush everything on finish()
        Ok(())
    }
}

/// Enum wrapping either a local file or cloud writer for unified handling
pub enum OutputWriter {
    /// Local file system writer
    File(std::fs::File),
    /// Cloud storage writer
    Cloud(CloudWriter),
}

impl OutputWriter {
    /// Create an OutputWriter for the given path
    ///
    /// Automatically detects whether the path is local or cloud and creates
    /// the appropriate writer.
    pub fn new(path: &str) -> Result<Self> {
        if crate::io::is_cloud_path(path) {
            Ok(OutputWriter::Cloud(CloudWriter::new(path)?))
        } else {
            let file = std::fs::File::create(path)?;
            Ok(OutputWriter::File(file))
        }
    }

    /// Finish writing
    ///
    /// For cloud writers, this uploads the buffered data.
    /// For local files, this syncs and closes the file.
    pub fn finish(self) -> Result<usize> {
        match self {
            OutputWriter::File(file) => {
                file.sync_all()?;
                Ok(0) // File doesn't track bytes written in this simple implementation
            }
            OutputWriter::Cloud(writer) => writer.finish(),
        }
    }
}

impl Write for OutputWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            OutputWriter::File(file) => file.write(buf),
            OutputWriter::Cloud(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            OutputWriter::File(file) => file.flush(),
            OutputWriter::Cloud(writer) => writer.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_output_writer_local() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut writer = OutputWriter::new(path).unwrap();
        writer.write_all(b"Hello, world!").unwrap();
        writer.finish().unwrap();

        let content = std::fs::read_to_string(path).unwrap();
        assert_eq!(content, "Hello, world!");
    }
}
