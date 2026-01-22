//! Cloud storage adapter that bridges async object_store with sync std::io::Read
//!
//! This module provides a `CloudReader` that fetches data in large chunks from
//! cloud storage and serves it synchronously through the `std::io::Read` trait.

use crate::{HailError, Result};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing::trace;
use url::Url;

/// Default chunk size for cloud reads (8MB)
const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Shared Tokio runtime for IO operations
static IO_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Runtime::new().expect("Failed to create IO runtime")
});

/// A reader that fetches data from cloud storage in chunks
///
/// This struct implements `std::io::Read` and `std::io::Seek`, allowing it to be
/// used with the synchronous buffer stack. Data is fetched from cloud storage
/// in large chunks to minimize HTTP request overhead.
pub struct CloudReader {
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    position: u64,
    buffer: Vec<u8>,
    buffer_start: u64,
    file_size: u64,
    chunk_size: usize,
}

impl CloudReader {
    /// Create a new CloudReader for the given store and path
    pub fn new(store: Arc<dyn ObjectStore>, path: ObjPath, file_size: u64) -> Self {
        CloudReader {
            store,
            path,
            position: 0,
            buffer: Vec::new(),
            buffer_start: 0,
            file_size,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Set the chunk size for reads
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Fill the internal buffer starting from the current position
    fn fill_buffer(&mut self) -> std::io::Result<()> {
        // Calculate the range to fetch
        let start = self.position;
        let end = std::cmp::min(self.position + self.chunk_size as u64, self.file_size);

        if start >= self.file_size {
            self.buffer.clear();
            self.buffer_start = start;
            return Ok(());
        }

        trace!("CloudReader: fetching range {}..{}", start, end);

        // Fetch the range from cloud storage
        let range = start as usize..end as usize;
        let store = self.store.clone();
        let path = self.path.clone();

        let start_time = std::time::Instant::now();
        let bytes = IO_RUNTIME.block_on(async {
            store.get_range(&path, range).await
        }).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        trace!("CloudReader: fetch completed in {:?}", start_time.elapsed());

        self.buffer = bytes.to_vec();
        self.buffer_start = start;
        Ok(())
    }

    /// Check if the current position is within the buffered range
    fn position_in_buffer(&self) -> bool {
        if self.buffer.is_empty() {
            return false;
        }
        let buffer_end = self.buffer_start + self.buffer.len() as u64;
        self.position >= self.buffer_start && self.position < buffer_end
    }

    /// Get the offset within the buffer for the current position
    fn buffer_offset(&self) -> usize {
        (self.position - self.buffer_start) as usize
    }
}

impl Read for CloudReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.file_size {
            return Ok(0); // EOF
        }

        // Refill buffer if needed
        if !self.position_in_buffer() {
            self.fill_buffer()?;
        }

        // Copy data from buffer to output
        let offset = self.buffer_offset();
        let available = self.buffer.len() - offset;
        let to_copy = std::cmp::min(available, buf.len());

        if to_copy == 0 {
            return Ok(0); // EOF
        }

        buf[..to_copy].copy_from_slice(&self.buffer[offset..offset + to_copy]);
        self.position += to_copy as u64;
        Ok(to_copy)
    }
}

impl Seek for CloudReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_position = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                if offset >= 0 {
                    self.file_size.saturating_add(offset as u64)
                } else {
                    self.file_size.saturating_sub((-offset) as u64)
                }
            }
            SeekFrom::Current(offset) => {
                if offset >= 0 {
                    self.position.saturating_add(offset as u64)
                } else {
                    self.position.saturating_sub((-offset) as u64)
                }
            }
        };

        self.position = new_position;
        Ok(self.position)
    }
}

/// A boxed reader that can be either a local file or a cloud reader
pub type BoxedReader = Box<dyn Read + Send + Sync>;

/// Create a reader for a path, detecting whether it's local or cloud
///
/// Supported URL schemes:
/// - `gs://bucket/path` - Google Cloud Storage
/// - `s3://bucket/path` - Amazon S3
/// - `http://` or `https://` - HTTP(S) URLs
/// - Local file path - Regular file system access
pub fn get_reader(path: &str) -> Result<BoxedReader> {
    if path.starts_with("gs://") || path.starts_with("s3://") || path.starts_with("http://") || path.starts_with("https://") {
        create_cloud_reader(path)
    } else {
        // Local file
        let file = File::open(path)
            .map_err(|e| HailError::Io(e))?;
        Ok(Box::new(file))
    }
}

/// Create a cloud reader for the given URL
fn create_cloud_reader(url_str: &str) -> Result<BoxedReader> {
    let url = Url::parse(url_str)
        .map_err(|e| HailError::InvalidFormat(format!("Invalid URL: {}", e)))?;

    let (store, path): (Arc<dyn ObjectStore>, ObjPath) = match url.scheme() {
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
        "http" | "https" => {
            // For HTTP, use the HttpStore
            let http = object_store::http::HttpBuilder::new()
                .with_url(url_str)
                .build()
                .map_err(|e| HailError::InvalidFormat(format!("Failed to create HTTP client: {}", e)))?;

            (Arc::new(http), ObjPath::from(""))
        }
        scheme => {
            return Err(HailError::InvalidFormat(format!("Unsupported URL scheme: {}", scheme)));
        }
    };

    // Get the file size via HEAD request
    let file_size = IO_RUNTIME.block_on(async {
        store.head(&path).await
    }).map_err(|e| HailError::InvalidFormat(format!("Failed to get file metadata: {}", e)))?.size as u64;

    Ok(Box::new(CloudReader::new(store, path, file_size)))
}

/// Join a base path with a child path, handling both local and cloud paths correctly
///
/// For cloud URLs, this handles path joining with forward slashes.
/// For local paths, this uses the OS-appropriate separator.
pub fn join_path(base: &str, child: &str) -> String {
    // Normalize child path (remove leading separators)
    let child = child.trim_start_matches('/').trim_start_matches('\\');

    if base.starts_with("gs://") || base.starts_with("s3://") || base.starts_with("http://") || base.starts_with("https://") {
        // Cloud URL - always use forward slashes
        let base = base.trim_end_matches('/');
        format!("{}/{}", base, child)
    } else {
        // Local path - use std::path for OS-appropriate handling
        let path = std::path::Path::new(base).join(child);
        path.to_string_lossy().to_string()
    }
}

/// Check if a path is a cloud URL
pub fn is_cloud_path(path: &str) -> bool {
    path.starts_with("gs://") || path.starts_with("s3://") || path.starts_with("http://") || path.starts_with("https://")
}

/// Read a specific byte range from a file (local or cloud)
///
/// This is the key function for efficient random access. For cloud storage,
/// it uses HTTP Range requests to fetch only the needed bytes.
///
/// # Arguments
/// * `path` - Path to the file (local or cloud URL)
/// * `offset` - Start offset in bytes
/// * `length` - Number of bytes to read
///
/// # Returns
/// A vector containing the requested bytes
pub fn range_read(path: &str, offset: u64, length: usize) -> Result<Vec<u8>> {
    if is_cloud_path(path) {
        range_read_cloud(path, offset, length)
    } else {
        range_read_local(path, offset, length)
    }
}

/// Read a byte range from a local file
fn range_read_local(path: &str, offset: u64, length: usize) -> Result<Vec<u8>> {
    use std::fs::File;

    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut buf = vec![0u8; length];
    let bytes_read = file.read(&mut buf)?;
    buf.truncate(bytes_read);

    Ok(buf)
}

/// Read a byte range from a cloud storage URL
fn range_read_cloud(url_str: &str, offset: u64, length: usize) -> Result<Vec<u8>> {
    let url = Url::parse(url_str)
        .map_err(|e| HailError::InvalidFormat(format!("Invalid URL: {}", e)))?;

    let (store, path): (Arc<dyn ObjectStore>, ObjPath) = match url.scheme() {
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
        "http" | "https" => {
            let http = object_store::http::HttpBuilder::new()
                .with_url(url_str)
                .build()
                .map_err(|e| HailError::InvalidFormat(format!("Failed to create HTTP client: {}", e)))?;
            (Arc::new(http), ObjPath::from(""))
        }
        scheme => {
            return Err(HailError::InvalidFormat(format!("Unsupported URL scheme: {}", scheme)));
        }
    };

    let range = offset as usize..(offset as usize + length);
    let bytes = IO_RUNTIME.block_on(async {
        store.get_range(&path, range).await
    }).map_err(|e| HailError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

    Ok(bytes.to_vec())
}

/// Read a single compressed block from a partition file
///
/// This reads the block at the given file offset, including the 4-byte
/// length header and the block data.
///
/// # Arguments
/// * `path` - Path to the partition file (local or cloud)
/// * `file_offset` - Offset into the file where the block starts
///
/// # Returns
/// A tuple of (block_data, compressed_length) where block_data includes
/// the decompressed size prefix (first 4 bytes)
pub fn read_single_block(path: &str, file_offset: u64) -> Result<Vec<u8>> {
    // Read the 4-byte block length header first
    let header = range_read(path, file_offset, 4)?;
    if header.len() < 4 {
        return Err(HailError::UnexpectedEof);
    }
    let block_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;

    // Now read the actual block data
    let block_data = range_read(path, file_offset + 4, block_len)?;

    Ok(block_data)
}

/// Get the size of a file (local or cloud)
///
/// # Arguments
/// * `path` - Path to the file (local or cloud URL)
///
/// # Returns
/// The size of the file in bytes
pub fn get_file_size(path: &str) -> Result<u64> {
    if is_cloud_path(path) {
        let url = Url::parse(path)
            .map_err(|e| HailError::InvalidFormat(format!("Invalid URL: {}", e)))?;

        let (store, obj_path): (Arc<dyn ObjectStore>, ObjPath) = match url.scheme() {
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
            "http" | "https" => {
                let http = object_store::http::HttpBuilder::new()
                    .with_url(path)
                    .build()
                    .map_err(|e| HailError::InvalidFormat(format!("Failed to create HTTP client: {}", e)))?;
                (Arc::new(http), ObjPath::from(""))
            }
            scheme => {
                return Err(HailError::InvalidFormat(format!("Unsupported URL scheme: {}", scheme)));
            }
        };

        let meta = IO_RUNTIME.block_on(async {
            store.head(&obj_path).await
        }).map_err(|e| HailError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

        Ok(meta.size as u64)
    } else {
        let metadata = std::fs::metadata(path)?;
        Ok(metadata.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_path_local() {
        let base = "/path/to/table.ht";
        let child = "rows/metadata.json.gz";
        let result = join_path(base, child);
        assert!(result.contains("rows"));
        assert!(result.contains("metadata.json.gz"));
    }

    #[test]
    fn test_join_path_gcs() {
        let base = "gs://my-bucket/data/table.ht";
        let child = "rows/metadata.json.gz";
        let result = join_path(base, child);
        assert_eq!(result, "gs://my-bucket/data/table.ht/rows/metadata.json.gz");
    }

    #[test]
    fn test_join_path_s3() {
        let base = "s3://my-bucket/data/table.ht/";
        let child = "/rows/metadata.json.gz";
        let result = join_path(base, child);
        assert_eq!(result, "s3://my-bucket/data/table.ht/rows/metadata.json.gz");
    }

    #[test]
    fn test_is_cloud_path() {
        assert!(is_cloud_path("gs://bucket/path"));
        assert!(is_cloud_path("s3://bucket/path"));
        assert!(is_cloud_path("https://example.com/path"));
        assert!(!is_cloud_path("/local/path"));
        assert!(!is_cloud_path("./relative/path"));
    }

    #[test]
    fn test_get_reader_local_file() {
        // This test uses a file that exists in the repo
        let result = get_reader("Cargo.toml");
        assert!(result.is_ok());
    }
}
