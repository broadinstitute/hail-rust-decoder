//! Zstd decompression buffer
//!
//! Reads Zstd-compressed blocks formatted as: [4-byte decompressed size][zstd data]

use crate::error::{HailError, Result};
use crate::buffer::InputBuffer;

/// Zstd decompression buffer
pub struct ZstdBuffer<B: InputBuffer> {
    inner: B,
    decompressed: Vec<u8>,
    position: usize,
}

impl<B: InputBuffer> ZstdBuffer<B> {
    /// Create a new Zstd buffer
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            decompressed: Vec::new(),
            position: 0,
        }
    }

    /// Read and decompress the next block
    fn read_next_block(&mut self) -> Result<bool> {
        // Read 4-byte decompressed size
        let decompressed_size = match self.inner.read_i32() {
            Ok(size) => size as usize,
            Err(HailError::UnexpectedEof) => return Ok(false),
            Err(e) => return Err(e),
        };

        // Read the Zstd magic number (0xFD2FB528)
        let mut magic = [0u8; 4];
        self.inner.read_exact(&mut magic)?;

        if magic != [0x28, 0xB5, 0x2F, 0xFD] {
            return Err(HailError::InvalidFormat(format!(
                "Invalid Zstd magic number: {:02x}{:02x}{:02x}{:02x}",
                magic[0], magic[1], magic[2], magic[3]
            )));
        }

        // For now, we need to read the full compressed block
        // In a real implementation, we'd need to parse the Zstd frame header
        // to determine the compressed size. For simplicity, we'll buffer and decompress.
        //
        // This is a simplified version - a production implementation would need
        // to properly parse Zstd frame headers or use a streaming API.

        // TODO: Implement proper Zstd frame reading
        // For now, this is a stub that will be completed in Phase 1
        // Need to read the rest of the Zstd frame and decompress

        self.decompressed.clear();
        self.decompressed.resize(decompressed_size, 0);

        // Decompress using zstd crate
        // This is where we'll integrate the zstd library

        self.position = 0;
        Ok(true)
    }

    fn available(&self) -> usize {
        self.decompressed.len().saturating_sub(self.position)
    }
}

impl<B: InputBuffer> InputBuffer for ZstdBuffer<B> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut offset = 0;

        while offset < buf.len() {
            if self.available() == 0 {
                if !self.read_next_block()? {
                    return Err(HailError::UnexpectedEof);
                }
            }

            let to_copy = buf.len() - offset;
            let available = self.available();
            let copy_len = to_copy.min(available);

            buf[offset..offset + copy_len]
                .copy_from_slice(&self.decompressed[self.position..self.position + copy_len]);

            self.position += copy_len;
            offset += copy_len;
        }

        Ok(())
    }
}

// TODO: Add tests once Zstd decompression is fully implemented
