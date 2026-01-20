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

        eprintln!("ZSTD: Expected decompressed size: {} bytes", decompressed_size);

        // Read the Zstd magic number (0xFD2FB528 in little-endian: 28 B5 2F FD)
        let mut magic = [0u8; 4];
        self.inner.read_exact(&mut magic)?;

        eprintln!("ZSTD: Magic bytes: {:02x?}", magic);

        if magic != [0x28, 0xB5, 0x2F, 0xFD] {
            return Err(HailError::InvalidFormat(format!(
                "Invalid Zstd magic number: {:02x}{:02x}{:02x}{:02x}",
                magic[0], magic[1], magic[2], magic[3]
            )));
        }

        // Handle empty blocks (decompressed_size = 0)
        if decompressed_size == 0 {
            // Still need to read and validate the Zstd frame
            // Read bytes until we have a valid frame
            let mut compressed_buffer = vec![0x28, 0xB5, 0x2F, 0xFD]; // Start with magic

            // Read bytes one at a time until we can decompress
            loop {
                match zstd::decode_all(&compressed_buffer[..]) {
                    Ok(decompressed) => {
                        if decompressed.is_empty() {
                            self.decompressed.clear();
                            self.position = 0;
                            // Continue to the next block since this one is empty
                            return self.read_next_block();
                        } else {
                            return Err(HailError::InvalidFormat(format!(
                                "Expected empty block but got {} bytes",
                                decompressed.len()
                            )));
                        }
                    }
                    Err(_) => {
                        // Need more data
                        let mut one_byte = [0u8; 1];
                        match self.inner.read_exact(&mut one_byte) {
                            Ok(()) => compressed_buffer.push(one_byte[0]),
                            Err(HailError::UnexpectedEof) => {
                                return Err(HailError::InvalidFormat(
                                    "Incomplete Zstd frame".to_string()
                                ));
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }

                // Safety check
                if compressed_buffer.len() > 1024 {
                    return Err(HailError::InvalidFormat(
                        "Empty block frame too large".to_string()
                    ));
                }
            }
        }

        // We need to read the entire Zstd frame. The challenge is that we don't know
        // the compressed size in advance. We'll use a chunked reading approach where
        // we buffer data and attempt to decompress until we get exactly the expected
        // decompressed size.

        // Strategy: Read chunks and build up a buffer until we can decompress successfully
        let mut compressed_buffer = Vec::with_capacity(decompressed_size); // estimate
        compressed_buffer.extend_from_slice(&magic); // Include the magic we already read

        // We'll read the rest of the frame by attempting decompression iteratively
        // The Zstd decoder will tell us when it has a complete frame
        loop {
            // Try to decompress what we have so far
            match zstd::decode_all(&compressed_buffer[..]) {
                Ok(decompressed) => {
                    // Check if we got the expected size
                    if decompressed.len() == decompressed_size {
                        eprintln!("ZSTD: Decompressed {} bytes successfully", decompressed.len());
                        eprintln!("ZSTD: Data: {:02x?}", &decompressed[..decompressed.len().min(20)]);
                        self.decompressed = decompressed;
                        self.position = 0;
                        return Ok(true);
                    } else if decompressed.len() > decompressed_size {
                        // We read too much - this shouldn't happen with a valid frame
                        return Err(HailError::InvalidFormat(format!(
                            "Decompressed size mismatch: expected {}, got {}",
                            decompressed_size,
                            decompressed.len()
                        )));
                    }
                    // If decompressed.len() < decompressed_size, we need more data
                    // (though this is unusual for Zstd frames)
                }
                Err(_) => {
                    // Decompression failed - we likely need more data
                    // Read one more byte and try again
                    let mut one_byte = [0u8; 1];
                    match self.inner.read_exact(&mut one_byte) {
                        Ok(()) => compressed_buffer.push(one_byte[0]),
                        Err(HailError::UnexpectedEof) => {
                            return Err(HailError::InvalidFormat(
                                "Incomplete Zstd frame".to_string()
                            ));
                        }
                        Err(e) => return Err(e),
                    }
                }
            }

            // Safety check: prevent infinite loop
            if compressed_buffer.len() > decompressed_size * 2 + 1024 {
                return Err(HailError::InvalidFormat(
                    "Zstd frame too large".to_string()
                ));
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_zstd_decompression() {
        // Create a simple test: compress "Hello" and wrap it in the expected format
        let original = b"Hello";
        let compressed = zstd::encode_all(&original[..], 3).unwrap();

        // Build the format: [decompressed_size][compressed_data]
        let mut data = Vec::new();
        data.extend_from_slice(&(original.len() as i32).to_le_bytes());
        data.extend_from_slice(&compressed);

        // Wrap in a stream block: [block_length][block_data]
        let mut stream_data = Vec::new();
        stream_data.extend_from_slice(&(data.len() as u32).to_le_bytes());
        stream_data.extend_from_slice(&data);

        // Test reading through the buffer stack
        let cursor = Cursor::new(stream_data);
        let stream_buffer = crate::buffer::StreamBlockBuffer::new(cursor);
        let mut zstd_buffer = ZstdBuffer::new(stream_buffer);

        let mut result = vec![0u8; original.len()];
        zstd_buffer.read_exact(&mut result).unwrap();

        assert_eq!(&result[..], &original[..]);
    }

    #[test]
    fn test_invalid_magic() {
        // Create data with invalid magic number
        let mut data = Vec::new();
        data.extend_from_slice(&5i32.to_le_bytes()); // decompressed size
        data.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // bad magic

        let mut stream_data = Vec::new();
        stream_data.extend_from_slice(&(data.len() as u32).to_le_bytes());
        stream_data.extend_from_slice(&data);

        let cursor = Cursor::new(stream_data);
        let stream_buffer = crate::buffer::StreamBlockBuffer::new(cursor);
        let mut zstd_buffer = ZstdBuffer::new(stream_buffer);

        let mut result = [0u8; 5];
        let err = zstd_buffer.read_exact(&mut result).unwrap_err();

        match err {
            HailError::InvalidFormat(msg) => {
                assert!(msg.contains("Invalid Zstd magic"));
            }
            _ => panic!("Expected InvalidFormat error"),
        }
    }

    #[test]
    fn test_multiple_blocks() {
        // Test reading across multiple Zstd blocks
        let data1 = b"First";
        let data2 = b"Second";

        let compressed1 = zstd::encode_all(&data1[..], 3).unwrap();
        let compressed2 = zstd::encode_all(&data2[..], 3).unwrap();

        let mut stream_data = Vec::new();

        // First block
        let mut block1 = Vec::new();
        block1.extend_from_slice(&(data1.len() as i32).to_le_bytes());
        block1.extend_from_slice(&compressed1);
        stream_data.extend_from_slice(&(block1.len() as u32).to_le_bytes());
        stream_data.extend_from_slice(&block1);

        // Second block
        let mut block2 = Vec::new();
        block2.extend_from_slice(&(data2.len() as i32).to_le_bytes());
        block2.extend_from_slice(&compressed2);
        stream_data.extend_from_slice(&(block2.len() as u32).to_le_bytes());
        stream_data.extend_from_slice(&block2);

        let cursor = Cursor::new(stream_data);
        let stream_buffer = crate::buffer::StreamBlockBuffer::new(cursor);
        let mut zstd_buffer = ZstdBuffer::new(stream_buffer);

        let mut result = vec![0u8; data1.len() + data2.len()];
        zstd_buffer.read_exact(&mut result).unwrap();

        assert_eq!(&result[..data1.len()], &data1[..]);
        assert_eq!(&result[data1.len()..], &data2[..]);
    }
}
