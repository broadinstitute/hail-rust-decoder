//! Output buffer implementations for writing Hail data
//!
//! This module implements the write-side of Hail's 4-layer buffer stack:
//! 1. LEB128OutputBuffer - encodes variable-length integers
//! 2. BlockingOutputBuffer - provides fixed-size buffering
//! 3. ZstdOutputBuffer - compresses chunks using Zstd
//! 4. StreamBlockOutputBuffer - writes length-prefixed blocks

use crate::error::Result;
use std::io::Write;

/// Trait for all output buffers
pub trait OutputBuffer {
    /// Write a single byte
    fn write_u8(&mut self, v: u8) -> Result<()>;

    /// Write a boolean
    fn write_bool(&mut self, v: bool) -> Result<()> {
        self.write_u8(if v { 1 } else { 0 })
    }

    /// Write a 32-bit integer (little-endian)
    fn write_i32(&mut self, v: i32) -> Result<()> {
        self.write_bytes(&v.to_le_bytes())
    }

    /// Write a 64-bit integer (little-endian)
    fn write_i64(&mut self, v: i64) -> Result<()> {
        self.write_bytes(&v.to_le_bytes())
    }

    /// Write a 32-bit float (little-endian)
    fn write_f32(&mut self, v: f32) -> Result<()> {
        self.write_bytes(&v.to_le_bytes())
    }

    /// Write a 64-bit float (little-endian)
    fn write_f64(&mut self, v: f64) -> Result<()> {
        self.write_bytes(&v.to_le_bytes())
    }

    /// Write raw bytes
    fn write_bytes(&mut self, v: &[u8]) -> Result<()>;

    /// Flush any buffered data
    fn flush(&mut self) -> Result<()>;
}

/// Stream block output buffer - writes length-prefixed blocks
pub struct StreamBlockOutputBuffer<W: Write> {
    writer: W,
}

impl<W: Write> StreamBlockOutputBuffer<W> {
    /// Create a new stream block output buffer
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Write a complete block with length prefix
    pub fn write_block(&mut self, data: &[u8]) -> Result<()> {
        // Write 4-byte length prefix (little-endian)
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        // Write block data
        self.writer.write_all(data)?;
        Ok(())
    }

    /// Get the inner writer
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> OutputBuffer for StreamBlockOutputBuffer<W> {
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.writer.write_all(&[v])?;
        Ok(())
    }

    fn write_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.writer.write_all(v)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

/// Zstd compression output buffer
pub struct ZstdOutputBuffer<'a> {
    encoder: zstd::stream::Encoder<'a, Vec<u8>>,
}

impl<'a> ZstdOutputBuffer<'a> {
    /// Create a new Zstd output buffer
    pub fn new(compression_level: i32) -> Result<Self> {
        let encoder = zstd::stream::Encoder::new(Vec::new(), compression_level)?;
        Ok(Self { encoder })
    }

    /// Finish compression and return compressed data
    pub fn finish(self) -> Result<Vec<u8>> {
        let data = self.encoder.finish()?;
        Ok(data)
    }
}

impl<'a> OutputBuffer for ZstdOutputBuffer<'a> {
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.encoder.write_all(&[v])?;
        Ok(())
    }

    fn write_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.encoder.write_all(v)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.encoder.flush()?;
        Ok(())
    }
}

/// Blocking output buffer - buffers data into fixed size chunks
pub struct BlockingOutputBuffer {
    buffer: Vec<u8>,
    block_size: usize,
    blocks: Vec<Vec<u8>>,
}

impl BlockingOutputBuffer {
    /// Create a new blocking output buffer with given block size
    pub fn new(block_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(block_size),
            block_size,
            blocks: Vec::new(),
        }
    }

    /// Create a new blocking output buffer with default size (64KB)
    pub fn with_default_size() -> Self {
        Self::new(64 * 1024)
    }

    /// Get all blocks including the current buffer
    pub fn into_blocks(mut self) -> Vec<Vec<u8>> {
        if !self.buffer.is_empty() {
            self.blocks.push(std::mem::take(&mut self.buffer));
        }
        self.blocks
    }

    /// Get all data as a single vector
    pub fn into_data(self) -> Vec<u8> {
        let blocks = self.into_blocks();
        let total_len: usize = blocks.iter().map(|b| b.len()).sum();
        let mut data = Vec::with_capacity(total_len);
        for block in blocks {
            data.extend(block);
        }
        data
    }
}

impl OutputBuffer for BlockingOutputBuffer {
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.buffer.push(v);
        if self.buffer.len() >= self.block_size {
            self.blocks.push(std::mem::take(&mut self.buffer));
            self.buffer = Vec::with_capacity(self.block_size);
        }
        Ok(())
    }

    fn write_bytes(&mut self, v: &[u8]) -> Result<()> {
        for byte in v {
            self.write_u8(*byte)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.blocks.push(std::mem::take(&mut self.buffer));
            self.buffer = Vec::with_capacity(self.block_size);
        }
        Ok(())
    }
}

/// LEB128 output buffer - encodes integers using LEB128
pub struct LEB128OutputBuffer<B: OutputBuffer> {
    inner: B,
}

impl<B: OutputBuffer> LEB128OutputBuffer<B> {
    /// Create a new LEB128 output buffer
    pub fn new(inner: B) -> Self {
        Self { inner }
    }

    /// Write an unsigned LEB128 integer
    pub fn write_uleb128(&mut self, mut value: u64) -> Result<()> {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80; // More bytes to come
            }
            self.inner.write_u8(byte)?;
            if value == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Write a signed LEB128 integer
    pub fn write_sleb128(&mut self, mut value: i64) -> Result<()> {
        let mut more = true;
        while more {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;

            // Check if we need more bytes
            let sign_bit = (byte & 0x40) != 0;
            if (value == 0 && !sign_bit) || (value == -1 && sign_bit) {
                more = false;
            } else {
                byte |= 0x80;
            }
            self.inner.write_u8(byte)?;
        }
        Ok(())
    }

    /// Get the inner buffer
    pub fn into_inner(self) -> B {
        self.inner
    }

    /// Get mutable access to inner buffer
    pub fn inner_mut(&mut self) -> &mut B {
        &mut self.inner
    }
}

impl<B: OutputBuffer> OutputBuffer for LEB128OutputBuffer<B> {
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.inner.write_u8(v)
    }

    fn write_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.inner.write_bytes(v)
    }

    // Override integer writes to use LEB128 encoding (ULEB128 as per Hail format)
    fn write_i32(&mut self, v: i32) -> Result<()> {
        // Hail uses ULEB128 for Int32 - cast to u64 preserving bit pattern
        self.write_uleb128(v as u32 as u64)
    }

    fn write_i64(&mut self, v: i64) -> Result<()> {
        // Hail uses ULEB128 for Int64 - cast to u64 preserving bit pattern
        self.write_uleb128(v as u64)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}

/// Builder for constructing output buffer stacks
pub struct OutputBufferBuilder;

impl OutputBufferBuilder {
    /// Build a complete output buffer stack matching Hail's LEB128BufferSpec
    /// Returns a buffer that writes to a Vec<u8>
    pub fn build_leb128_stack() -> LEB128OutputBuffer<BlockingOutputBuffer> {
        let blocking = BlockingOutputBuffer::with_default_size();
        LEB128OutputBuffer::new(blocking)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uleb128_single_byte() {
        let blocking = BlockingOutputBuffer::with_default_size();
        let mut buffer = LEB128OutputBuffer::new(blocking);

        buffer.write_uleb128(127).unwrap();

        let data = buffer.into_inner().into_data();
        assert_eq!(data, vec![0x7F]);
    }

    #[test]
    fn test_uleb128_multi_byte() {
        let blocking = BlockingOutputBuffer::with_default_size();
        let mut buffer = LEB128OutputBuffer::new(blocking);

        buffer.write_uleb128(128).unwrap();

        let data = buffer.into_inner().into_data();
        assert_eq!(data, vec![0x80, 0x01]);
    }

    #[test]
    fn test_uleb128_large_value() {
        let blocking = BlockingOutputBuffer::with_default_size();
        let mut buffer = LEB128OutputBuffer::new(blocking);

        buffer.write_uleb128(300).unwrap();

        let data = buffer.into_inner().into_data();
        assert_eq!(data, vec![0xAC, 0x02]);
    }

    #[test]
    fn test_blocking_buffer() {
        let mut buffer = BlockingOutputBuffer::new(4);

        buffer.write_bytes(&[1, 2, 3, 4, 5, 6]).unwrap();

        let blocks = buffer.into_blocks();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0], vec![1, 2, 3, 4]);
        assert_eq!(blocks[1], vec![5, 6]);
    }
}
