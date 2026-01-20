//! Blocking buffer implementation
//!
//! Provides fixed-size buffering on top of another buffer

use crate::error::Result;
use crate::buffer::InputBuffer;

/// Fixed-size blocking buffer (typically 64KB)
pub struct BlockingBuffer<B: InputBuffer> {
    inner: B,
    buffer: Vec<u8>,
    position: usize,
    filled: usize,
}

impl<B: InputBuffer> BlockingBuffer<B> {
    /// Create a new blocking buffer with the given block size
    pub fn new(inner: B, block_size: usize) -> Self {
        Self {
            inner,
            buffer: vec![0; block_size],
            position: 0,
            filled: 0,
        }
    }

    /// Create with default 64KB block size
    pub fn with_default_size(inner: B) -> Self {
        Self::new(inner, 64 * 1024)
    }

    fn refill(&mut self) -> Result<()> {
        self.inner.read_exact(&mut self.buffer)?;
        self.position = 0;
        self.filled = self.buffer.len();
        Ok(())
    }

    fn available(&self) -> usize {
        self.filled.saturating_sub(self.position)
    }
}

impl<B: InputBuffer> InputBuffer for BlockingBuffer<B> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut offset = 0;

        while offset < buf.len() {
            if self.available() == 0 {
                self.refill()?;
            }

            let to_copy = buf.len() - offset;
            let available = self.available();
            let copy_len = to_copy.min(available);

            buf[offset..offset + copy_len]
                .copy_from_slice(&self.buffer[self.position..self.position + copy_len]);

            self.position += copy_len;
            offset += copy_len;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::stream_block::StreamBlockBuffer;

    #[test]
    fn test_blocking_buffer() {
        let data = vec![
            10, 0, 0, 0, // block length = 10
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        ];

        let stream = StreamBlockBuffer::new(&data[..]);
        let mut buffer = BlockingBuffer::new(stream, 4); // Small block for testing

        let mut result = [0u8; 10];
        buffer.read_exact(&mut result).unwrap();

        assert_eq!(result, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }
}
