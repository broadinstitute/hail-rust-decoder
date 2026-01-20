//! Stream block buffer implementation
//!
//! Reads data formatted as: [4-byte length][data block]...

use crate::error::{HailError, Result};
use crate::buffer::InputBuffer;
use std::io::Read;

/// Stream block buffer that reads length-prefixed blocks
pub struct StreamBlockBuffer<R: Read> {
    reader: R,
    current_block: Vec<u8>,
    position: usize,
}

impl<R: Read> StreamBlockBuffer<R> {
    /// Create a new stream block buffer
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            current_block: Vec::new(),
            position: 0,
        }
    }

    /// Read the next block from the stream
    fn read_next_block(&mut self) -> Result<bool> {
        // Read 4-byte block length (little-endian)
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(false); // End of stream
            }
            Err(e) => return Err(e.into()),
        }

        let block_len = u32::from_le_bytes(len_buf) as usize;

        // Read the block data
        self.current_block.resize(block_len, 0);
        self.reader.read_exact(&mut self.current_block)?;
        self.position = 0;

        Ok(true)
    }

    /// Get available bytes in current block
    fn available(&self) -> usize {
        self.current_block.len().saturating_sub(self.position)
    }
}

impl<R: Read> InputBuffer for StreamBlockBuffer<R> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut offset = 0;

        while offset < buf.len() {
            // If we need more data, read the next block
            if self.available() == 0 {
                if !self.read_next_block()? {
                    return Err(HailError::UnexpectedEof);
                }
            }

            // Copy from current block
            let to_copy = buf.len() - offset;
            let available = self.available();
            let copy_len = to_copy.min(available);

            buf[offset..offset + copy_len]
                .copy_from_slice(&self.current_block[self.position..self.position + copy_len]);

            self.position += copy_len;
            offset += copy_len;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_single_block() {
        // Block format: [length: 4][data: 5]
        let data = vec![
            5, 0, 0, 0, // length = 5
            1, 2, 3, 4, 5, // data
        ];

        let mut buffer = StreamBlockBuffer::new(&data[..]);
        let mut result = [0u8; 5];
        buffer.read_exact(&mut result).unwrap();

        assert_eq!(result, [1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_read_multiple_blocks() {
        let data = vec![
            3, 0, 0, 0, // length = 3
            1, 2, 3, // data
            2, 0, 0, 0, // length = 2
            4, 5, // data
        ];

        let mut buffer = StreamBlockBuffer::new(&data[..]);
        let mut result = [0u8; 5];
        buffer.read_exact(&mut result).unwrap();

        assert_eq!(result, [1, 2, 3, 4, 5]);
    }
}
