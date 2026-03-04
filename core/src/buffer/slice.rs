//! Slice buffer implementation for reading from byte slices
//!
//! Provides an `InputBuffer` implementation for reading directly from
//! an in-memory byte slice, useful for random access to decompressed blocks.

use crate::buffer::InputBuffer;
use crate::error::{HailError, Result};

/// A buffer that reads from a byte slice
///
/// This is used for reading data from a decompressed block that's already
/// in memory, enabling efficient random access to specific rows.
pub struct SliceBuffer<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> SliceBuffer<'a> {
    /// Create a new SliceBuffer from a byte slice
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    /// Get the remaining bytes that haven't been read yet
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.position)
    }

    /// Get the current position in the buffer
    pub fn position(&self) -> usize {
        self.position
    }
}

impl<'a> InputBuffer for SliceBuffer<'a> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        if self.position + buf.len() > self.data.len() {
            return Err(HailError::UnexpectedEof);
        }

        buf.copy_from_slice(&self.data[self.position..self.position + buf.len()]);
        self.position += buf.len();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_buffer_read() {
        let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut buffer = SliceBuffer::new(&data);

        let mut result = [0u8; 4];
        buffer.read_exact(&mut result).unwrap();
        assert_eq!(result, [1, 2, 3, 4]);

        buffer.read_exact(&mut result).unwrap();
        assert_eq!(result, [5, 6, 7, 8]);
    }

    #[test]
    fn test_slice_buffer_eof() {
        let data = [1, 2, 3];
        let mut buffer = SliceBuffer::new(&data);

        let mut result = [0u8; 5];
        let err = buffer.read_exact(&mut result);
        assert!(err.is_err());
    }

    #[test]
    fn test_slice_buffer_single_byte() {
        let data = [42, 99, 7];
        let mut buffer = SliceBuffer::new(&data);

        assert_eq!(buffer.read_u8().unwrap(), 42);
        assert_eq!(buffer.read_u8().unwrap(), 99);
        assert_eq!(buffer.read_u8().unwrap(), 7);
        assert!(buffer.read_u8().is_err());
    }

    #[test]
    fn test_slice_buffer_remaining() {
        let data = [1, 2, 3, 4, 5];
        let mut buffer = SliceBuffer::new(&data);

        assert_eq!(buffer.remaining(), 5);
        buffer.read_u8().unwrap();
        assert_eq!(buffer.remaining(), 4);
    }
}
