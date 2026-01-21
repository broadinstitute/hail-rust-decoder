//! Streaming query support for Hail tables
//!
//! This module provides streaming iterators for lazy row reading,
//! enabling memory-efficient queries on large tables.

use crate::buffer::InputBuffer;
use crate::codec::{EncodedType, EncodedValue};
use crate::query::{KeyRange, KeyValue, QueryBound};
use crate::HailError;
use crate::Result;

/// Iterator that streams rows from a single partition
///
/// This struct enables lazy iteration over rows in a partition file,
/// reading and filtering rows one at a time instead of loading all into memory.
pub struct PartitionStream {
    buffer: Box<dyn InputBuffer>,
    row_type: EncodedType,
    ranges: Vec<KeyRange>,
}

impl PartitionStream {
    /// Create a new partition stream
    ///
    /// # Arguments
    /// * `buffer` - The initialized buffer for reading partition data
    /// * `row_type` - The row schema for decoding
    /// * `ranges` - Key range filters to apply
    pub fn new(buffer: Box<dyn InputBuffer>, row_type: EncodedType, ranges: Vec<KeyRange>) -> Self {
        Self {
            buffer,
            row_type,
            ranges,
        }
    }
}

impl Iterator for PartitionStream {
    type Item = Result<EncodedValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Read row present flag
            let row_present = match self.buffer.read_bool() {
                Ok(p) => p,
                Err(HailError::UnexpectedEof) => return None,
                Err(e) => return Some(Err(e)),
            };

            if !row_present {
                // Skip null rows
                continue;
            }

            // Decode the row
            let row = match self.row_type.read_present_value(&mut *self.buffer) {
                Ok(r) => r,
                Err(HailError::UnexpectedEof) => return None,
                Err(e) => return Some(Err(e)),
            };

            // Apply filters
            if row_matches_ranges(&row, &self.ranges) {
                return Some(Ok(row));
            }
            // If no match, continue to next row
        }
    }
}

/// Check if a row matches all the key range filters
pub fn row_matches_ranges(row: &EncodedValue, ranges: &[KeyRange]) -> bool {
    for range in ranges {
        // Extract the value using the field path (supports nested access)
        let field_value = extract_field_by_path(row, &range.field_path);

        if let Some(value) = field_value {
            if let Some(key_value) = encoded_to_key_value(value) {
                if !value_in_range(&key_value, range) {
                    return false;
                }
            }
        }
    }
    true
}

/// Extract a field from an EncodedValue by following a field path
fn extract_field_by_path<'a>(value: &'a EncodedValue, path: &[String]) -> Option<&'a EncodedValue> {
    if path.is_empty() {
        return Some(value);
    }

    let mut current = value;
    for field_name in path {
        match current {
            EncodedValue::Struct(fields) => {
                current = fields
                    .iter()
                    .find(|(name, _)| name == field_name)
                    .map(|(_, v)| v)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Convert an EncodedValue to a KeyValue for comparison
fn encoded_to_key_value(value: &EncodedValue) -> Option<KeyValue> {
    match value {
        EncodedValue::Binary(b) => {
            Some(KeyValue::String(String::from_utf8_lossy(b).into_owned()))
        }
        EncodedValue::Int32(i) => Some(KeyValue::Int32(*i)),
        EncodedValue::Int64(i) => Some(KeyValue::Int64(*i)),
        EncodedValue::Float32(f) => Some(KeyValue::Float32(*f)),
        EncodedValue::Float64(f) => Some(KeyValue::Float64(*f)),
        EncodedValue::Boolean(b) => Some(KeyValue::Boolean(*b)),
        _ => None,
    }
}

/// Check if a value is within a key range
fn value_in_range(value: &KeyValue, range: &KeyRange) -> bool {
    // Check start bound
    match &range.start {
        QueryBound::Included(start) => {
            if value < start {
                return false;
            }
        }
        QueryBound::Excluded(start) => {
            if value <= start {
                return false;
            }
        }
        QueryBound::Unbounded => {}
    }

    // Check end bound
    match &range.end {
        QueryBound::Included(end) => {
            if value > end {
                return false;
            }
        }
        QueryBound::Excluded(end) => {
            if value >= end {
                return false;
            }
        }
        QueryBound::Unbounded => {}
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_matches_ranges_empty() {
        let row = EncodedValue::Struct(vec![
            ("field1".to_string(), EncodedValue::Int32(42)),
        ]);
        assert!(row_matches_ranges(&row, &[]));
    }

    #[test]
    fn test_row_matches_ranges_point() {
        let row = EncodedValue::Struct(vec![
            ("field1".to_string(), EncodedValue::Int32(42)),
        ]);

        let range = KeyRange::point("field1".to_string(), KeyValue::Int32(42));
        assert!(row_matches_ranges(&row, &[range]));

        let range2 = KeyRange::point("field1".to_string(), KeyValue::Int32(99));
        assert!(!row_matches_ranges(&row, &[range2]));
    }

    #[test]
    fn test_row_matches_ranges_nested() {
        let row = EncodedValue::Struct(vec![
            ("outer".to_string(), EncodedValue::Struct(vec![
                ("inner".to_string(), EncodedValue::Binary(b"test".to_vec())),
            ])),
        ]);

        let range = KeyRange::point_nested(
            vec!["outer".to_string(), "inner".to_string()],
            KeyValue::String("test".to_string()),
        );
        assert!(row_matches_ranges(&row, &[range]));
    }
}
