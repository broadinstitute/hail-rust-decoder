//! Comparison utilities for EncodedValues.
//!
//! This module centralizes the logic for comparing `EncodedValue`s, enabling
//! merge joins and sorted iteration across multiple tables.

use crate::codec::EncodedValue;
use std::cmp::Ordering;

/// Compare two encoded values.
///
/// This handles primitive types and recurses into Structs if necessary.
/// Returns None if types are incompatible.
pub fn compare_encoded_values(a: &EncodedValue, b: &EncodedValue) -> Option<Ordering> {
    match (a, b) {
        (EncodedValue::Binary(a), EncodedValue::Binary(b)) => {
            // Treat binary as strings for comparison (utf-8 aware)
            let a_str = String::from_utf8_lossy(a);
            let b_str = String::from_utf8_lossy(b);
            Some(a_str.cmp(&b_str))
        }
        (EncodedValue::Int32(a), EncodedValue::Int32(b)) => Some(a.cmp(b)),
        (EncodedValue::Int64(a), EncodedValue::Int64(b)) => Some(a.cmp(b)),
        (EncodedValue::Float32(a), EncodedValue::Float32(b)) => a.partial_cmp(b),
        (EncodedValue::Float64(a), EncodedValue::Float64(b)) => a.partial_cmp(b),
        (EncodedValue::Boolean(a), EncodedValue::Boolean(b)) => Some(a.cmp(b)),
        // Handle cross-type integer comparisons (common in Hail)
        (EncodedValue::Int32(a), EncodedValue::Int64(b)) => (*a as i64).partial_cmp(b),
        (EncodedValue::Int64(a), EncodedValue::Int32(b)) => a.partial_cmp(&(*b as i64)),

        // Array comparison (lexicographical by elements)
        (EncodedValue::Array(a_elems), EncodedValue::Array(b_elems)) => {
            for (i, val_a) in a_elems.iter().enumerate() {
                if i >= b_elems.len() {
                    return Some(Ordering::Greater); // a is longer
                }
                let val_b = &b_elems[i];
                match compare_encoded_values(val_a, val_b) {
                    Some(Ordering::Equal) => continue,
                    other => return other,
                }
            }
            if a_elems.len() < b_elems.len() {
                Some(Ordering::Less) // a is shorter
            } else {
                Some(Ordering::Equal)
            }
        }

        // Struct comparison (lexicographical by fields)
        (EncodedValue::Struct(a_fields), EncodedValue::Struct(b_fields)) => {
            // For structs, we usually assume fields are in the same order if schemas match.
            // A more robust implementation would compare by field name, but for row keys
            // in Hail, relying on order is standard for the Key fields.
            for (i, (_, val_a)) in a_fields.iter().enumerate() {
                if i >= b_fields.len() {
                    return Some(Ordering::Greater); // a is longer
                }
                let (_, val_b) = &b_fields[i];
                match compare_encoded_values(val_a, val_b) {
                    Some(Ordering::Equal) => continue,
                    other => return other,
                }
            }
            if a_fields.len() < b_fields.len() {
                Some(Ordering::Less) // a is shorter
            } else {
                Some(Ordering::Equal)
            }
        }
        _ => None,
    }
}

/// Extract a field by name from a struct
fn get_field<'a>(row: &'a EncodedValue, name: &str) -> Option<&'a EncodedValue> {
    if let EncodedValue::Struct(fields) = row {
        fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    } else {
        None
    }
}

/// Extract key fields from a row and compare them.
pub fn compare_rows(row_a: &EncodedValue, row_b: &EncodedValue, key_fields: &[String]) -> Ordering {
    for key in key_fields {
        let val_a = get_field(row_a, key);
        let val_b = get_field(row_b, key);

        match (val_a, val_b) {
            (Some(a), Some(b)) => {
                match compare_encoded_values(a, b) {
                    Some(Ordering::Equal) => continue,
                    Some(ord) => return ord,
                    None => return Ordering::Equal, // Should probably error, but fallback to equal
                }
            }
            (Some(_), None) => return Ordering::Greater, // Present > Missing
            (None, Some(_)) => return Ordering::Less,
            (None, None) => continue,
        }
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_int32() {
        assert_eq!(
            compare_encoded_values(&EncodedValue::Int32(5), &EncodedValue::Int32(10)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_encoded_values(&EncodedValue::Int32(10), &EncodedValue::Int32(5)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            compare_encoded_values(&EncodedValue::Int32(5), &EncodedValue::Int32(5)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn test_compare_binary() {
        assert_eq!(
            compare_encoded_values(
                &EncodedValue::Binary(b"abc".to_vec()),
                &EncodedValue::Binary(b"def".to_vec())
            ),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn test_compare_rows() {
        let row_a = EncodedValue::Struct(vec![
            ("chrom".to_string(), EncodedValue::Binary(b"chr1".to_vec())),
            ("pos".to_string(), EncodedValue::Int32(100)),
        ]);
        let row_b = EncodedValue::Struct(vec![
            ("chrom".to_string(), EncodedValue::Binary(b"chr1".to_vec())),
            ("pos".to_string(), EncodedValue::Int32(200)),
        ]);

        let keys = vec!["chrom".to_string(), "pos".to_string()];
        assert_eq!(compare_rows(&row_a, &row_b, &keys), Ordering::Less);
    }
}
