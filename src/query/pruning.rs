//! Partition pruning logic for Hail tables

use crate::metadata::Interval;
use crate::query::types::{KeyRange, KeyValue, QueryBound};
use serde_json::Value;

/// Extract a KeyValue from a JSON object by field name
fn extract_key_value(json: &Value, field: &str) -> Option<KeyValue> {
    json.get(field).and_then(KeyValue::from_json)
}

/// Check if a query range overlaps with a partition interval for a specific field
fn range_overlaps(
    interval: &Interval,
    field: &str,
    query_start: &QueryBound,
    query_end: &QueryBound,
) -> bool {
    // Extract the field values from the interval start and end
    let interval_start = match extract_key_value(&interval.start, field) {
        Some(v) => v,
        None => return true, // If we can't extract, assume overlap (conservative)
    };

    let interval_end = match extract_key_value(&interval.end, field) {
        Some(v) => v,
        None => return true, // If we can't extract, assume overlap (conservative)
    };

    // Check if ranges overlap
    // Interval: [interval_start, interval_end]
    // Query: [query_start, query_end]
    // Overlaps if: interval_start <= query_end AND interval_end >= query_start

    // Check if query_end < interval_start (no overlap)
    match query_end {
        QueryBound::Included(qe) => {
            if qe < &interval_start {
                return false;
            }
        }
        QueryBound::Excluded(qe) => {
            if qe <= &interval_start {
                return false;
            }
        }
        QueryBound::Unbounded => {}
    }

    // Check if query_start > interval_end (no overlap)
    match query_start {
        QueryBound::Included(qs) => {
            if qs > &interval_end {
                return false;
            }
        }
        QueryBound::Excluded(qs) => {
            if qs >= &interval_end {
                return false;
            }
        }
        QueryBound::Unbounded => {}
    }

    // If we haven't returned false, there's overlap
    true
}

/// Filter partitions based on key ranges
///
/// Returns a vector of partition indices that may contain rows matching the query.
/// This is used for partition pruning - we can skip reading partitions that don't
/// overlap with the query range.
pub fn filter_partitions(range_bounds: &[Interval], ranges: &[KeyRange]) -> Vec<usize> {
    range_bounds
        .iter()
        .enumerate()
        .filter(|(_, interval)| {
            // A partition matches if ALL query ranges overlap with it
            ranges.iter().all(|range| {
                range_overlaps(interval, &range.field, &range.start, &range.end)
            })
        })
        .map(|(idx, _)| idx)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_interval(
        gene_id: &str,
        chrom: &str,
        start: i32,
        gene_id_end: &str,
        chrom_end: &str,
        end: i32,
    ) -> Interval {
        Interval {
            start: json!({
                "gene_id": gene_id,
                "chrom": chrom,
                "start": start,
            }),
            end: json!({
                "gene_id": gene_id_end,
                "chrom": chrom_end,
                "start": end,
            }),
            include_start: true,
            include_end: true,
        }
    }

    #[test]
    fn test_filter_partitions_single_match() {
        let intervals = vec![
            create_test_interval("ENSG00000066468", "10", 121478332, "ENSG00000185960", "X", 624344),
        ];

        // Query for chromosome 10
        let ranges = vec![KeyRange::point(
            "chrom".to_string(),
            KeyValue::String("10".to_string()),
        )];

        let result = filter_partitions(&intervals, &ranges);
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_filter_partitions_no_match() {
        let intervals = vec![
            create_test_interval("ENSG00000066468", "10", 121478332, "ENSG00000185960", "X", 624344),
        ];

        // Query for chromosome 1 (not in the data)
        let ranges = vec![KeyRange::point(
            "chrom".to_string(),
            KeyValue::String("1".to_string()),
        )];

        let result = filter_partitions(&intervals, &ranges);
        assert_eq!(result, Vec::<usize>::new());
    }

    #[test]
    fn test_filter_partitions_range_query() {
        let intervals = vec![
            create_test_interval("ENSG00000066468", "10", 121478332, "ENSG00000185960", "X", 624344),
        ];

        // Query for chromosome range (includes both 10 and X)
        let ranges = vec![
            KeyRange::inclusive(
                "chrom".to_string(),
                KeyValue::String("1".to_string()),
                KeyValue::String("Z".to_string()),
            ),
        ];

        let result = filter_partitions(&intervals, &ranges);
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_filter_partitions_multiple_intervals() {
        let intervals = vec![
            create_test_interval("ENSG00000000001", "1", 1000000, "ENSG00000000100", "1", 2000000),
            create_test_interval("ENSG00000000101", "2", 3000000, "ENSG00000000200", "2", 4000000),
            create_test_interval("ENSG00000000201", "3", 5000000, "ENSG00000000300", "3", 6000000),
        ];

        // Query for chromosome 2
        let ranges = vec![KeyRange::point(
            "chrom".to_string(),
            KeyValue::String("2".to_string()),
        )];

        let result = filter_partitions(&intervals, &ranges);
        assert_eq!(result, vec![1]);
    }
}
