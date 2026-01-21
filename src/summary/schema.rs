//! Schema formatting for Hail table summaries
//!
//! This module provides utilities to pretty-print Hail schema strings
//! with proper indentation for nested structures.

/// Format a Hail vType schema string with proper indentation
///
/// Takes the raw vType string (e.g., `+struct{locus:+Locus(GRCh38),alleles:+Array[str],...}`)
/// and returns a nicely formatted version with newlines and indentation.
///
/// # Arguments
/// * `v_type` - The raw vType string from the codec spec
///
/// # Returns
/// A formatted string with proper indentation
pub fn format_schema(v_type: &str) -> String {
    let mut result = String::new();
    let mut indent = 0;
    let mut chars = v_type.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '{' | '[' | '<' => {
                result.push(c);
                result.push('\n');
                indent += 2;
                result.push_str(&" ".repeat(indent));
            }
            '}' | ']' | '>' => {
                result.push('\n');
                indent = indent.saturating_sub(2);
                result.push_str(&" ".repeat(indent));
                result.push(c);
            }
            ',' => {
                result.push(c);
                result.push('\n');
                result.push_str(&" ".repeat(indent));
            }
            _ => result.push(c),
        }
    }

    result
}

/// Format a Hail schema into a more human-readable form
///
/// This provides a cleaner display that strips internal markers
/// like '+' for required fields.
///
/// # Arguments
/// * `v_type` - The raw vType string
///
/// # Returns
/// A cleaned-up, indented schema string
pub fn format_schema_clean(v_type: &str) -> String {
    // First clean up the vType string
    let cleaned = v_type
        .replace("+struct", "struct")
        .replace("+Struct", "Struct")
        .replace("+Array", "Array")
        .replace("+Set", "Set")
        .replace("+Dict", "Dict")
        .replace("+Locus", "Locus")
        .replace("+Interval", "Interval")
        .replace("+Call", "Call")
        .replace("+int32", "int32")
        .replace("+int64", "int64")
        .replace("+float32", "float32")
        .replace("+float64", "float64")
        .replace("+str", "str")
        .replace("+bool", "bool");

    format_schema(&cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_simple_schema() {
        let input = "struct{a:int32,b:str}";
        let result = format_schema(input);
        assert!(result.contains("struct"));
        assert!(result.contains("a:int32"));
        assert!(result.contains("b:str"));
    }

    #[test]
    fn test_format_nested_schema() {
        let input = "struct{a:struct{b:int32}}";
        let result = format_schema(input);
        assert!(result.contains("struct"));
        // Should have nested indentation
        let lines: Vec<&str> = result.lines().collect();
        assert!(lines.len() > 1);
    }
}
