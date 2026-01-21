//! Encoded types (EType) for Hail's binary format
//!
//! Hail uses a different type system for encoding (EType) vs the logical type (HailType).
//! The encoding type determines the binary representation.

use crate::buffer::InputBuffer;
use crate::error::{HailError, Result};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};

/// Encoded type representation
#[derive(Debug, Clone, PartialEq)]
pub enum EncodedType {
    /// Binary/String with byte-length prefix (+EBinary)
    EBinary { required: bool },

    /// Struct with encoded fields (+EBaseStruct)
    EBaseStruct {
        required: bool,
        fields: Vec<EncodedField>,
    },

    /// 32-bit integer
    EInt32 { required: bool },

    /// 64-bit integer
    EInt64 { required: bool },

    /// 32-bit float
    EFloat32 { required: bool },

    /// 64-bit float
    EFloat64 { required: bool },

    /// Boolean
    EBoolean { required: bool },

    /// Array
    EArray {
        required: bool,
        element: Box<EncodedType>,
    },
}

/// Encoded struct field
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedField {
    pub name: String,
    pub encoded_type: EncodedType,
    pub index: usize,
}

impl EncodedType {
    /// Check if this type is required (no null flag)
    pub fn is_required(&self) -> bool {
        match self {
            EncodedType::EBinary { required } => *required,
            EncodedType::EBaseStruct { required, .. } => *required,
            EncodedType::EInt32 { required } => *required,
            EncodedType::EInt64 { required } => *required,
            EncodedType::EFloat32 { required } => *required,
            EncodedType::EFloat64 { required } => *required,
            EncodedType::EBoolean { required } => *required,
            EncodedType::EArray { required, .. } => *required,
        }
    }

    /// Read a standalone value (checks nullable flag if needed)
    /// Use this for top-level values or values outside of structs
    pub fn read(&self, buffer: &mut dyn InputBuffer) -> Result<EncodedValue> {
        // For standalone values, check nullable flag if needed
        if !self.is_required() {
            let is_present = buffer.read_bool()?;
            if !is_present {
                return Ok(EncodedValue::Null);
            }
        } else {
        }

        // Decode the present value
        self.read_present_value(buffer)
    }

    /// Read a value that's known to be present (for struct fields after bitmap check)
    /// This does NOT check for a nullable flag - assumes caller has checked the bitmap
    pub fn read_present_value(&self, buffer: &mut dyn InputBuffer) -> Result<EncodedValue> {
        match self {
            EncodedType::EBinary { .. } => {
                // Read i32 byte-length prefix (from EBinary.scala line 54: in.readInt())
                let length = buffer.read_i32()?;
                if length < 0 || length > 1_000_000 {
                    return Err(HailError::InvalidFormat(format!(
                        "Invalid binary length: {}",
                        length
                    )));
                }
                let length = length as usize;
                let mut bytes = vec![0u8; length];
                buffer.read_exact(&mut bytes)?;
                let preview = String::from_utf8_lossy(&bytes);
                if preview.len() < 50 {
                } else {
                }
                Ok(EncodedValue::Binary(bytes))
            }
            EncodedType::EBaseStruct { fields, .. } => {
                // Struct encoding format (from Hail's EBaseStruct.scala):
                // [missing bitmap][field data for present fields only]
                //
                // Missing bitmap:
                // - 1 bit per nullable field (1 = missing, 0 = present)
                // - Packed 8 fields per byte
                // - Only counts nullable fields (required fields don't get a bit)
                // - Number of bytes = ⌈n_nullable_fields / 8⌉

                // STEP 1: Count nullable fields and read bitmap
                let n_nullable = fields.iter().filter(|f| !f.encoded_type.is_required()).count();
                let n_missing_bytes = (n_nullable + 7) / 8;

                let mut missing_bitmap = vec![0u8; n_missing_bytes];
                buffer.read_exact(&mut missing_bitmap)?;

                // STEP 2: Decode each field
                let mut field_values = Vec::with_capacity(fields.len());
                let mut nullable_idx = 0; // Index in the bitmap (only increments for nullable fields)

                for (_field_idx, field) in fields.iter().enumerate() {
                    if field.encoded_type.is_required() {
                        // Required field - always present, read it
                        let value = field.encoded_type.read_present_value(buffer)?;
                        field_values.push((field.name.clone(), value));
                    } else {
                        // Nullable field - check bitmap
                        let byte_idx = nullable_idx / 8;
                        let bit_idx = nullable_idx % 8;
                        let bit = (missing_bitmap[byte_idx] >> bit_idx) & 1;
                        let is_missing = bit == 1; // 1 = missing, 0 = present

                        if is_missing {
                            field_values.push((field.name.clone(), EncodedValue::Null));
                        } else {
                            // Field is present - read it WITHOUT checking nullable flag
                            let value = field.encoded_type.read_present_value(buffer)?;
                            field_values.push((field.name.clone(), value));
                        }

                        nullable_idx += 1;
                    }
                }

                Ok(EncodedValue::Struct(field_values))
            }
            EncodedType::EInt32 { .. } => {
                let val = buffer.read_i32()?;
                Ok(EncodedValue::Int32(val))
            }
            EncodedType::EInt64 { .. } => {
                let val = buffer.read_i64()?;
                Ok(EncodedValue::Int64(val))
            }
            EncodedType::EFloat32 { .. } => Ok(EncodedValue::Float32(buffer.read_f32()?)),
            EncodedType::EFloat64 { .. } => Ok(EncodedValue::Float64(buffer.read_f64()?)),
            EncodedType::EBoolean { .. } => {
                let val = buffer.read_bool()?;
                Ok(EncodedValue::Boolean(val))
            }
            EncodedType::EArray { element, .. } => {
                // Array encoding format (from Hail's EArray.scala):
                // [i32 length][missing bitmap (if nullable elements)][elements (only present ones)]
                //
                // Missing bitmap:
                // - 1 bit per element (1 = missing, 0 = present)
                // - Packed 8 elements per byte
                // - Only included if elements are nullable
                // - Number of bytes = ⌈length / 8⌉

                // STEP 1: Read array length
                let length = buffer.read_i32()?;

                // Sanity check on length
                if length < 0 || length > 100000 {
                    return Err(HailError::InvalidFormat(format!(
                        "Invalid array length: {}",
                        length
                    )));
                }
                let length = length as usize;

                // STEP 2: Read missing bitmap if elements are nullable
                let missing_bitmap = if !element.is_required() {
                    let n_missing_bytes = (length + 7) / 8; // ⌈length / 8⌉
                    let mut bitmap = vec![0u8; n_missing_bytes];
                    buffer.read_exact(&mut bitmap)?;
                    Some(bitmap)
                } else {
                    None
                };

                // STEP 3: Read elements (only present ones if nullable)
                let mut elements = Vec::with_capacity(length);
                for i in 0..length {
                    // Check if element is present
                    let is_present = if let Some(ref bitmap) = missing_bitmap {
                        let byte_idx = i / 8;
                        let bit_idx = i % 8;
                        let bit = (bitmap[byte_idx] >> bit_idx) & 1;
                        bit == 0 // 0 = present, 1 = missing
                    } else {
                        true // All required elements are present
                    };

                    if is_present {
                        // Read the element WITHOUT checking nullable flag
                        // (the array's missing bitmap already determined presence)
                        elements.push(element.read_present_value(buffer)?);
                    } else {
                        // Element is missing
                        elements.push(EncodedValue::Null);
                    }
                }

                Ok(EncodedValue::Array(elements))
            }
        }
    }

    /// Parse from Hail's EType string format
    /// Example: "+EBaseStruct{field1:+EBinary,field2:+EInt32}"
    pub fn parse(s: &str) -> Result<Self> {
        let (required, rest) = if let Some(stripped) = s.strip_prefix('+') {
            (true, stripped)
        } else {
            (false, s)
        };

        if rest.starts_with("EBinary") {
            Ok(EncodedType::EBinary { required })
        } else if rest.starts_with("EInt32") {
            Ok(EncodedType::EInt32 { required })
        } else if rest.starts_with("EInt64") {
            Ok(EncodedType::EInt64 { required })
        } else if rest.starts_with("EFloat32") {
            Ok(EncodedType::EFloat32 { required })
        } else if rest.starts_with("EFloat64") {
            Ok(EncodedType::EFloat64 { required })
        } else if rest.starts_with("EBoolean") {
            Ok(EncodedType::EBoolean { required })
        } else if rest.starts_with("EBaseStruct{") {
            // Parse struct fields
            // This is a simplified parser - production would need full parser
            Ok(EncodedType::EBaseStruct {
                required,
                fields: vec![], // TODO: Implement full parser
            })
        } else {
            Err(HailError::InvalidFormat(format!(
                "Unknown encoded type: {}",
                s
            )))
        }
    }
}

/// Value decoded using encoded type
#[derive(Debug, Clone, PartialEq)]
pub enum EncodedValue {
    Null,
    Binary(Vec<u8>),
    Struct(Vec<(String, EncodedValue)>),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    Array(Vec<EncodedValue>),
}

impl EncodedValue {
    /// Convert binary to string
    pub fn as_string(&self) -> Option<String> {
        match self {
            EncodedValue::Binary(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        }
    }

    /// Convert to i32
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            EncodedValue::Int32(v) => Some(*v),
            _ => None,
        }
    }

    /// Convert to i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            EncodedValue::Int64(v) => Some(*v),
            _ => None,
        }
    }
}

impl Serialize for EncodedValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            EncodedValue::Null => serializer.serialize_none(),
            EncodedValue::Binary(bytes) => {
                // Convert to UTF-8 string (lossy) for JSON compatibility
                let s = String::from_utf8_lossy(bytes);
                serializer.serialize_str(&s)
            }
            EncodedValue::Struct(fields) => {
                let mut map = serializer.serialize_map(Some(fields.len()))?;
                for (name, value) in fields {
                    map.serialize_entry(name, value)?;
                }
                map.end()
            }
            EncodedValue::Int32(v) => serializer.serialize_i32(*v),
            EncodedValue::Int64(v) => serializer.serialize_i64(*v),
            EncodedValue::Float32(v) => serializer.serialize_f32(*v),
            EncodedValue::Float64(v) => serializer.serialize_f64(*v),
            EncodedValue::Boolean(v) => serializer.serialize_bool(*v),
            EncodedValue::Array(elements) => {
                let mut seq = serializer.serialize_seq(Some(elements.len()))?;
                for elem in elements {
                    seq.serialize_element(elem)?;
                }
                seq.end()
            }
        }
    }
}
