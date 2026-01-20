//! Encoded types (EType) for Hail's binary format
//!
//! Hail uses a different type system for encoding (EType) vs the logical type (HailType).
//! The encoding type determines the binary representation.

use crate::buffer::InputBuffer;
use crate::error::{HailError, Result};

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

    /// Read a value using this encoding
    pub fn read(&self, buffer: &mut dyn InputBuffer) -> Result<EncodedValue> {
        // If not required, check for null flag
        if !self.is_required() {
            let is_present = buffer.read_bool()?;
            if !is_present {
                return Ok(EncodedValue::Null);
            }
        }

        match self {
            EncodedType::EBinary { .. } => {
                // Read byte-length prefix
                let length = buffer.read_u8()? as usize;
                let mut bytes = vec![0u8; length];
                buffer.read_exact(&mut bytes)?;
                Ok(EncodedValue::Binary(bytes))
            }
            EncodedType::EBaseStruct { fields, .. } => {
                let mut field_values = Vec::with_capacity(fields.len());
                for field in fields {
                    let value = field.encoded_type.read(buffer)?;
                    field_values.push((field.name.clone(), value));
                }
                Ok(EncodedValue::Struct(field_values))
            }
            EncodedType::EInt32 { .. } => Ok(EncodedValue::Int32(buffer.read_i32()?)),
            EncodedType::EInt64 { .. } => Ok(EncodedValue::Int64(buffer.read_i64()?)),
            EncodedType::EFloat32 { .. } => Ok(EncodedValue::Float32(buffer.read_f32()?)),
            EncodedType::EFloat64 { .. } => Ok(EncodedValue::Float64(buffer.read_f64()?)),
            EncodedType::EBoolean { .. } => Ok(EncodedValue::Boolean(buffer.read_bool()?)),
            EncodedType::EArray { element, .. } => {
                // Arrays: [i32 length][elements...]
                let length = buffer.read_i32()? as usize;
                let mut elements = Vec::with_capacity(length);
                for _ in 0..length {
                    elements.push(element.read(buffer)?);
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
