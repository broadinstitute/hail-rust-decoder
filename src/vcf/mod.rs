//! VCF support for Hail Decoder
//!
//! This module provides support for reading VCF (Variant Call Format) files
//! and converting them to the same `EncodedType`/`EncodedValue` format used
//! by Hail Tables.
//!
//! ## Modules
//!
//! - `schema`: Converts VCF headers to `EncodedType` schemas
//! - `codec`: Converts VCF records to `EncodedValue` rows

pub mod codec;
pub mod schema;
