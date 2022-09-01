//! Testing helpers for use writing unit and integration tests of the `ssstar` crate
//!
//! This is internal to `ssstar` and is not intended for use by any other crates.  Breaking changes
//! can be made at any time.  The only reason this is published at all is that `cargo publish`
//! requires that all `dev-dependencies` be resolvable in the public registry.
pub mod logging;
pub mod minio;
pub mod tar;
pub mod test_data;

/// Test code that reports errors can just cheat and use `eyre`
pub type Result<T> = color_eyre::Result<T>;
