//! All integration tests are located in this single logical test fixture, because Rust must
//! compile and link each integration test separately which can get slow once the code base passes
//! a certain size.
//!
//! For all practical purposes each submodule here is a separate logical integration test fixture.
//! See mod-level comments in those modules for more details.

/// Test code that reports errors can just cheat and use `eyre`
type Result<T> = color_eyre::Result<T>;

mod objstore;
mod progress;
