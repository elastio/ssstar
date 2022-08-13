use url::Url;

mod config;
mod error;
mod objstore;

pub use config::Config;
pub use error::{Result, S3TarError};
pub use objstore::*;

