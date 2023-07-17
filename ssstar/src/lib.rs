#![doc = include_str!("../README.md")]

mod async_bridge;
mod config;
mod create;
mod credentials_provider;
mod error;
mod extract;
mod objstore;
#[cfg(feature = "storage")]
mod storage;
mod tar;
mod writers;

pub use config::Config;
pub use create::*;
pub use credentials_provider::{
    CredentialsProvider, CredentialsUpdateCallback, CredentialsUpdateOutput,
};
pub use error::{Result, S3TarError};
pub use extract::*;
#[cfg(feature = "storage")]
pub use storage::*;
