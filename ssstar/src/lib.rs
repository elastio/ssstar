#![doc = include_str!("../README.md")]

mod async_bridge;
mod config;
mod create;
mod custom_credentials_provider;
mod error;
mod extract;
mod objstore;
#[cfg(feature = "storage")]
mod storage;
mod tar;
mod writers;

pub use config::Config;
pub use create::*;
pub use custom_credentials_provider::{
    CustomCredentialsProvider, CustomCredentialsUpdateCallback, CustomCredentialsUpdateOutput,
};
pub use error::{Result, S3TarError};
pub use extract::*;
#[cfg(feature = "storage")]
pub use storage::*;
