pub mod minio;
pub mod test_data;
pub mod tar;
pub mod logging;

/// Test code that reports errors can just cheat and use `eyre`
pub type Result<T> = color_eyre::Result<T>;
