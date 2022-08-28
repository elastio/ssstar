pub mod logging;
pub mod minio;
pub mod tar;
pub mod test_data;

/// Test code that reports errors can just cheat and use `eyre`
pub type Result<T> = color_eyre::Result<T>;
