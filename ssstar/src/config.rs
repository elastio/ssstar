use url::Url;

/// The configuration settings that control the behavior of archive creation and extraction.
///
///
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
pub struct Config {
    /// Use a custom S3 endpoint instead of AWS.
    ///
    /// Use this to operate on a non-Amazon S3-compatible service.  If this is set, the AWS region
    /// is ignored.
    #[cfg_attr(feature = "clap", clap(long, global = true, value_name = "URL"))]
    pub(crate) s3_endpoint: Option<Url>,

    /// The chunk size that ssstar uses for multipart transfers of individual files.
    ///
    /// Multipart transfers will be used for objects larger than `multipart_threshold`.
    ///
    /// Can be specified as an integer, ie "1000000", or with a suffix ie "10MB".
    ///
    /// Note that the maximum number of chunks in an upload is 10,000, so for very large objects
    /// this chunk size may be overridden if it's smaller than 1/10,000th of the size of the
    /// object.
    #[cfg_attr(feature = "clap", clap(long, default_value = "8MiB", global = true))]
    pub(crate) multipart_chunk_size: byte_unit::Byte,

    /// The size threshold ssstar uses for multipart transfers of individual objects.
    ///
    /// If an object is this size of larger, then it will be transfered in chunks of
    /// `multipart_chunk_size` bytes each.
    ///
    /// Can be specified as an integer, ie "1000000", or with a suffix ie "10MB"
    #[cfg_attr(feature = "clap", clap(long, default_value = "8MiB", global = true))]
    pub(crate) multipart_threshold: byte_unit::Byte,

    /// The maximum number of concurrent requests to the bucket when performing transfers.
    ///
    /// In case of multipart transfers, each chunk counts as a separate request.
    ///
    /// A higher number of concurrent requests may be necessary in order to saturate very fast
    /// connections to S3, but this will also increase RAM usage during the transfer.
    #[cfg_attr(feature = "clap", clap(long, default_value = "10", global = true))]
    pub(crate) max_concurrent_requests: usize,

    /// The maximum number of tasks in the task queue.
    ///
    /// In case of multipart transfers, each chunk counts as a separate task.
    #[cfg_attr(feature = "clap", clap(long, default_value = "1000", global = true))]
    pub(crate) max_queue_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        // XXX: Unfortunately this is duplicated here and in the `clap` attributes, unfortunately I
        // can't find a better way unless we unconditionally take a clap dependency in the lib
        // crate which I'm not willing to do
        Self {
            s3_endpoint: None,
            multipart_chunk_size: byte_unit::Byte::from_bytes(8 * 1024 * 1024),
            multipart_threshold: byte_unit::Byte::from_bytes(8 * 1024 * 1024),
            max_concurrent_requests: 10,
            max_queue_size: 1000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// If clap is enabled, verify that the `Default` impl and the clap-declared defaults match, to
    /// detect if they ever drift out of sync in the future
    #[cfg(feature = "clap")]
    #[test]
    fn defaults_match() {
        use clap::Parser;

        let args: &'static [&'static str] = &[];
        let clap_default = Config::parse_from(args);

        let rust_default = Config::default();

        assert_eq!(clap_default, rust_default);
    }
}
