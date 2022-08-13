use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, Error};
use aws_smithy_http::endpoint::Endpoint;
use once_cell::sync::OnceCell;
use snafu::prelude::*;
use std::sync::Arc;
use tracing::debug;
use url::Url;

mod error;

pub use error::{Result, S3TarError};

/// The configuration settings that control the behavior of archive creation and extraction.
///
///
#[derive(Clone, Debug)]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
pub struct Config {
    /// Use a custom S3 endpoint instead of AWS.
    ///
    /// Use this to operate on a non-Amazon S3-compatible service.  If this is set, the AWS region
    /// is ignored.
    #[cfg_attr(feature = "clap", clap(long, global = true, value_name = "URL"))]
    s3_endpoint: Option<Url>,

    /// The chunk size that ssstar uses for multipart transfers of individual files.
    ///
    /// Multipart transfers will be used for objects larger than `multipart_threshold`.
    ///
    /// Can be specified as an integer, ie "1000000", or with a suffix ie "10MB".
    ///
    /// Note that the maximum number of chunks in an upload is 10,000, so for very large objects
    /// this chunk size may be overridden if it's smaller than 1/10,000th of the size of the
    /// object.
    #[cfg_attr(feature = "clap", clap(long, default_value = "8MB", global = true))]
    multipart_chunk_size: byte_unit::Byte,

    /// The size threshold ssstar uses for multipart transfers of individual objects.
    ///
    /// If an object is this size of larger, then it will be transfered in chunks of
    /// `multipart_chunk_size` bytes each.
    ///
    /// Can be specified as an integer, ie "1000000", or with a suffix ie "10MB"
    #[cfg_attr(feature = "clap", clap(long, default_value = "8MB", global = true))]
    multipart_threshold: byte_unit::Byte,

    /// The maximum number of concurrent requests to the bucket when performing transfers.
    ///
    /// In case of multipart transfers, each chunk counts as a separate request.
    ///
    /// A higher number of concurrent requests may be necessary in order to saturate very fast
    /// connections to S3, but this will also increase RAM usage during the transfer.
    #[cfg_attr(feature = "clap", clap(long, default_value = "10", global = true))]
    max_concurrent_requests: u64,

    /// The maximum number of tasks in the task queue.
    ///
    /// In case of multipart transfers, each chunk counts as a separate task.
    #[cfg_attr(feature = "clap", clap(long, default_value = "1000", global = true))]
    max_queue_size: u64,
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

/// An object storage system like S3.
///
/// Not all object storage systems expose an S3-compatible API, so to ensure we can add support for
/// those other systems in the future, the object storage implementation is abstracted behind a
/// trait.
///
/// Use [`ObjectStorageFactory`] to create an instance of this trait.
#[async_trait::async_trait]
pub trait ObjectStorage: Sync + Send + 'static {
    /// Given a URL that contains a bucket (and possibly an object key or glob also), extract the
    /// bucket name, validate it against the underlying object storage system, and if it's valid
    /// then produce a `Bucket` object representing the bucket.
    async fn extract_bucket_from_url(&self, url: &Url) -> Result<Bucket>;

    /// Given a URL that refers to an object, a prefix, or a glob on a bucket, parse the URL and
    /// produce a corresponding `CreateArchiveInput` instance.
    ///
    /// This needs to communicate with the object storage APIs in order to validate some details so
    /// if the URL isn't valid this operation can fail.
    async fn parse_input_url(&self, url: &Url) -> Result<CreateArchiveInput> {
        let bucket = self.extract_bucket_from_url(url).await?;

        CreateArchiveInput::parse_path(bucket, url.path())
    }
}

/// Singleton type which constructs [`ObjectStorage`] implementations on demand.
///
/// Note that each implementation is also a singleton, so no more than one instance will ever be
/// created.
pub struct ObjectStorageFactory {
    config: Config,
}

impl ObjectStorageFactory {
    /// Get the ObjectStorageFactory instance, creating it if it doesn't already exist.
    ///
    /// Note that the `config` argument is ignored if a factory instance was previously created
    /// with a prior call to this method.  It's not possible to have multiple configurations in use
    /// in a single process.
    pub fn instance(config: Config) -> Arc<Self> {
        static INSTANCE: OnceCell<Arc<ObjectStorageFactory>> = OnceCell::new();

        INSTANCE
            .get_or_init(move || Arc::new(Self { config }))
            .clone()
    }

    /// Given the URL to an object storage bucket, prefix, or object, determine which
    /// implementation handles that particular object storage technology and return an instance of
    /// it.
    ///
    /// If the URL isn't recognized as being supported by ssstar, an error is returned
    pub async fn from_url(&self, url: &Url) -> Result<Arc<dyn ObjectStorage>> {
        if url.scheme() == "s3" {
            Ok(self.s3().await)
        } else {
            crate::error::UnsupportedObjectStorageSnafu { url: url.clone() }.fail()
        }
    }

    /// Return a [`ObjectStorage`] implementation for S3 or an S3-compatible API
    pub async fn s3(&self) -> Arc<dyn ObjectStorage> {
        static INSTANCE: OnceCell<Arc<dyn ObjectStorage>> = OnceCell::new();

        // The process of initializing the AWS SDK for Rust is itself async, so we can't just use
        // `get_or_init` here.  However, it's not a big deal due to two threads calling this
        // function at the same time we create two SDK clients.  It's a bit of wasted effort as one
        // of them will be immediately dropped, but it's not a huge problem.
        if let Some(instance) = INSTANCE.get() {
            instance.clone()
        } else {
            // Hasn't been initialized yet, so create a new S3 client
            match INSTANCE.try_insert(Arc::new(S3::new(self.config.clone()).await)) {
                // No other initialization happened in another thread; use the instance we just
                // created
                Ok(instance) => instance.clone(),

                // Some other thread got there first; we should use the instance the other thread
                // created, and drop the one we created
                Err((existing_instance, _new_instance)) => existing_instance.clone(),
            }
        }
    }
}

/// Implementation of [`ObjectStorage`] for S3 and S3-compatible APIs
struct S3 {
    config: Config,
    client: aws_sdk_s3::Client,
}

impl S3 {
    async fn new(config: Config) -> Self {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
        let aws_config = aws_config::from_env().region(region_provider).load().await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(s3_endpoint) = &config.s3_endpoint {
            // AWS SDK uses the `Uri` type in `http`.  There doesn't seem to be an easy way to
            // convert between the two...
            let uri: http::Uri = s3_endpoint.to_string().parse().unwrap_or_else(|e| {
                panic!(
                    "BUG: URL '{}' could not be converted into Uri: {}",
                    s3_endpoint, e
                )
            });

            s3_config_builder = s3_config_builder.endpoint_resolver(Endpoint::immutable(uri));
        }
        let aws_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

        Self {
            config,
            client: aws_client,
        }
    }
}

#[async_trait::async_trait]
impl ObjectStorage for S3 {
    async fn extract_bucket_from_url(&self, url: &Url) -> Result<Bucket> {
        // S3 URLs are of the form:
        // s3://bucket/path
        // In URL terms, the `bucket` part is considered the host name.
        let bucket = url
            .host_str()
            .ok_or_else(|| crate::error::MissingBucketSnafu { url: url.clone() }.build())?;

        debug!(bucket, "Validating access to bucket");

        self.client
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
            .with_context(|_| crate::error::BucketInvalidOrNotAccessibleSnafu {
                bucket: bucket.to_string(),
            })?;

        debug!(bucket, "Access to bucket is confirmed");

        Ok(Bucket {
            name: bucket.to_string(),
        })
    }
}

/// The description of an S3 bucket sufficient to transfer objects to and from.
#[derive(Clone, Debug)]
pub struct Bucket {
    /// The name of the bucket, which must be unique within the region where the bucket is located
    pub(crate) name: String,
}

/// An input to a tar archive.
///
/// When creating a tar archive, the user can specify the objects to ingest in a few different
/// ways.  This type represents them all
#[derive(Clone, Debug)]
pub enum CreateArchiveInput {
    /// A single S3 object located in a bucket
    Object {
        /// The key name which identifies the object in the bucket
        key: String,

        /// The ID of the object version to read.
        ///
        /// If versioning on the bucket isn't enabled, this should be `None`.
        ///
        /// If versioning on the bucket is enabled, and this is `None`, then the most recent
        /// version of the object will be used.
        version_id: Option<String>,

        /// The bucket in which this object is located
        bucket: Bucket,
    },

    /// All S3 objects in a bucket which have a common prefix.
    Prefix {
        /// The prefix to read.  All objects that have this prefix will be read.
        ///
        /// Prefixes must end with `/`, otherwise they are not treated as prefixes by the S3 API.
        /// Thus, this is guaranteed to end with "/"
        prefix: String,

        /// The bucket in which this prefix is located
        bucket: Bucket,
    },

    /// All S3 objects in the bucket
    ///
    /// This means the user specified only the bucket and nothing else in the URL, ie
    /// `s3://mybucket/`.  The final trailing `/` is optional; with or without it such a URL will
    /// be treated as refering to the entire bucket.
    Bucket(Bucket),

    /// A glob expression (using wildcards like `*` or `?`) which will be evaluated against all
    /// objects in the bucket, with matching objects being included
    Glob {
        /// The glob pattern to evaluate against all objects in the bucket
        pattern: glob::Pattern,

        /// The bucket whose objects will be evaluated against the glob pattern
        bucket: Bucket,
    },
}

impl CreateArchiveInput {
    /// Given the already-parsed bucket component of an input URL, and the path part, determine
    /// what kind of input this is and return the corresponding value.
    ///
    /// The "path" here is everything after the `s3://bucket/` part of the URL.  It could be empty
    /// or contain a prefix or object name or glob.
    fn parse_path(bucket: Bucket, path: &str) -> Result<Self> {
        if path.is_empty() || path == "/" {
            // There's nothing here just a bucket
            Ok(Self::Bucket(bucket))
        } else if path.contains('*')
            || path.contains('?')
            || path.contains('[')
            || path.contains(']')
        {
            // It looks like there's a glob here.
            let pattern = glob::Pattern::new(path).with_context(|_| {
                crate::error::InvalidGlobPatternSnafu {
                    pattern: path.to_string(),
                }
            })?;

            Ok(Self::Glob { pattern, bucket })
        } else if path.ends_with("/") {
            // Looks like a prefix
            Ok(Self::Prefix {
                prefix: path.to_string(),
                bucket,
            })
        } else {
            // The only remaining possibility is that it's a single object key
            Ok(Self::Object {
                key: path.to_string(),

                // For now this will always be None.
                // TODO: How can the version ID be specified in the S3 URL?
                version_id: None,
                bucket,
            })
        }
    }
}
