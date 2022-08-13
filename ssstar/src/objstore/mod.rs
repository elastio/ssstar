use crate::{Config, Result};
use once_cell::sync::OnceCell;
use snafu::prelude::*;
use std::sync::Arc;
use url::Url;

mod s3;

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
            match INSTANCE.try_insert(Arc::new(s3::S3::new(self.config.clone()).await)) {
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
