use crate::{create, Config, Result};
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
pub(crate) trait ObjectStorage: std::fmt::Debug + Sync + Send + 'static {
    /// Given a URL that contains a bucket (and possibly an object key or glob also), extract the
    /// bucket name, validate it against the underlying object storage system, and if it's valid
    /// then return the bucket name to the caller
    async fn extract_bucket_from_url(&self, url: &Url) -> Result<String>;

    /// Test if the versioning feature is enabled on this bucket
    async fn is_bucket_versioning_enabled(&self, bucket: &str) -> Result<bool>;

    /// List all objects on this object storage system that match the specified input.
    ///
    /// This will require evaluating the archive input spec against the contents of the bucket
    /// specified in the input, using whatever implementation-specific APIs are applicable
    async fn list_matching_objects(
        &self,
        input: create::CreateArchiveInput,
    ) -> Result<Vec<create::InputObject>>;
}

/// Singleton type which constructs [`ObjectStorage`] implementations on demand.
///
/// Note that each implementation is also a singleton, so no more than one instance will ever be
/// created.
#[derive(Debug)]
pub(crate) struct ObjectStorageFactory {
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
