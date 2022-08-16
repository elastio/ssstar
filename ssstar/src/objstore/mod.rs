use crate::{create, Config, Result};
use dyn_clone::DynClone;
use once_cell::sync::OnceCell;
use snafu::prelude::*;
use std::{any::Any, ops::Range, sync::Arc};
use tokio::io::{AsyncWrite, DuplexStream};
use url::Url;

mod s3;

/// An object storage system like S3.
///
/// Not all object storage systems expose an S3-compatible API, so to ensure we can add support for
/// those other systems in the future, the object storage implementation is abstracted behind a
/// trait.
///
/// Use [`ObjectStorageFactory`] to create an instance of this trait.
///
/// Note that all implementations are trivially cloneable such that the cost of a clone is the cost
/// of increasing the ref count on an `Arc`
#[async_trait::async_trait]
pub(crate) trait ObjectStorage: DynClone + std::fmt::Debug + Sync + Send + 'static {
    /// Given a URL that contains a bucket (and possibly an object key or glob also), extract the
    /// bucket name, validate it against the underlying object storage system, and if it's valid
    /// then return the bucket name to the caller
    async fn extract_bucket_from_url(&self, url: &Url) -> Result<Box<dyn Bucket>>;
}

dyn_clone::clone_trait_object!(ObjectStorage);

/// A bucket which is like a namespace in which object storage systems store named objects.
///
/// Each implementation of this trait is specific to the corresponding implementation of
/// [`ObjectStorage`] and cannot be mixed with another implementation or a runtime panic can ocurr.
///
/// Note that all implementations are trivially cloneable such that the cost of a clone is the cost
/// of increasing the ref count on an `Arc`
#[async_trait::async_trait]
pub(crate) trait Bucket: DynClone + std::fmt::Debug + Sync + Send + 'static {
    /// HACK so that implementations can downcast from `Arc<dyn Bucket>` to the
    /// implementation-specific type.  Pretend you didn't see this.
    #[doc(hidden)]
    fn as_any(&self) -> &(dyn Any + Sync + Send);

    fn objstore(&self) -> Box<dyn ObjectStorage>;

    fn name(&self) -> &str;

    /// List all objects in this bucket that match the specified selector
    ///
    /// This will require evaluating the archive input spec against the contents of the bucket,
    /// using whatever implementation-specific APIs are applicable
    async fn list_matching_objects(
        &self,
        selector: create::ObjectSelector,
    ) -> Result<Vec<create::InputObject>>;

    /// Read a part of an object.
    ///
    /// This performs the read as a single network call, which means it's not suited for reading
    /// large (multiple hundreds of MB or more) data.  For that, multiple `read_object_part` calls
    /// should be made in parallel for different ranges of the same object.
    async fn read_object_part(&self, key: String, version_id: Option<String>, byte_range: Range<u64>) -> Result<bytes::Bytes>;

    /// Construct an [`DuplexStream`] implementation that will upload all written data to the object
    /// identified as `key`.
    ///
    /// The size of the object to write doesn't have to be known exactly, but the caller should
    /// provide a size hint if it can predict approximately how large the object will be.
    ///
    /// The internal implementation is optimized for concurrency, and will divide the written data
    /// up into chunks which are uploaded in parallel, subject to the max concurrency in the
    /// config.
    async fn create_object_writer(
        &self,
        key: String,
        size_hint: Option<u64>,
    ) -> Result<DuplexStream>;
}

dyn_clone::clone_trait_object!(Bucket);

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
