use crate::{create, Config, Result};
use dyn_clone::DynClone;
use std::{any::Any, ops::Range};
use tokio::io::DuplexStream;
use tokio::sync::{mpsc, oneshot};
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
    /// Given a URL that contains a bucket and might also contain an object key and version ID,
    /// extract all of those components.
    ///
    /// Fails if the URL is not valid for this object storage technology.  Also fails if the
    /// specified bucket doesn't exist.  Does not verify that the object key or version ID exist.
    ///
    /// Result of a tuple of `(bucket, key, version_id)`
    async fn parse_url(
        &self,
        url: &Url,
    ) -> Result<(Box<dyn Bucket>, Option<String>, Option<String>)>;

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

    /// Query the size of the specified object
    async fn get_object_size(&self, key: String, version_id: Option<String>) -> Result<u64>;

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
    async fn read_object_part(
        &self,
        key: String,
        version_id: Option<String>,
        byte_range: Range<u64>,
    ) -> Result<bytes::Bytes>;

    /// Read some or all of an object in one operation.
    ///
    /// Unlike [`Self::read_object_part`], this can be used for reading large objects, even up to
    /// the max allowed 5TB size.  Internally, the single read request will be split into multiple
    /// smaller parts, read in parallel (up to the configured maximum concurrency).
    ///
    /// This is the easier method to use, but it doesn't provide any control over the individual
    /// read operations made against the object.  If that is required (as it is when reading many
    /// objects at a time), use [`Self::read_object_part`].
    async fn read_object(
        &self,
        key: String,
        version_id: Option<String>,
        byte_range: Range<u64>,
    ) -> Result<mpsc::Receiver<Result<bytes::Bytes>>>;

    /// Given the known size of an object to upload, either define the ranges corresponding to the
    /// parts that should be uploaded separately via multipart upload, or return `None` indicating
    /// that the object isn't big enough to bother with multipart uploading.
    ///
    /// Applies the multipart threshold in the config provided at object storage init time.
    ///
    /// If the result is `Some`, the resulting Vec is guaranteed to have the following properties:
    /// - Contains more than one element
    /// - Ranges are sorted in ascending order of the `start` field
    /// - The first range in the vec has a `start` field of 0
    /// - The last range in the vec has an `end` field equal to `size`
    /// - Each range is contiguous and non-overlapping with the previous range
    ///
    /// A `Some` result means the object should be uploaded using [`Self::start_multipart_upload`].
    /// A `None` result means the object is small and should be uploaded with
    /// [`Self::put_small_object`].
    ///
    /// If the size of the object is not known in advance, you must use
    /// [`Self::create_object_writer`] which is quite a bit more complex.
    ///
    /// If `size` is larger than the maximum allowed object size for the object storage technology,
    /// this call will fail.
    fn partition_for_multipart_upload(
        &self,
        key: &str,
        size: u64,
    ) -> Result<Option<Vec<Range<u64>>>>;

    /// Start a multipart upload of an object with a known size which has already been partitioned
    /// with a previous call to [`Self::partition_for_multipart_upload`].
    ///
    /// The `parts` must have been returned from a prior call to `partition_for_multipart_upload`.
    fn start_multipart_upload(
        &self,
        key: String,
        parts: Vec<Range<u64>>,
    ) -> Box<dyn MultipartUploader>;

    /// Upload a small object to object storage directly without any multi-part chunking or fancy
    /// asynchrony.
    ///
    /// This should only be used for objects under the multpart threshold in size.  For anything
    /// bigger, use the more complex [`Self::create_object_writer`].
    async fn put_small_object(&self, key: String, data: bytes::Bytes) -> Result<()>;

    /// Construct an [`DuplexStream`] implementation that will upload all written data to the object
    /// identified as `key`.
    ///
    /// The size of the object to write doesn't have to be known exactly, but the caller should
    /// provide a size hint if it can predict approximately how large the object will be.
    ///
    /// The internal implementation is optimized for concurrency, and will divide the written data
    /// up into chunks which are uploaded in parallel, subject to the max concurrency in the
    /// config.
    ///
    /// The return value is a tuple with the following:
    ///
    /// - The [`DuplexStream`] writer, to which the data to upload to the object should be written.
    ///   In the event there is some error with the upload, writes to this writer will fail with a
    ///   BrokenPipe error, in which case callers should await the results receiver to get the
    ///   actual error details.
    /// - Status receiver, which receives multiple messages reporting the number of bytes uploaded
    ///   to object storage with each upload operation.  To calculate the total number of bytes
    ///   uploaded, receivers of this data must maintain a running total; this receiver yields just
    ///   the amount of bytes uploaded after each upload operation completes.  Callers who don't care
    ///   about progress reporting can drop this.
    /// - Results reciever, which will receive the result of the async task that processes all of
    ///   the writes sent to the [`DuplexStream`].  Callers should await this receiver, which will
    ///   complete only when the data written to the `DuplexStream` have all been uploaded
    ///   successfully, or some error ocurrs that causes the uploading to be aborted.
    async fn create_object_writer(
        &self,
        key: String,
        size_hint: Option<u64>,
    ) -> Result<(
        DuplexStream,
        mpsc::UnboundedReceiver<usize>,
        oneshot::Receiver<Result<u64>>,
    )>;
}

dyn_clone::clone_trait_object!(Bucket);

/// Multi-part upload client which provides a high-level API for uploading a large object with a
/// known size to object storage in multiple parts.
///
/// Clones all share the same internal state, so multiple clones can be used to call `parts` and
/// `upload_part` and the result will be as if all of those calls were made on a single instance.
///
/// `finish` must only be called once across all clones or an error ocurrs.
#[async_trait::async_trait]
pub(crate) trait MultipartUploader: DynClone + Sync + Send + 'static {
    /// Initialize this multi-part upload.
    ///
    /// This must be called exactly once, and must be the first call made on this object.
    /// `upload_part` and `finish` will panic if `init` isn't called first.
    async fn init(&self) -> Result<()>;

    /// The parts of the object to upload.
    ///
    /// These are always sorted in order from lowest to highest starting offset, and are always
    /// contiguous and non-overlapping.  Parts can be uploaded in any order, as long as they are
    /// all uploaded.
    fn parts(&self) -> &[Range<u64>];

    /// Upload a part of this object.
    ///
    /// `range` must match a range returned by [`Self::parts`], and `bytes` must have the same
    /// length as the provided range.
    async fn upload_part(&self, range: Range<u64>, bytes: bytes::Bytes) -> Result<()>;

    /// Finish the multi-part upload, after all parts have been upload with [`Self::upload_part`].
    ///
    /// This must be called exactly once across all clones of an uploader instance, and only after
    /// all parts have been uploaded with `upload_part`.
    async fn finish(&self) -> Result<()>;
}

dyn_clone::clone_trait_object!(MultipartUploader);

/// Singleton type which constructs [`ObjectStorage`] implementations on demand.
#[derive(Debug)]
pub(crate) struct ObjectStorageFactory;

impl ObjectStorageFactory {
    /// Given the URL to an object storage bucket, prefix, or object, determine which
    /// implementation handles that particular object storage technology and return an instance of
    /// it.
    ///
    /// If the URL isn't recognized as being supported by ssstar, an error is returned
    #[allow(clippy::wrong_self_convention)] // For a factory object I think it's obvious what this means
    pub async fn from_url(config: Config, url: &Url) -> Result<Box<dyn ObjectStorage>> {
        if url.scheme() == "s3" {
            Self::s3(config).await
        } else {
            crate::error::UnsupportedObjectStorageSnafu { url: url.clone() }.fail()
        }
    }

    /// Return a [`ObjectStorage`] implementation for S3 or an S3-compatible API
    pub async fn s3(config: Config) -> Result<Box<dyn ObjectStorage>> {
        // NOTE: Earlier versions of this code used a `OnceCell` object to lazily create just one
        // `S3` instance for the entire process.  This unfortunately won't work when in cases where
        // multiple tokio runtimes are in use, such as for example in Rust tests.  Each `Client`
        // object in the AWS SDK holds on to some `hyper` resources which are tied to the runtime,
        // and if the runtime is dropped and these resources are subsequently used, then a panic
        // can happen.  So, every call to `s3` will make a new `ObjectStorage` instance.  Sad.
        //
        // The bug in question is https://github.com/hyperium/hyper/issues/2892, and it seems not
        // likely to be fixed any time soon.
        Ok(Box::new(s3::S3::new(config.clone()).await?))
    }
}
