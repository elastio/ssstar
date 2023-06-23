//! Abstractions which allow for a pluggable storage layer for creating and extracting from the
//! archive.
//!
//! In the `ssstar` CLI the archive is always a `tar` format file or S3 object, but the `ssstar`
//! library crate is designed to also be used as part of the Elastio data protection solution.
//! There we need something more exotic than a simple tar archive, because we need to be able to
//! perform incremental ingest of S3 buckets, skipping reading objects that haven't changed.
use crate::objstore::{Bucket, Object};
use crate::{CreateProgressCallback, ExtractFilter, ExtractProgressCallback, Result};
use bytes::Bytes;
use dyn_clone::DynClone;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;

/// An object on object storage that is an input to an archive.
///
/// There is a related (non-public) trait `Object` in the `objstore` module which is implemented by
/// each object storage technology to abstract away the specific APIs it uses.  This struct wraps
/// that internal-only trait to make parts of it available to the storage API
#[derive(Clone, Debug)]
pub struct InputObject {
    inner: Box<dyn Object>,
    metadata: once_cell::sync::OnceCell<serde_json::Map<String, serde_json::Value>>,
}

impl InputObject {
    pub(crate) fn new(inner: Box<dyn Object>) -> Self {
        Self {
            inner,
            metadata: Default::default(),
        }
    }

    /// The key that uniquely identifies this object.
    ///
    /// Can contain prefixes separated by `/` to create the impression of a hierarchical FS-like
    /// structure, although that's just a convention
    pub fn key(&self) -> &str {
        self.inner.key()
    }

    /// If versioning is enabled on the bucket, the version ID of the object.
    pub fn version_id(&self) -> Option<&str> {
        self.inner.version_id()
    }

    /// Size in bytes of the object data
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// Timestamp when the object was created.
    pub fn timestamp(&self) -> chrono::DateTime<chrono::Utc> {
        self.inner.timestamp()
    }

    /// Query the object storage APIs to assemble all of the object's metadata (other than that
    /// already exposed via other methods on the trait) and serialize all of that metadata into a
    /// JSON representation.
    ///
    /// Consumers of this JSON can't make any assumptions about what kind of metadata is in here,
    /// only that it can be restored.
    ///
    /// The metadata produced here will be passed back to the object storage implementation of
    /// [`ObjectStorageBucket`] when the object is being restored from an archive.
    ///
    /// When restoring metadata for an object, the object storage impl must be able to work
    /// correctly with metadata obtained from an earlier version of its impl, or from impls from
    /// other object stores.  In such cases, as much of the metadata as possible should be
    /// restored.
    ///
    /// For example if an S3 object is backed up, and restored to Google GCP object storage, the
    /// tags should be preserved, even though things like storage class are AWS-specific can cannot
    /// be restored go GCP.
    pub async fn load_metadata(&self) -> Result<serde_json::Map<String, serde_json::Value>> {
        // Only retrieve the metadata from the underlying object store once.  If it was already
        // successfully retrieved, no need to retrieve it again.

        match self.metadata.get() {
            Some(metadata) => Ok(metadata.clone()),
            None => {
                let metadata = self.inner.load_metadata().await?;
                let _ = self.metadata.set(metadata.clone());
                Ok(metadata)
            }
        }
    }

    /// Consume this object and from it construct a [`ArchiveObject`] that is suitable for
    /// serializing into persistent storage as part of an archive operation.
    ///
    /// This will automatically call [`Self::load_metadata`], if it hasn't already been called, and
    /// include the custom metadata in the [`ArchiveObjectMetadata`]
    pub async fn into_archive_object(self) -> Result<ArchiveObject> {
        Ok(ArchiveObject {
            obj_store_type: self.inner.bucket().objstore().typ(),
            key: self.inner.key().to_owned(),
            version_id: self.inner.version_id().map(|id| id.to_owned()),
            metadata: ArchiveObjectMetadata {
                size: self.inner.len(),
                timestamp: self.inner.timestamp(),
                extra: self.load_metadata().await?,
            },
        })
    }
}

/// Descriptor for an object in object storage that we want to be able to store in an archive and
/// re-constitute into object storage from that archive later.
///
/// We support multiple object storage technologies, and each object storage technology has some
/// specific object properties (ie, storage class in S3) which we want to be able to preserve when
/// archiving, so this struct is necessarily somewhat generic.
///
/// Further complicating the shape of the type is that we support restoring a regular `tar` archive
/// into object storage, which of course won't have any of the object storage-specific properties.
/// So this struct is a place to capture metadata about an object storage object, all of which is
/// optional.
///
/// Each object storage implementation (see [`crate::objstore`]) will be responsible for
/// translating this generic representation of object metadata into its specific representation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ArchiveObject {
    /// A string that represents the object store type that this object was obtained from.
    ///
    /// This is set to the value returned by [`crate::objstore::ObjectStorage::typ`] for the
    /// [`ObjectStorage`] impl that obtained this object
    obj_store_type: &'static str,
    key: String,
    version_id: Option<String>,
    metadata: ArchiveObjectMetadata,
}

/// Metadata about an [`ArchiveObject`] minus the key and version ID which is common to all objects
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ArchiveObjectMetadata {
    pub size: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,

    /// Additional metadata that is specific to the object storage implementation.
    ///
    /// Outside of that object store's implementation, no assumptions are made about this
    /// additional metadata, only we attempt to preserve it in the archive.
    ///
    /// It's not guaranteed that this metadata will be preserved, so object storage implementations
    /// must not fail if this metadata is not preserved.
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// An implementation of an archive format that can be used to create an archive from object store
/// objects, and to restore objects to an object store from an archive.
#[async_trait::async_trait]
pub trait ArchiveFormat {
    type Creator: ArchiveCreator;
    type Extractor: ArchiveExtractor;
    type CreateParams: Sized;
    type ExtractParams: Sized;

    /// Create a new archive into which will be written one or more objects.
    ///
    /// How this works exactly depends on the implementation.  The only requirements at the trait
    /// level are that the init operation is async and fallible.
    async fn create_archive(
        &self,
        params: Self::CreateParams,
        progress: Arc<dyn CreateProgressCallback>,
    ) -> Result<Self::Creator>;

    /// Open an existing archive for use extracting its contents to object storage.
    ///
    /// Like [`Self::create_archive`] this is implementation-specific.
    async fn extract_archive(
        &self,
        params: Self::ExtractParams,
        progress: Arc<dyn ExtractProgressCallback>,
    ) -> Result<Self::Extractor>;
}

/// High-level interface for writing data to some archive.
///
/// We provide an implementation that will write data to a `tar` stream but other implementations
/// are possible.
#[async_trait::async_trait]
pub trait ArchiveCreator {
    /// Prepare the creator to accept objects to be written to the archive, optionally filtering
    /// out objects that do not need to be added because they are already present from a prior
    /// archive.
    ///
    /// This is called once after the creator is first created, before any objects are added.  The
    /// list of all objects that are to be written to the archive is passed in the the caller.  The
    /// storage implementation can optionally remove those objects that it knows it has already
    /// archived previously, if it has a way to nonetheless include those objects in this archive
    /// by reference.
    ///
    /// Only those objects returned by this function will be passed to [`Self::add_object`], but
    /// *all* objects in the `objects` list are expected to be in this archive when it is
    /// completed.
    async fn prepare_archive(&self, objects: Vec<InputObject>) -> Result<Vec<InputObject>>;

    /// Add an object to the archive.
    ///
    /// Because objects written to archives always come from object storage and might be multiple
    /// TB large, the contents of the object are specified as a stream that yields `Result<Bytes>`
    /// since that's what the code that downloads the object (potentially in multiple parts)
    /// provides us.
    ///
    /// This async function does not complete until the `data` stream has completed and all data
    /// yielded by that stream has been written to the archive (although not necessarily *flushed*
    /// to the archive; see [`Self::flush_and_finish`]).
    async fn add_object(
        &self,
        object: ArchiveObject,
        data: Box<dyn Stream<Item = Result<bytes::Bytes>>>,
    ) -> Result<()>;

    /// Flush any buffered data that hasn't been written already and finish out the archive.
    /// After this call completes successfully, any further attempts to write to the archive will
    /// fail.
    ///
    /// If [`Self::add_object`] fails due to some error writing to the archive, it's possible that
    /// the actual error details are not included in that error due to buffering and the use of a
    /// separate async writer task.  Thus `flush_and_finish` should be called even if
    /// [`Self::add_object`] failed, to ensure the full error details are available.
    async fn flush_and_finish(self) -> Result<u64>;
}

/// Trait which abstracts the algorithm for breaking up an archive object into multiple parts for
/// uploading to object storage.
///
/// The storage back-end shouldn't be tightly coupled to the object storage technology that
/// archives are extracted into, but one leak in that clean abstraction is the fact that the
/// storage back-end is the optimal place to break up large objects into smaller parts that can be
/// processed and uploaded to object storage separately.
pub trait MultiPartPartitioner: DynClone + Sync + Send + 'static {
    /// Given the known size of an object to upload, either define the ranges corresponding to the
    /// parts that should be uploaded separately via multipart upload, or return `None` indicating
    /// that the object isn't big enough to bother with multipart uploading.
    ///
    /// If the result is `Some`, the resulting Vec is guaranteed to have the following properties:
    /// - Contains more than one element
    /// - Ranges are sorted in ascending order of the `start` field
    /// - The first range in the vec has a `start` field of 0
    /// - The last range in the vec has an `end` field equal to `size`
    /// - Each range is contiguous and non-overlapping with the previous range
    ///
    /// If `size` is larger than the maximum allowed object size for the object storage technology,
    /// this call will fail.
    fn partition_for_multipart_upload(
        &self,
        key: &str,
        size: u64,
    ) -> Result<Option<Vec<Range<u64>>>>;
}

impl<T> MultiPartPartitioner for T
where
    T: Bucket,
{
    fn partition_for_multipart_upload(
        &self,
        key: &str,
        size: u64,
    ) -> Result<Option<Vec<Range<u64>>>> {
        <T as Bucket>::partition_for_multipart_upload(self, key, size)
    }
}

dyn_clone::clone_trait_object!(MultiPartPartitioner);

/// Extracts objects from an archive that was created with the corresponding [`ArchiveCreator`]
/// implementation.
#[async_trait::async_trait]
pub trait ArchiveExtractor {
    /// The total size in bytes of this archive, including data and metadata
    ///
    /// This might be unknown if the input to the archive is a plain `AsyncRead` impl
    fn total_bytes(&self) -> Option<u64>;

    /// Start an asynchronous process reading objects from the archive.
    ///
    /// If `filters` is not empty, then only objects matching at least one of the extract filters
    /// specified in `filters` will be read; all others will be skipped.  If `filters` is empty
    /// then all objects in the archive wll be read.
    ///
    /// The return value is a `Stream` that will asynchronously yield read objects from the
    /// archive.  Large objects will be yielded in parts suitable for multi-part upload.
    ///
    /// The read objects will be split up into multiple parts using the partioning logic provided
    /// in `multi_part_partitioner`.
    async fn read_objects(
        &self,
        filters: Vec<ExtractFilter>,
        multi_part_partitioner: Box<dyn MultiPartPartitioner>,
    ) -> Result<Box<dyn Stream<Item = ArchiveEntryComponent>>>;
}

/// An entry (meaning an object) in an archive which is being extracted, translated into one or
/// more components for more convenient processing.
///
/// Instances of this struct are produced by [`ArchiveExtractor::read_objects`].
///
/// Because archives may be read sequentially, there is not a lot of opportunity for concurrency in
/// the extracting of archive items (although there's plenty of concurrency reading the archive
/// data in chunks, and also in writing the objects to object storage after extraction).
///
/// Because of the sequential nature of archive processing, if we want to achieve any
/// concurrency we need to do it by processing the entries themselves in parallel.  To help us
/// do that, we break those entries up into multipart chunks at the source, [`ArchiveExtractor`]
/// implementation that reads from the archive.  These multiple chunks can then be processed more
/// or less in parallel by the downstream processing stages.
pub enum ArchiveEntryComponent {
    /// A small object in an archive which is small enough that there's no value in processing it
    /// in multiple parts.
    ///
    /// "small" here is defined as smaller than the configured multipart threshold specified in
    /// [`crate::Config`].
    SmallObject { object: ArchiveObject, data: Bytes },

    /// The start of an object that is large enough to be processed in multiple parts.
    ///
    /// This is guaranteed to be followed by at least one `ObjectPart`, and after all `ObjectPart`s
    /// exactly one `EndMultipartObject`.
    ///
    /// The threshold for treating a object as multipart is based on the multipart threshold in
    /// [`crate::Config`]
    StartMultipartObject {
        object: ArchiveObject,
        parts: Vec<Range<u64>>,
    },

    /// A part of a multipart object
    ObjectPart { part: Range<u64>, data: Bytes },

    /// Marks the end of a multipart object
    EndMultipartObject,
}
