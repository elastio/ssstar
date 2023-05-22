//! Abstractions which allow for a pluggable storage layer for creating and extracting from the
//! archive.
//!
//! In the `ssstar` CLI the archive is always a `tar` format file or S3 object, but the `ssstar`
//! library crate is designed to also be used as part of the Elastio data protection solution.
//! There we need something more exotic than a simple tar archive, because we need to be able to
//! perform incremental ingest of S3 buckets, skipping reading objects that haven't changed.
use crate::objstore::Bucket;
use crate::{CreateProgressCallback, ExtractFilter, ExtractProgressCallback, Result};
use bytes::Bytes;
use dyn_clone::DynClone;
use futures::Stream;
use std::ops::Range;
use std::sync::Arc;

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
#[derive(Clone, Debug, Default)]
pub struct ArchiveObject {
    pub obj_store_type: &'static str,
    pub key: String,
    pub version_id: Option<String>,
    pub metadata: ArchiveObjectMetadata,
}

/// Metadata about an [`ArchiveObject`] minus the key and version ID which is common to all objects
#[derive(Clone, Debug, Default)]
pub struct ArchiveObjectMetadata {
    pub size: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub tags: Vec<(String, Option<String>)>,

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
    /// Given the total number of objects and total number of bytes that will be written to the archive,
    /// estimate the size of the archive.
    ///
    /// This doesn't have to be precise, but if it is approximate it should err on the side of
    /// larger than the actual size as opposed to smaller.  The estimated size will be used to
    /// determine what the multipart size should be when uploading, so if the estimate is smaller
    /// than the actual size it's possible the multipart size will be chosen that's too small to
    /// represent the entire archive, since S3 has a limited of 10K parts in each multi-part
    /// upload.
    fn estimate_archive_size(&self, total_objects: u64, total_bytes: u64) -> u64;

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
