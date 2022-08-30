//! Implementation of the extract operation which restores from a tar archive to S3.
//!
//! The pattern used for archive creation is similar to the way extract works.  The caller starts
//! with [`ExtractArchiveJobBuilder`], optionally adds extract filters, then calls
//! [`ExtractArchiveJobBuilder::build`] to construct an [`ExtractArchiveJob`] instance.  A call to
//! [`ExtractArchiveJob::run`] actually performs the job.  Progress is reported via  a
//! caller-provided implementation of the [`ExtractProgressCallback`] trait.
//!
//! Unfortunately extract isn't quite as parallelized as archive creation.  Archive creation will
//! perform many parallel downloads of objects, or even if there's only one object to archive it
//! will perform many parallel downloads of different parts of the object in parallel.  Archive
//! extraction doesn't have that luxury.
//!
//! While the archive, if located on object storage, will be broken up into chunks and downloaded
//! in parallel (up to the max request count), nonetheless the sequential nature of the tar format
//! means that the `tar` crate will yield entries one at a time, and it's not possible using the
//! `tar` crate's API to read multiple non-overlapping ranges of each object in parallel.  So the
//! extract process runs serially, producing objects to extract to object storage.
//!
//! If the archive contains mainly very large objects (100MB or larger), extract performance will
//! still benefit from parallel processing because the uploading of those large objects, once their
//! data is extracted, can be done in parallel using the
//! [`crate::objstore::Bucket::create_object_writer`] method.  This will buffer chunks extracted
//! from the tar archive and perform multiple parallel chunk uploads until the entire object has
//! been extracted.  Assuming the upload to object storage is slower than the extracting of the
//! archive from whatever source it came from, the use of parallel chunk uploads will improve
//! performance compared to a non-parallel approach.
//!
//! However, for archives with many small (< the multipart threshold) objects, none of this
//! parallelism will be available, so each object will be restored to object storage one at a time.
//! For small objects, uploading the data takes no time at all, so the latency of the S3 API calls
//! dominates.
//!
//! Perhaps a future enhancement will enhance this so that even in the case of many small objects
//! the extract process can benefit from parallelism, but this is much more challenging than in the
//! archive creation implementation.
use crate::objstore::{Bucket, ObjectStorageFactory};
use crate::tar::CountingReader;
use crate::{Config, Result};
use bytes::{Bytes, BytesMut};
use snafu::prelude::*;
use std::future::Future;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tracing::{debug, debug_span, error, info, info_span, instrument, trace, Instrument};
use url::Url;

/// Represents from where the archive will be read
pub enum SourceArchive {
    /// Read the tar archive from object storage at the specified URL.
    ///
    /// The URL must specify a bucket and a complete object name, and obviously that object must
    /// exist and be a tar archive without any compression.
    ObjectStorage(Url),

    /// Read the tar archive from the local filesystem
    File(PathBuf),

    /// Read the tar archive to some arbitrary [`std::io::Read`] impl.
    Reader(Box<dyn Read + Send>),
}

impl std::fmt::Debug for SourceArchive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObjectStorage(url) => f.debug_tuple("ObjectStorage").field(url).finish(),
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
            Self::Reader(_) => f
                .debug_tuple("Reader")
                .field(&"dyn Read".to_string())
                .finish(),
        }
    }
}

impl SourceArchive {
    /// Obtain more details about the archive source and convert this into a
    /// [`SourceArchiveInternal`]
    #[instrument(skip(objstore_factory))]
    async fn into_internal(
        self,
        objstore_factory: Arc<ObjectStorageFactory>,
    ) -> Result<SourceArchiveInternal> {
        match self {
            Self::ObjectStorage(url) => {
                let objstore = objstore_factory.from_url(&url).await?;
                let (bucket, key, version_id) = objstore.parse_url(&url).await?;

                let key = key.ok_or_else(|| {
                    crate::error::ArchiveUrlInvalidSnafu { url: url.clone() }.build()
                })?;

                let len = bucket.get_object_size(key.clone(), None).await?;

                Ok(SourceArchiveInternal::ObjectStorage {
                    bucket,
                    key,
                    version_id,
                    len,
                })
            }
            Self::File(path) => {
                let metadata = tokio::fs::metadata(&path).await.with_context(|_| {
                    crate::error::OpeningArchiveFileSnafu { path: path.clone() }
                })?;

                Ok(SourceArchiveInternal::File { path, metadata })
            }
            Self::Reader(reader) => {
                // Noting to do here, all we know about the source is that it's an opaque reader
                Ok(SourceArchiveInternal::Reader(reader))
            }
        }
    }
}

/// Internal-only representation of [`SourceArchive`] that includes additional details about the
/// input
enum SourceArchiveInternal {
    ObjectStorage {
        bucket: Box<dyn Bucket>,
        key: String,
        version_id: Option<String>,
        len: u64,
    },

    File {
        path: PathBuf,
        metadata: std::fs::Metadata,
    },

    Reader(Box<dyn Read + Send>),
}

impl std::fmt::Debug for SourceArchiveInternal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObjectStorage {
                bucket,
                key,
                version_id,
                len,
            } => f
                .debug_struct("ObjectStorage")
                .field("bucket", &bucket.name())
                .field("key", &key)
                .field("version_id", &version_id)
                .field("len", &len)
                .finish(),
            Self::File { path, metadata } => f
                .debug_struct("File")
                .field("path", &path)
                .field("metadata", metadata)
                .finish(),
            Self::Reader(_) => f
                .debug_tuple("Reader")
                .field(&"dyn Read".to_string())
                .finish(),
        }
    }
}

impl SourceArchiveInternal {
    /// Produce a blocking [`std::io::Read`] impl that will produce the contents of the tar
    /// archive.
    ///
    /// This needs to be blocking because the `tar` crate doesn't work with async I/O.
    async fn into_reader(
        self,
        progress: Arc<dyn ExtractProgressCallback>,
    ) -> Result<CountingReader<Box<dyn Read + Send>>> {
        let reader: Box<dyn Read + Send> = match self {
            Self::ObjectStorage {
                bucket,
                key,
                version_id,
                len,
            } => {
                // Read the object as a stream of chunks of bytes, then use our async bridge code
                // to put a blocking `Read` interface on top
                Box::new(crate::async_bridge::stream_as_reader(
                    tokio_stream::wrappers::ReceiverStream::new(
                        bucket.read_object(key, version_id, 0..len).await?,
                    ),
                ))
            }
            SourceArchiveInternal::File { path, metadata: _ } => {
                // Open the file with the blocking `File::open` which needs to be done in a
                // blocking thread
                let file = tokio::task::spawn_blocking(move || {
                    std::fs::File::open(&path).with_context(|_| {
                        crate::error::OpeningArchiveFileSnafu { path: path.clone() }
                    })
                })
                .await
                .with_context(|_| crate::error::SpawnBlockingSnafu {})??;

                Box::new(file)
            }
            SourceArchiveInternal::Reader(reader) => reader,
        };

        // We need to wrap whatever the reader is in `CountingReader` both to report progress as
        // the archive is read, but also so we can report the total size of the archive at the end
        // of the extraction.
        Ok(CountingReader::new(reader, progress))
    }
}

impl SourceArchiveInternal {
    /// Attempt to get the size of the archive represented by this instance.
    ///
    /// This isn't always possible since `Reader` has no size.
    async fn get_size(&self) -> Result<Option<u64>> {
        match self {
            Self::ObjectStorage { len, .. } => {
                // S3 objects always have a size
                Ok(Some(*len))
            }
            Self::File { metadata, .. } => {
                // Files always have a size
                Ok(Some(metadata.len()))
            }
            Self::Reader(_) => {
                // Arbitrary `Read` trait objects don't have any way of telling us their size
                Ok(None)
            }
        }
    }
}

#[derive(Debug)]
enum ExtractFilter {
    Object { key: String },
    Prefix { prefix: String },
    Glob { pattern: glob::Pattern },
}

impl FromStr for ExtractFilter {
    type Err = crate::S3TarError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() || s == "/" {
            // That's not valid
            crate::error::InvalidFilterSnafu {
                filter: s.to_string(),
            }
            .fail()
        } else if s.contains('*') || s.contains('?') || s.contains('[') || s.contains(']') {
            // This looks like a glob
            Ok(Self::Glob {
                pattern: glob::Pattern::from_str(s).with_context(|_| {
                    crate::error::InvalidGlobPatternSnafu {
                        pattern: s.to_string(),
                    }
                })?,
            })
        } else if s.ends_with("/") {
            // This is a prefix
            Ok(Self::Prefix {
                prefix: s.to_string(),
            })
        } else {
            // Else this is the key of a specific object to extract
            Ok(Self::Object { key: s.to_string() })
        }
    }
}

impl ExtractFilter {
    /// Evaluate this filter against the tar entry file path
    ///
    /// Tar archive entries always have a relative path, and the path is equal to the object key,
    /// inclusive of any prefixes.  So if the object was `foo/bar/baz`, then `path` here will also
    /// be `foo/bar/baz`
    fn matches(&self, path: &Path) -> bool {
        match self {
            Self::Object { key } => path.to_string_lossy() == key.as_ref(),
            Self::Prefix { prefix } => path.to_string_lossy().starts_with(&*prefix),
            Self::Glob { pattern } => {
                // To make sure the glob matching behaviors like it does in unix shells, require
                // that `/` path separator chars must be matched by literal `/` and will never be
                // matched by a `*` or `?`.  Without this, `prefix1/*` will match an object
                // `prefix1/prefix2/test` which is absolutely not how UNIX shell globbing works
                let match_options = glob::MatchOptions {
                    require_literal_separator: true,
                    ..Default::default()
                };

                pattern.matches_with(&path.to_string_lossy(), match_options)
            }
        }
    }
}

#[derive(Debug)]
pub struct ExtractArchiveJobBuilder {
    config: Config,
    objstore_factory: Arc<ObjectStorageFactory>,
    source_archive: SourceArchive,
    target_bucket: Box<dyn Bucket>,
    target_prefix: String,
    filters: Vec<ExtractFilter>,
}

impl ExtractArchiveJobBuilder {
    pub async fn new(config: Config, source: SourceArchive, target: Url) -> Result<Self> {
        let objstore_factory = ObjectStorageFactory::instance(config.clone());
        let objstore = objstore_factory.from_url(&target).await?;

        let (bucket, key, _) = objstore.parse_url(&target).await?;

        Ok(Self {
            config,
            objstore_factory,
            source_archive: source,
            target_bucket: bucket,
            target_prefix: key.unwrap_or_default(),
            filters: Vec::new(),
        })
    }

    pub fn add_filter(&mut self, filter: impl AsRef<str>) -> Result<()> {
        self.filters.push(filter.as_ref().parse()?);

        Ok(())
    }

    pub async fn build(self) -> Result<ExtractArchiveJob> {
        // Validate the source archive by actually getting its metadata
        let source_archive = self
            .source_archive
            .into_internal(self.objstore_factory)
            .await?;

        // For progress estimation purposes it's helpful to know how large the archive we're going
        // to read is.
        let archive_size = source_archive.get_size().await?;

        Ok(ExtractArchiveJob {
            config: self.config,
            source_archive,
            target_bucket: self.target_bucket,
            target_prefix: self.target_prefix,
            filters: self.filters,
            archive_size,
        })
    }
}

/// A trait which callers can implement to get detailed progress updates as the extract operation
/// is progressing.
#[allow(unused_variables)]
pub trait ExtractProgressCallback: Sync + Send {
    /// The extracting operation is now starting.
    ///
    /// The archive size is provided if it's known.  Extraction from an arbitrary stream doesn't
    /// have a known size.
    fn extract_starting(&self, archive_size: Option<u64>) {}

    /// Some bytes from the archive have been read.
    ///
    /// This measures raw bytes read from the archive, it will include actual objects' data, plus
    /// the metadata structures of the `tar` format.
    fn extract_archive_part_read(&self, bytes: usize) {}

    /// An object in the archive is being skipped because it doesn't match any of the provided
    /// filters
    fn extract_object_skipped(&self, key: &str, size: u64) {}

    /// Starting to extract the data for a given object
    fn extract_object_starting(&self, key: &str, size: u64) {}

    /// A part of an object has been read from the archive and will now go into the queue for
    /// writing to object storage.
    fn extract_object_part_read(&self, key: &str, bytes: usize) {}

    /// The extracting of data for the given object has finished.
    ///
    /// The upload of the extracted data to object storage is still probably going on and is
    /// reported in separate progress calls.
    fn extract_object_finished(&self, key: &str, size: u64) {}

    /// The extraction process has finished, everything in the archive has been read.
    ///
    /// The `total_bytes` argument is the total number of bytes read from the archive, and it
    /// includes extracted and skipped objects as well as all tar format headers.
    ///
    /// This doesn't meant the extract job is done.  The data that was extracted still needs to be
    /// written to object storage, and the progress of that is reported separately.
    fn extract_finished(
        &self,
        extracted_objects: usize,
        extracted_object_bytes: u64,
        skipped_objects: usize,
        skipped_object_bytes: u64,
        total_bytes: u64,
    ) {
    }

    /// The upload of the specified object is starting
    fn object_upload_starting(&self, key: &str, size: u64) {}

    /// A part of an object has been uploaded to object storage
    fn object_part_uploaded(&self, key: &str, bytes: usize) {}

    /// The object has been uploaded in its entirety
    fn object_uploaded(&self, key: &str, size: u64) {}

    /// All objects that matched the filter criteria have been uploaded and the job is now ready to
    /// complete
    fn objects_uploaded(&self, total_objects: usize, total_object_bytes: u64) {}
}

#[derive(Debug)]
pub struct ExtractArchiveJob {
    config: Config,
    source_archive: SourceArchiveInternal,
    target_bucket: Box<dyn Bucket>,
    target_prefix: String,
    filters: Vec<ExtractFilter>,
    archive_size: Option<u64>,
}

impl ExtractArchiveJob {
    /// The size of the archive that is to be extracted.
    ///
    /// This might not be known if the input is a generic `AsyncRead`, such as a stream from `stdin`
    pub fn archive_size(&self) -> Option<u64> {
        self.archive_size
    }

    /// Alternative to [`Self::run`] which doesn't require a [`ProgressCallback`] implementation,
    /// for callers that do not care about progress information.
    pub async fn run_without_progress(self, abort: impl Future<Output = ()>) -> Result<()> {
        // A dummy impl of ProgressCallback that doesn't do anything with any of the progress
        // updates
        struct NoProgress {}
        impl ExtractProgressCallback for NoProgress {}

        self.run(abort, NoProgress {}).await
    }

    /// Run the job, returning only when the job has run to completion (or failed)
    ///
    /// If the `abort` future is completed, it's a signal that the job should be aborted.
    /// Existing transfers will be abandoned and queued transfers will be dropped, then this method
    /// returns an abort error.
    pub async fn run<Abort, Progress>(self, _abort: Abort, progress: Progress) -> Result<()>
    where
        Abort: Future<Output = ()>,
        Progress: ExtractProgressCallback + 'static,
    {
        let span = info_span!("run",
            source_archive = ?self.source_archive,
            target_bucket = self.target_bucket.name(),
            target_prefix = %self.target_prefix);

        async move {
            info!(?self.filters, ?self.archive_size, "Starting extract archive job");

            let progress: Arc<dyn ExtractProgressCallback> = Arc::new(progress);

            progress.extract_starting(self.archive_size);

            // The reading of entries from the tar archive using the `tar` crate is a blocking
            // operation.  Start a blocking task to do that now.
            let (entry_sender, entry_receiver) = mpsc::channel(self.config.max_concurrent_requests);
            let reader_span =
                info_span!("read_tar_entries_blocking", source_archive = ?self.source_archive);
            let reader = self.source_archive.into_reader(progress.clone()).await?;
            let reader_fut = {
                let progress = progress.clone();
                tokio::task::spawn_blocking(move || {
                    let _guard = reader_span.enter();

                    debug!("Starting blocking tar read task");

                    match Self::read_tar_entries_blocking(
                        self.filters,
                        reader,
                        progress,
                        entry_sender,
                        self.config.multipart_threshold.get_bytes() as usize,
                        self.config.multipart_chunk_size.get_bytes() as usize,
                    ) {
                        Ok(()) => {
                            debug!("Blocking tar read task completing successfully");

                            Ok(())
                        }
                        Err(e) => {
                            error!(err = ?e, "Blocking tar read task failed");

                            Err(e)
                        }
                    }
                })
            };

            // Start an async task that will process those `TarEntry` structs produced by the
            // blocking tar reader task

            let processor_fut = {
                let progress = progress.clone();

                tokio::spawn(async move {
                    debug!("Starting tar entry processor task");

                    match Self::process_tar_entries(
                        self.target_bucket,
                        self.target_prefix,
                        progress,
                        entry_receiver,
                    )
                    .await
                    {
                        Ok(totals) => {
                            debug!("Tar entry processor task completed successfully");

                            Ok(totals)
                        }

                        Err(e) => {
                            error!(err = ?e, "Tar entry processor task failed");

                            Err(e)
                        }
                    }
                })
            };

            // Wait for both tasks to finish and only then look at results
            let (reader_result, processor_result) = futures::join!(reader_fut, processor_fut);
            let reader_result =
                reader_result.with_context(|_| crate::error::SpawnBlockingSnafu {})?;
            let processor_result =
                processor_result.with_context(|_| crate::error::SpawnSnafu {})?;

            // If there's an error in the reader, the processor can also fail but with a less
            // meaningful error.  So evaluate reader results first.
            //
            // If the error indicates the sender mpsc was dropped, that's the queue to check the
            // processor task's result instead
            let (total_objects, total_object_bytes) = match reader_result {
                Ok(_) => {
                    // Good sign.  Only the processor can fail
                    processor_result?
                }
                e @ Err(crate::S3TarError::TarExtractAborted) => {
                    // This is the error the reader fails with when it got an error sending
                    // something to the processor task.  This suggests the processor task has
                    // failed.
                    processor_result?;

                    // `processor_result` wasn't an error, so return the reader error
                    return e;
                }
                Err(e) => {
                    // Any other error means the failure was on the reader's side
                    return Err(e);
                }
            };

            progress.objects_uploaded(total_objects, total_object_bytes);

            info!("Finished extract job");

            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Blocking worker task that iterates over the entries in the `tar` archive, and pushes them
    /// onto the MPSC queue where they will be processed by async code doing the extraction
    fn read_tar_entries_blocking(
        filters: Vec<ExtractFilter>,
        reader: CountingReader<Box<dyn Read + Send>>,
        progress: Arc<dyn ExtractProgressCallback>,
        mut entry_sender: mpsc::Sender<TarEntryComponent>,
        multipart_threshold: usize,
        multipart_chunk_size: usize,
    ) -> Result<()> {
        let mut archive = tar::Archive::new(reader);

        let mut extracted_objects = 0usize;
        let mut extracted_object_bytes = 0u64;
        let mut skipped_objects = 0usize;
        let mut skipped_object_bytes = 0u64;

        for result in archive
            .entries()
            .with_context(|_| crate::error::TarReadSnafu {})?
        {
            let mut entry = result.with_context(|_| crate::error::TarReadSnafu {})?;

            let path = entry
                .path()
                .with_context(|_| crate::error::TarReadSnafu {})?
                .into_owned();
            let len = entry.size();
            let key = path.display().to_string();

            let span = debug_span!("Processing tar entry", path = %path.display(), len);
            let _guard = span.enter();

            // To save processing effort, apply the filters right here at the source.  If this
            // entry doesn't match one of the filters, and there are any filters, then skip
            // processing it.
            //
            // NOTE: all of the data of this entry will still be read by the `tar::Archive` because
            // it's not possible to seek in this reader.  A future enhancement https://github.com/elastio/ssstar/issues/1
            // would be to support `Seek` for tar archives that are in local files or object
            // storage, which would allow quickly skipping files that don't pass the filters
            let included = if filters.is_empty() {
                debug!("No filters are applied so this entry is included");
                true
            } else {
                if let Some(filter) = filters.iter().find(|filter| filter.matches(&path)) {
                    debug!(?filter, "Filter matched so this entry is included");
                    true
                } else {
                    debug!("No filters matched, so this entry is excluded");
                    false
                }
            };

            if !included {
                progress.extract_object_skipped(&key, len);
                skipped_objects += 1;
                skipped_object_bytes += len;

                continue;
            }

            progress.extract_object_starting(&key, len);

            // the `tar::Entry` type itself implements `Read` which will read the data for the
            // entry.
            // we exploit that here.
            if len < multipart_threshold as u64 {
                // This file is small enough there's no value in processing it as multiple parts
                debug!("Processing entry as a small file");
                let data = Self::read_data(&mut entry, len as usize)?;

                progress.extract_object_part_read(&key, len as usize);
                progress.extract_object_finished(&key, len);
                extracted_objects += 1;
                extracted_object_bytes += len;

                Self::send_component(
                    TarEntryComponent::SmallFile { path, data },
                    &mut entry_sender,
                )?;
            } else {
                // This file is big enough that it's worth breaking up into multipart chunks
                debug!("Processing entry as multi-part");
                Self::send_component(
                    TarEntryComponent::StartMultipartFile { path, len },
                    &mut entry_sender,
                )?;

                // NOTE: don't confuse the concept of multipart in this module with the object
                // store-specific multi-part upload APIs.  Each `Bucket` impl is responsible for
                // ensuring that multi-part chunking is applied correctly for that particular
                // implementation.  Meaning these chunk boundaries aren't necessarily the exact
                // chunk boundaries that will be used when uploading to object storage.  The
                // purpose of multi-part here is to start sending data from the blocking tar reader
                // to the async processor task quickly, so it can start uploading without waiting
                // for the entire download and extract to finish.
                let mut bytes_remaining = len;
                while bytes_remaining > 0 {
                    let data = Self::read_data(
                        &mut entry,
                        bytes_remaining.min(multipart_chunk_size as u64) as usize,
                    )?;
                    bytes_remaining -= data.len() as u64;
                    progress.extract_object_part_read(&key, data.len());

                    let component = if bytes_remaining > 0 {
                        TarEntryComponent::FilePart { data }
                    } else {
                        TarEntryComponent::EndMultipartFile { data }
                    };

                    Self::send_component(component, &mut entry_sender)?;
                }

                progress.extract_object_finished(&key, len);
                extracted_objects += 1;
                extracted_object_bytes += len;
            }
        }

        debug!("Completed processing all tar entries");

        // Recover the underlying CountingReader to we can get the total read bytes count
        let reader = archive.into_inner();

        progress.extract_finished(
            extracted_objects,
            extracted_object_bytes,
            skipped_objects,
            skipped_object_bytes,
            reader.total_bytes_read(),
        );

        Ok(())
    }

    /// Helper that reads `len` bytes from a reader into a `Bytes` structure.
    fn read_data(reader: &mut impl Read, len: usize) -> Result<Bytes> {
        let mut bytes = BytesMut::zeroed(len);

        reader
            .read_exact(&mut bytes)
            .with_context(|_| crate::error::TarReadSnafu {})?;

        Ok(bytes.freeze())
    }

    fn send_component(
        component: TarEntryComponent,
        sender: &mpsc::Sender<TarEntryComponent>,
    ) -> Result<()> {
        if sender.blocking_send(component).is_err() {
            // The async task receiving these entries has dropped the receiver, which is a sign
            // the extract task is aborted
            debug!("TarEntryComponent receiver task dropped the receiver; aborting blocking reader task");
            return crate::error::TarExtractAbortedSnafu {}.fail();
        }

        Ok(())
    }

    /// Async worker task that gets tar entry components from the reader task and performs upload
    /// of those components to object storage.
    ///
    /// Returns the total number of objects and total number of bytes uploaded for progress
    /// reporting by the caller, since this function can't tell if `entry_receiver` stopped
    /// producing components because there is no more work to to, or because of an error in the
    /// reader.
    #[instrument(skip(target_bucket, progress, entry_receiver), fields(target_bucket = target_bucket.name()))]
    async fn process_tar_entries(
        target_bucket: Box<dyn Bucket>,
        target_prefix: String,
        progress: Arc<dyn ExtractProgressCallback>,
        mut entry_receiver: mpsc::Receiver<TarEntryComponent>,
    ) -> Result<(usize, u64)> {
        // Keep processing entries until the sender is dropped
        let mut total_objects = 0usize;
        let mut total_object_bytes = 0u64;

        while let Some(tar_entry_component) = entry_receiver.recv().await {
            match tar_entry_component {
                TarEntryComponent::SmallFile { path, data } => {
                    let key = format!("{}{}", target_prefix, path.display());
                    let len = data.len();
                    debug!(path= %path.display(),len, %key, "Uploading small file");

                    progress.object_upload_starting(&key, len as u64);

                    target_bucket.put_small_object(key.clone(), data).await?;

                    progress.object_part_uploaded(&key, len);
                    progress.object_uploaded(&key, len as u64);
                    total_objects += 1;
                    total_object_bytes += len as u64;
                }
                TarEntryComponent::StartMultipartFile { path, len } => {
                    let key = format!("{}{}", target_prefix, path.display());
                    debug!(path = %path.display(), len, %key, "Starting multipart file upload");

                    progress.object_upload_starting(&key, len);

                    let (mut bytes_writer, mut progress_receiver, result_receiver) = target_bucket
                        .create_object_writer(key.clone(), Some(len))
                        .await?;

                    // Keep reading the entry receiver to get all of the parts of this file
                    let mut total_bytes_read = 0u64;

                    let loop_result = loop {
                        tokio::select! {
                            result = entry_receiver.recv() => {
                                let tar_entry_component = match result {
                                    None => {
                                        // This means there was an error reading from the tar
                                        // archive and the tar reader task exited prematurely,
                                        // dropping the sender.
                                        error!("Tar entry sender dropped prematurely; aborting processing loop");
                                        break crate::error::TarExtractAbortedSnafu{}.fail();
                                    },
                                    Some(tar_entry_component) => tar_entry_component
                                };

                                match tar_entry_component {
                                    TarEntryComponent::FilePart { data } => {
                                        debug!(path = %path.display(),len = data.len(), "Uploading file part");
                                        total_bytes_read += data.len() as u64;
                                        bytes_writer.write_all(&data).await.with_context(|_| crate::error::UploadWriterSnafu {})?;
                                    },
                                    TarEntryComponent::EndMultipartFile { data } => {
                                        debug!(path = %path.display(),len = data.len(), "Uploading final file part");
                                        total_bytes_read += data.len() as u64;
                                        assert_eq!(len, total_bytes_read,
                                            "BUG: File {} has size {} bytes but the tar reader sent only {} bytes",
                                                path.display(),
                                                len,
                                                total_bytes_read);

                                        bytes_writer.write_all(&data).await.with_context(|_| crate::error::UploadWriterSnafu {})?;

                                        break Ok(());
                                    },
                                    TarEntryComponent::SmallFile {..} | TarEntryComponent::StartMultipartFile { .. } =>  {
                                        // Neither of these should be sent while a multi-part
                                        // upload is still in process
                                        unreachable!();
                                    }
                                }
                            },
                            result = progress_receiver.recv() => {
                                if let Some(bytes_uploaded) = result {
                                    progress.object_part_uploaded(&key, bytes_uploaded);
                                }
                            },
                        }
                    };

                    // Signal to the upload task that no more data will be forthcoming
                    drop(bytes_writer);

                    // The upload task typically will keep running for at least a few hundred ms
                    // longer, and maybe several seconds.  Keep up with progress updates during
                    // that types.
                    while let Some(bytes_uploaded) = progress_receiver.recv().await {
                        progress.object_part_uploaded(&key, bytes_uploaded);
                    }

                    // Whether the loop exited with a success or error result, if there's an error
                    // result from the async worker that's doing upload, fail with that since it's
                    // likely a more meaningful explanation of what went wrong
                    match result_receiver.await {
                        Ok(Ok(total_bytes_uploaded)) => {
                            // Everything is good on the uploader side
                            assert_eq!(len, total_bytes_uploaded);
                            trace!(path = %path.display(), len, %key, "Async upload task succeeded");

                            progress.object_uploaded(&key, len);
                            total_objects += 1;
                            total_object_bytes += len;
                        }
                        Ok(Err(e)) => {
                            // The async upload task reported an error
                            error!(path = %path.display(), len, %key, "Async upload failed while performing multipart upload");

                            return Err(e);
                        }
                        Err(_) => {
                            // Something went wrong; the async upload task must have panicked
                            // because it dropped the sender before sending a result
                            return crate::error::AsyncObjectWriterPanicSnafu {}.fail();
                        }
                    }

                    if loop_result.is_ok() {
                        debug!(path = %path.display(), len, %key, "Finished multipart file upload");
                    } else {
                        error!(path = %path.display(), len, %key, "Multipart upload error; aborting processing task");
                    }

                    loop_result?;
                }
                TarEntryComponent::FilePart { .. } | TarEntryComponent::EndMultipartFile { .. } => {
                    // These components should never be sent other than following a
                    // StartMultipartFile so to see them here is a bug
                    unreachable!()
                }
            }
        }

        debug!("Entry sender dropped; no more tar entries to process");

        Ok((total_objects, total_object_bytes))
    }
}

/// An entry (meaning a file) in a `tar` archive which is being extracted, translated into one or
/// more components for more convenient processing.
///
/// Because tar files are read sequentially, there is not a lot of opportunity for concurrency in
/// the extracting of archive items (although there's plenty of concurrency reading the archive
/// data in chunks, and also in writing the objects to object storage after extraction).
///
/// Because of the sequential nature of tar archive processing, if we want to achieve any
/// concurrency we need to do it by processing the tar entries themselves in parallel.  To help us
/// do that, we break those entries up into multipart chunks at the source, the blocking task that
/// reads from the tar archive.  These multiple chunks can then be processed more or less in
/// parallel by the downstream processing stages.
enum TarEntryComponent {
    /// A small file in a tar archive which is small enough that there's no value in processing it
    /// in multiple parts.
    ///
    /// "small" here is defined as smaller than the configured multipart threshold specified in
    /// [`crate::Config`].
    SmallFile { path: PathBuf, data: Bytes },

    /// The start of a file that is large enough to be processed in multiple parts.
    ///
    /// This is guaranteed to be followed by at least one `FilePart`, and after all `FilePart`s
    /// exactly one `EndMultipartFile`.
    ///
    /// The threshold for treating a file as multipart is based on the multipart threshold in
    /// [`crate::Config`]
    StartMultipartFile { path: PathBuf, len: u64 },

    /// A part of a multipart file
    FilePart { data: Bytes },

    /// The final part of a multipart file
    EndMultipartFile { data: Bytes },
}
