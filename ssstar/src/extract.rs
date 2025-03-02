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
//! Notwithstanding the limitations of the (blocking) `tar` crate's API, parallelism is employed as
//! much as possible.  The reading of the input tar archive is performed in an async task (with
//! multiple reads in parallel if reading from a file or object storage), then that data is sent to
//! a blocking worker where `tar` is used to extract the archive.  Once `tar` has extracted part or
//! all of a file, another async task takes over to actually upload that data to S3.
//!
//! A single async task processes extracted small files in their entirety, and large files broken
//! up into chunks, and does so in parallel.  This means that there should be minimal performance
//! penalty when dealing with many small files compared to fewer large files in the archive.
use crate::objstore::{Bucket, MultipartUploader, ObjectStorageFactory};
use crate::tar::CountingReader;
use crate::{Config, Result};
use bytes::{Bytes, BytesMut};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use snafu::{IntoError, prelude::*};
use std::future::Future;
use std::io::{BufReader, Read};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{Instrument, debug, debug_span, error, info, info_span, instrument};
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
    Reader(Box<dyn Read + Send + Sync>),
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
    #[instrument(skip(config))]
    async fn into_internal(self, config: Config) -> Result<SourceArchiveInternal> {
        match self {
            Self::ObjectStorage(url) => {
                let objstore = ObjectStorageFactory::from_url(config, &url).await?;
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

    Reader(Box<dyn Read + Send + Sync>),
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
    ) -> Result<CountingReader<Box<dyn Read + Send + Sync>>> {
        // If we're reading from a local file, or from a stream (ie stdin) then we want to buffer
        // those reads, since the `tar` crate isn't necessarily going to do a bunch of large
        // sequential reads for optimal performance.
        //
        // Rather than expecting the user to set a separate tunable for this buffer size, we'll use
        // a hard-coded value that seems reasonable for now.
        //
        // TODO: does this need to be runtime configurable?  Is this value a reasonable default?
        let read_buffer_size = 256 * 1024;

        let reader: Box<dyn Read + Send + Sync> = match self {
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

                // Buffer reads from a file for better performance
                Box::new(BufReader::with_capacity(read_buffer_size, file))
            }
            SourceArchiveInternal::Reader(reader) => {
                // Especially when reading from an arbitrary stream (which is almost certainly
                // stdin) buffering is important

                Box::new(BufReader::with_capacity(read_buffer_size, reader))
            }
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

/// A parsed filter string which can be used to select a subset of the objects in the archive.
///
/// This is not meant to be created directly; instead use the [`FromStr`] impl to parse a
/// string into an extract filter
#[derive(Debug)]
pub enum ExtractFilter {
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
        } else if s.ends_with('/') {
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
            Self::Prefix { prefix } => path.to_string_lossy().starts_with(&**prefix),
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
    source_archive: SourceArchive,
    target_bucket: Box<dyn Bucket>,
    target_prefix: String,
    filters: Vec<ExtractFilter>,
}

impl ExtractArchiveJobBuilder {
    pub async fn new(config: Config, source: SourceArchive, target: Url) -> Result<Self> {
        let objstore = ObjectStorageFactory::from_url(config.clone(), &target).await?;

        let (bucket, key, _) = objstore.parse_url(&target).await?;

        Ok(Self {
            config,
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
            .into_internal(self.config.clone())
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
        duration: Duration,
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
    fn objects_uploaded(&self, total_objects: usize, total_object_bytes: u64, duration: Duration) {}
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
                let target_bucket = self.target_bucket.clone();

                tokio::task::spawn_blocking(move || {
                    let _guard = reader_span.enter();

                    debug!("Starting blocking tar read task");

                    match Self::read_tar_entries_blocking(
                        target_bucket,
                        self.filters,
                        reader,
                        progress,
                        entry_sender,
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
                        self.config.max_concurrent_requests,
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
            let (total_objects, total_object_bytes, elapsed) = match reader_result {
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

            progress.objects_uploaded(total_objects, total_object_bytes, elapsed);

            info!("Finished extract job");

            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Blocking worker task that iterates over the entries in the `tar` archive, and pushes them
    /// onto the MPSC queue where they will be processed by async code doing the extraction
    fn read_tar_entries_blocking(
        target_bucket: Box<dyn Bucket>,
        filters: Vec<ExtractFilter>,
        reader: CountingReader<Box<dyn Read + Send + Sync>>,
        progress: Arc<dyn ExtractProgressCallback>,
        entry_sender: mpsc::Sender<TarEntryComponent>,
    ) -> Result<()> {
        let mut archive = tar::Archive::new(reader);

        let mut extracted_objects = 0usize;
        let mut extracted_object_bytes = 0u64;
        let mut skipped_objects = 0usize;
        let mut skipped_object_bytes = 0u64;
        let started = Instant::now();

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
            } else if let Some(filter) = filters.iter().find(|filter| filter.matches(&path)) {
                debug!(?filter, "Filter matched so this entry is included");
                true
            } else {
                debug!("No filters matched, so this entry is excluded");
                false
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
            match target_bucket.partition_for_multipart_upload(&key, len)? {
                None => {
                    // This object is too small to bother with multi-part uploads so read the whole
                    // thing and send it to the processing task
                    // This file is small enough there's no value in processing it as multiple parts
                    debug!("Processing entry as a small file");
                    let data = Self::read_data(&mut entry, len as usize)?;

                    progress.extract_object_part_read(&key, len as usize);
                    progress.extract_object_finished(&key, len);
                    extracted_objects += 1;
                    extracted_object_bytes += len;

                    Self::send_component(
                        TarEntryComponent::SmallFile { path, data },
                        &entry_sender,
                    )?;
                }
                Some(parts) => {
                    // Object is large enough it should be uploaded via multi-part.  Read the
                    // object in multiple parts corresponding the the provided ranges and send them
                    // to processing one at a time.
                    debug!(num_parts = parts.len(), "Processing entry as multi-part");
                    Self::send_component(
                        TarEntryComponent::StartMultipartFile {
                            path,
                            len,
                            parts: parts.clone(),
                        },
                        &entry_sender,
                    )?;

                    for part in parts {
                        let len = part.end - part.start;

                        let data = Self::read_data(&mut entry, len as usize)?;
                        progress.extract_object_part_read(&key, data.len());

                        let component = TarEntryComponent::FilePart { part, data };

                        Self::send_component(component, &entry_sender)?;
                    }

                    Self::send_component(TarEntryComponent::EndMultipartFile, &entry_sender)?;

                    progress.extract_object_finished(&key, len);
                    extracted_objects += 1;
                    extracted_object_bytes += len;
                }
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
            started.elapsed(),
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
            debug!(
                "TarEntryComponent receiver task dropped the receiver; aborting blocking reader task"
            );
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
        max_concurrent_requests: usize,
        target_bucket: Box<dyn Bucket>,
        target_prefix: String,
        progress: Arc<dyn ExtractProgressCallback>,
        entry_receiver: mpsc::Receiver<TarEntryComponent>,
    ) -> Result<(usize, u64, Duration)> {
        // Keep processing entries until the sender is dropped

        // For each entry sent from the tar reader thread, create a future that processes that
        // entry (meaning uploads it, either single-part or multi-part).  The result will be a
        // `Stream<impl Future>`, which we can then call `buffered` on to ensure no more than the
        // max concurrent operations are running at a time.
        //
        // This perhaps awkward-seeming construction is important for performance.  If there are
        // many small files, we want to upload them in parallel.  If there are fewer, large files
        // that are elligible for multipart upload, we want to upload those parts in parallel.  If
        // there's a mix of both, we want to nonetheless ensure there are many parallel upload
        // operations running for maximum extract performance.
        let entry_receiver = tokio_stream::wrappers::ReceiverStream::new(entry_receiver);

        // State that needs to be kept as we process tar entry components
        struct State<InitFut, PartUploadFut> {
            current_uploader: Option<Box<dyn MultipartUploader>>,
            current_key: Option<String>,
            init_fut: Option<InitFut>,
            part_upload_futs: Vec<PartUploadFut>,
            last_multipart_uploaded: Option<Range<u64>>,
            total_objects: Arc<AtomicUsize>,
            total_object_bytes: Arc<AtomicU64>,
        }

        let total_objects = Arc::new(AtomicUsize::new(0));
        let total_object_bytes = Arc::new(AtomicU64::new(0));
        let started = Instant::now();

        let futs = entry_receiver.scan(
            State {
                current_uploader: None,
                current_key: None,
                init_fut: None,
                part_upload_futs: Vec::new(),
                last_multipart_uploaded: None,
                total_objects: total_objects.clone(),
                total_object_bytes: total_object_bytes.clone(),
            },
            move |state, tar_entry_component| {
                let progress = progress.clone();

                let fut = match tar_entry_component {
                    // A "small" file was read from tar, too small for multipart uploading.  So
                    // just upload it directly to S3.
                    TarEntryComponent::SmallFile { path, data } => {
                        let key = format!("{}{}", target_prefix, path.display());
                        let len = data.len();

                        let target_bucket = target_bucket.clone();
                        let total_objects = state.total_objects.clone();
                        let total_object_bytes = state.total_object_bytes.clone();

                        async move {
                            debug!(path= %path.display(),len, %key, "Uploading small file");

                            progress.object_upload_starting(&key, len as u64);

                            target_bucket.put_small_object(key.clone(), data).await?;

                            progress.object_part_uploaded(&key, len);
                            progress.object_uploaded(&key, len as u64);

                            total_objects.fetch_add(1, Ordering::SeqCst);
                            total_object_bytes.fetch_add(len as u64, Ordering::SeqCst);

                            Ok(())
                        }
                        .boxed()
                    }

                    TarEntryComponent::StartMultipartFile {
                        path,
                        len,
                        parts
                    } => {
                        // A file large enough that it should be a multi-part upload is being read.
                        let key = format!("{}{}", target_prefix, path.display());

                        let uploader = target_bucket.start_multipart_upload(key.clone(),
                            parts);

                        state.current_key = Some(key.clone());
                        state.current_uploader = Some(uploader.clone());
                        state.part_upload_futs = Vec::new();
                        state.last_multipart_uploaded = None;

                        // The future that starts the multi-part upload also needs to be clonable,
                        // because every single file part upload future needs to wait until this
                        // future is done so it can be sure it's safe to start uploading.
                        // The hack with `Arc::new` on error is there for the same reason as
                        // `FilePart`
                        let init_fut = async move {
                            debug!(path = %path.display(), len, %key, "Starting multipart file upload");

                            uploader.init().await?;

                            progress.object_upload_starting(&key, len);

                            Ok(())
                        }.map_err(|e| {
                            // We can't use the `shared` combinator unless the Ok and Err types are
                            // both clonable.
                            Arc::new(e)
                        }).shared();

                        state.init_fut = Some(init_fut.clone());

                        init_fut
                            .map_err(|e| {
                                crate::error::TarFileStartMultipartFileSnafu { }.into_error(e)
                            })
                            .boxed()
                    }

                    TarEntryComponent::FilePart { data, part } => {
                        // A part of the multipart file was read and should be sent to S3
                        let uploader = state
                            .current_uploader
                            .clone()
                            .expect("BUG: Got FilePart without StartMultipartFile");
                        let total_object_bytes = state.total_object_bytes.clone();
                        let key = state.current_key.clone().expect("BUG: Missing current_key");
                        let init_fut = state.init_fut.clone().expect("BUG: Missing init_fut");

                        // To ensure data integrity, ensure this is contiguous and non-overlapping with
                        // the last part, if any
                        match &state.last_multipart_uploaded {
                            Some(last_part) => {
                                assert_eq!(last_part.end, part.start);
                            }
                            None => {
                                assert_eq!(0, part.start);
                            }
                        }

                        assert_eq!(part.end - part.start, data.len() as u64);

                        state.last_multipart_uploaded = Some(part.clone());

                        // The futures that actually upload parts need to be cloneable, because we
                        // store a copy of each part's future in the `state` var.  This comes in
                        // handy below when processing the end of the multipart file.
                        let part_fut = async move {
                            // Make sure the future that initiatlizes multipart upload has
                            // completed.  If not, calling `upload_part` on the uploader will fail
                            init_fut.await
                                .map_err(|e| {
                                    crate::error::TarFileStartMultipartFileSnafu {}.into_error(e)
                                })?;

                            let len = data.len();
                            debug!(%key, ?part, len, "Uploading file part");
                            uploader.upload_part(part, data).await?;

                            progress.object_part_uploaded(&key, len);
                            total_object_bytes.fetch_add(len as u64, Ordering::SeqCst);

                            Ok(())
                        }.map_err(|e| {
                            // We can't use the `shared` combinator unless the Ok and Err types are
                            // both clonable.
                            Arc::new(e)
                        }).shared();

                        state.part_upload_futs.push(part_fut.clone());

                        // But the Arc<S3TarError> stored in `part_fut` won't fly here since all of
                        // the other match arms use a regular S3TarError error type.  We've
                        // introduced a special case for this
                        part_fut
                            .map_err(|e| {
                            crate::error::TarFilePartUploadSnafu { }.into_error(e)
                            })
                            .boxed()
                    }

                    TarEntryComponent::EndMultipartFile => {
                        let uploader = state
                            .current_uploader
                            .take()
                            .expect("BUG: Got FilePart without StartMultipartFile");
                        let last_part = state
                            .last_multipart_uploaded
                            .take()
                            .expect("BUG: Missing last_multipart_uploaded");
                        let key = state.current_key.clone().expect("BUG: Missing current_key");
                        let total_objects = state.total_objects.clone();
                        let part_futs = std::mem::take(&mut state.part_upload_futs);

                        async move {
                            debug!(%key, "Completing multi-part file upload");

                            // We can't complete the multi-part upload until all of the parts have
                            // uploaded successfully.  That's why we hold all of the part futs in
                            // the state variable.
                            //
                            // If there's actually an error uploading any of the parts, it's
                            // unlikely execution will make it this far since they are polled in
                            // order, and the failed part will be found before the failure reported
                            // by this call.  However it seems like a bad practice to just ignore
                            // an error here.
                            //
                            // The ugliness with `TarFilePartUploadSnafu` is necessary for the same
                            // reason it's used to wrap an error in the previous match arm.
                            futures::future::try_join_all(part_futs).await
                                .map_err(|e| {
                                    crate::error::TarFilePartUploadSnafu {}.into_error(e)
                                })?;

                            // Now all parts are uploaded, so we can finish the multi-part upload
                            uploader.finish().await?;

                            let len = last_part.end;

                            progress.object_uploaded(&key, len);
                            total_objects.fetch_add(1, Ordering::SeqCst);

                            Ok(())
                        }
                        .boxed()
                    }
                };

                // The `scan` combinator expects a future that returns an Option<T>, where T in our
                // case is the future that processes the component
                futures::future::ready(Some(fut))
            },
        );

        // XXX: This hack should not be required.  But if this isn't here, I get the following
        // compile error:
        //
        // error: higher-ranked lifetime error
        //   --> ssstar/src/extract.rs:513:17
        //    |
        //513 | /                 tokio::spawn(async move {
        //514 | |                     debug!("Starting tar entry processor task");
        //515 | |
        //516 | |                     match Self::process_tar_entries(
        //...   |
        //536 | |                     }
        //537 | |                 })
        //    | |__________________^
        //    |
        //    = note: could not prove `for<'r, 's> impl for<'r, 's> futures::Future<Output = std::result::Result<(usize, u64, std::time::Duration), S3TarError>>: std::marker::Send`
        //
        // This seems to be a Rust compiler bug: https://github.com/rust-lang/rust/issues/102211
        //
        // It is happening as of this writing with Rust 1.64.0.  Once this bug is fixed, the
        // following line can be safely removed
        let futs = futs.boxed();

        // Consume the stream of futures, polling only some of them at a time to control
        // concurrency
        let mut futs = futs.buffered(max_concurrent_requests);

        while let Some(()) = futs.try_next().await? {}

        debug!("Entry sender dropped; no more tar entries to process");

        Ok((
            total_objects.load(Ordering::SeqCst),
            total_object_bytes.load(Ordering::SeqCst),
            started.elapsed(),
        ))
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
    StartMultipartFile {
        path: PathBuf,
        len: u64,
        parts: Vec<Range<u64>>,
    },

    /// A part of a multipart file
    FilePart { part: Range<u64>, data: Bytes },

    /// Marks the end of a multipart file
    EndMultipartFile,
}
