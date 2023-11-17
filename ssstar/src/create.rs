//! Implementation of the operation which creates a tar archive from inputs stored in object
//! storage.
//!
//! Creation starts with creating a [`CreateArchiveJobBuilder`], then adding one or more inputs
//! with [`CreateArchiveJobBuilder::add_input`].  When [`CreateArchiveJobBuilder::build`] is
//! called, those inputs are evaluated against object store APIs and a complete list of objects to
//! archive is constructed in memory, and returned as part of [`CreateArchiveJob`].  The actual
//! creation of the archive is performed by calling [`CreateArchiveJob::run`].
//!
//! Archive creation is done in parallel to the extent possible.  All input objects are sorted by
//! timestamp, oldest to newest, and objects larger than the multipart threshold are broken up into
//! multiple parts equal to multipart chunk size.  The result is a list of either entire objects
//! (smaller than multipart threshold), or multiple object parts (if object is larger than
//! multipart threshold), then these are processed in parallel up to the max requests config
//! parameter.
//!
//! As these pieces are downloaded, they are fed to the archive writing stage (but they are fed in
//! the order in which they appear in the object, no in the order in which they download, which can
//! be in any random order depending on network conditions and non-deterministic scheduling of
//! tasks).
//!
//! The writing stage writes this data to the tar archive using the `tar` crate.  However the `tar`
//! crate works only with blocking I/O, so this stage runs as a tokio blocking task.
//!
//! The `tar` crate writes to a [`std::io::Write`] impl, which in turn translates those writes to
//! an async write on a tokio `DuplexStream`.  Another async task reads from that `DuplexStream`,
//! buffers the writes to fit into the multiplart chunk size for the target object storage, and
//! uploads the chunks as they are formed by the `tar` crate's writes.  These uploads are performed
//! in parallel up to the maximum request count, if the tar archive is to be written directly to
//! object storage.
//!
//! The archive can also be written to a file on disk or an arbitrary [`tokio::io::AsyncWrite`]
//! impl; in which case the process works as described other than the final stage that translates
//! `tar`'s writes into chunks for upload; for the other writes the writes are passed directly from
//! `tar` to the corresponding async writer.
//!
//! Throughout all of this, progress is reported to the caller via a caller-provided
//! [`CreateProgressCallback`] trait implementation.
use crate::objstore::{Bucket, ObjectStorageFactory};
use crate::tar::TarBuilderWrapper;
use crate::{Config, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use itertools::Itertools;
use snafu::prelude::*;
use std::fmt::Display;
use std::future::Future;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWrite;
use tokio::sync::oneshot;
use tracing::{debug, error, instrument};
use url::Url;

/// Represents where we will write the target archive
pub enum TargetArchive {
    /// Write the tar archive to object storage at the specified URL.
    ///
    /// The URL must specify a bucket and a complete object name.
    ObjectStorage(Url),

    /// Write the tar archive to the local filesystem
    File(PathBuf),

    /// Write the tar archive to some arbitrary [`tokio::io::AsyncWrite`] impl.
    Writer(Box<dyn AsyncWrite + Send + Unpin>),
}

impl std::fmt::Debug for TargetArchive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObjectStorage(url) => f.debug_tuple("ObjectStorage").field(url).finish(),
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
            Self::Writer(_) => f
                .debug_tuple("Writer")
                .field(&"dyn AsyncWrite".to_string())
                .finish(),
        }
    }
}

/// An input to a tar archive.
///
/// When creating a tar archive, the user can specify the objects to ingest in a few different
/// ways.  This type represents them all
#[derive(Clone, Debug)]
pub(crate) struct CreateArchiveInput {
    /// The bucket in which the objects are to be found
    bucket: Box<dyn Bucket>,

    /// The selector that describes which objects in the bucket to include in the archive
    selector: ObjectSelector,
}

impl Display for CreateArchiveInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Make this into a human-readable string for use in error messages
        match &self.selector {
            ObjectSelector::Bucket => {
                write!(f, "Bucket {}", self.bucket.name())
            }
            ObjectSelector::Object {
                key,
                version_id: None,
            } => write!(f, "Object '{key}' in bucket '{}'", self.bucket.name()),
            ObjectSelector::Object {
                key,
                version_id: Some(version_id),
            } => write!(
                f,
                "Object '{key}' (version '{version_id}') in bucket '{}'",
                self.bucket.name()
            ),
            ObjectSelector::Prefix { prefix } => write!(
                f,
                "Objects with prefix '{prefix}' in bucket '{}'",
                self.bucket.name()
            ),
            ObjectSelector::Glob { pattern } => write!(
                f,
                "Objects matching glob '{pattern}' in bucket '{}'",
                self.bucket.name()
            ),
        }
    }
}

impl CreateArchiveInput {
    /// Given the already-parsed bucket and key components of an input URL, determine
    /// what kind of input selector this is and return the corresponding value.
    ///
    /// The "key" here is everything after the `s3://bucket/` part of the URL.  It could be empty
    /// or contain a prefix or object name or glob.
    fn parse_key(bucket: Box<dyn Bucket>, key: Option<String>) -> Result<Self> {
        match key {
            None => {
                // There's nothing here just a bucket
                Ok(Self {
                    bucket,
                    selector: ObjectSelector::Bucket,
                })
            }
            Some(key) => {
                if key.contains('*') || key.contains('?') || key.contains('[') || key.contains(']')
                {
                    // It looks like there's a glob here.  The actual parsing of the glob needs to be done
                    // by the object store impl itself though
                    Ok(Self {
                        bucket,
                        selector: ObjectSelector::Glob { pattern: key },
                    })
                } else if key.ends_with('/') {
                    // Looks like a prefix
                    Ok(Self {
                        bucket,
                        selector: ObjectSelector::Prefix { prefix: key },
                    })
                } else {
                    // The only remaining possibility is that it's a single object key
                    Ok(Self {
                        bucket,

                        selector: ObjectSelector::Object {
                            key,

                            // For now this will always be None.
                            // TODO: How can the version ID be specified in the S3 URL?
                            version_id: None,
                        },
                    })
                }
            }
        }
    }

    /// It's the same function like [`Self::into_possible_input_objects`] but with one exception
    /// that if this object selector doesn't match any objects, an error is raised, since it likely
    /// indicates a mistake on the user's part.
    #[instrument(err, skip(self))]
    async fn into_input_objects(self) -> Result<Vec<InputObject>> {
        let input_text = self.to_string();
        let input_objects = self.into_possible_input_objects().await?;
        if input_objects.is_empty() {
            crate::error::SelectorMatchesNoObjectsSnafu { input: input_text }.fail()
        } else {
            Ok(input_objects)
        }
    }

    /// Evaluate the input against the actual object store API and return all objects that
    /// corresond to this input.
    ///
    /// This could be a long-running operation if a bucket or prefix is specified which contains
    /// hundreds of thousands or millions of objects.
    #[instrument(err, skip(self))]
    async fn into_possible_input_objects(self) -> Result<Vec<InputObject>> {
        // Enumerating objects is an object storage implementation-specific operation
        let input_text = self.to_string();
        debug!(self = %input_text, "Listing all object store objects that match this archive input");
        let input_objects = self.bucket.list_matching_objects(self.selector).await?;
        debug!(
            self = %input_text,
            count = input_objects.len(),
            "Listing matching objects completed"
        );
        Ok(input_objects)
    }
}

/// Selector which describes the objects to include in the archive, within a particular bucket.
#[derive(Clone, Debug)]
pub(crate) enum ObjectSelector {
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
    },

    /// All S3 objects in a bucket which have a common prefix.
    Prefix {
        /// The prefix to read.  All objects that have this prefix will be read.
        ///
        /// Prefixes must end with `/`, otherwise they are not treated as prefixes by the S3 API.
        /// Thus, this is guaranteed to end with "/"
        prefix: String,
    },

    /// All S3 objects in the bucket
    ///
    /// This means the user specified only the bucket and nothing else in the URL, ie
    /// `s3://mybucket/`.  The final trailing `/` is optional; with or without it such a URL will
    /// be treated as refering to the entire bucket.
    Bucket,

    /// A glob expression (using wildcards like `*` or `?`) which will be evaluated against all
    /// objects in the bucket, with matching objects being included
    Glob {
        /// The glob pattern to evaluate against all objects in the bucket
        pattern: String,
    },
}

/// A specific object in object storage which will be included in the archive.
///
/// By the time this struct is created, we already know this object exists and its metadata.
#[derive(Clone, Debug)]
pub(crate) struct InputObject {
    pub bucket: Box<dyn Bucket>,
    pub key: String,
    pub version_id: Option<String>,
    pub size: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl InputObject {
    /// Break up this object into parts which will be downloaded and processed separately, in
    /// parallel.
    ///
    /// The size and number of parts is controlled by the config.
    fn into_parts(self, config: &Config) -> Vec<InputObjectPart> {
        if self.size < config.multipart_threshold.get_bytes() as u64 {
            // This object is too small to bother with multipart
            vec![InputObjectPart {
                part_number: 0,
                byte_range: 0..self.size,
                input_object: Arc::new(self),
            }]
        } else {
            let me = Arc::new(self);
            let mut parts = Vec::with_capacity(
                ((me.size + config.multipart_chunk_size.get_bytes() as u64 - 1)
                    / config.multipart_chunk_size.get_bytes() as u64) as usize,
            );
            let mut part_number = 0;
            let mut byte_offset = 0u64;

            while byte_offset < me.size {
                let byte_length =
                    (config.multipart_chunk_size.get_bytes() as u64).min(me.size - byte_offset);

                parts.push(InputObjectPart {
                    input_object: me.clone(),
                    part_number,
                    byte_range: byte_offset..(byte_offset + byte_length),
                });

                part_number += 1;
                byte_offset += byte_length;
            }

            parts
        }
    }
}

struct InputObjectPart {
    input_object: Arc<InputObject>,
    part_number: usize,
    byte_range: Range<u64>,
}

#[derive(Debug)]
pub struct CreateArchiveJobBuilder {
    config: Config,
    target: TargetArchive,
    inputs: Vec<CreateArchiveInput>,
    allow_empty: bool,
}

impl CreateArchiveJobBuilder {
    /// Initialize a new create archive job builder, but don't yet start the job.
    pub fn new(config: Config, target: TargetArchive) -> Self {
        Self {
            config,
            target,
            inputs: vec![],
            allow_empty: false,
        }
    }

    /// Set a flag to indicate whether job can be created without objects.
    /// By default it is `false`.
    pub fn allow_empty(&mut self, allow: bool) {
        self.allow_empty = allow;
    }

    /// Add one input URL to the job, validating the URL as part of the process.
    ///
    /// Before adding this input, the URL will be parsed to extract the bucket name and object key,
    /// then the object storage API will be queried to verify that the bucket is valid and
    /// accessible, and metadata about the input will be gathered.
    pub async fn add_input(&mut self, input: &Url) -> Result<()> {
        debug!(url = %input, "Adding archive input");

        // From the URL determine what object storage provider to use for this particular input
        let objstore = ObjectStorageFactory::from_url(self.config.clone(), input).await?;

        // Validate the bucket and extract it from the URL
        let (bucket, key, _version_id) = objstore.parse_url(input).await?;
        debug!(url = %input, ?bucket, "Confirmed bucket access for input");

        // Parse the path component of the URL into an archive input
        let input = CreateArchiveInput::parse_key(bucket, key)?;

        debug!(?input, "Adding archive input to job");

        self.inputs.push(input);

        Ok(())
    }

    /// Construct the actual archive creation job (but don't run it yet).
    ///
    /// This is a potentially long-running process, depending upon how many input objects there are
    /// for this job.  If the caller specified a prefix or an entire bucket with a lot of objects,
    /// it could take several seconds or conceivably even minutes to enumerate all of the objects.
    ///
    /// As part of the construction of the job, if the `target` specifies an object storage URL,
    /// accessibility of the bucket will be verified by calling the object storage API prior to
    /// returning.
    pub async fn build(self) -> Result<CreateArchiveJob> {
        // Expand all of the inputs into a concrete list of matching object store objects.
        //
        // This can be done in parallel for maximum winning
        debug!(
            input_count = self.inputs.len(),
            "Listing objects for all inputs"
        );

        let mut inputs = if self.allow_empty {
            let input_futs = self
                .inputs
                .into_iter()
                .map(move |input| input.into_possible_input_objects());
            futures::future::try_join_all(input_futs).await?
        } else {
            let input_futs = self
                .inputs
                .into_iter()
                .map(move |input| input.into_input_objects());
            futures::future::try_join_all(input_futs).await?
        }
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

        debug!(
            object_count = inputs.len(),
            "Listed all objects in all inputs"
        );

        // Sort all objects by their timestamp, from oldest to newest.  This is critical for best
        // performance when the archive is to be ingested by Elastio repeatedly.  We want objects
        // that existed the last time this archive was created to exist in the same place in the
        // stream, so that our dedupe will be maximally effective.
        inputs.sort_unstable_by_key(|input_object| input_object.timestamp);

        // Now dedupe the input objects, in case multiple archive inputs matched the same object.
        // It obviously makes no sense to include the same object twice in the archive.
        let inputs = inputs
            .into_iter()
            .dedup_by(|x, y| {
                x.bucket.name() == y.bucket.name() && x.key == y.key && x.version_id == y.version_id
            })
            .collect::<Vec<_>>();

        Ok(CreateArchiveJob {
            config: self.config,
            target: self.target,
            inputs,
            allow_empty: self.allow_empty,
        })
    }
}

/// A trait which callers can implement to get detailed progress updates as archive creation is
/// progressing.
#[allow(unused_variables)]
pub trait CreateProgressCallback: Sync + Send {
    /// The process of downloading all of the input objects is about to start
    fn input_objects_download_starting(&self, total_objects: usize, total_bytes: u64) {}

    /// The download of a new input object is starting.
    ///
    /// In truth downloads happen in parallel, but they are yielded in precisely the order they
    /// will appear in the tar archive, so for progress reporting purposes when the first part of
    /// an input object is available for writing to the tar archive, we say the download of that
    /// object has started.
    fn input_object_download_started(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: u64,
    ) {
    }

    /// Part of the data of one of the input objects was downloaded from object storage.
    ///
    /// Unlike the started and completed events, which are generated from an ordered sequence of
    /// input object part downloads, this event is reported as soon as the bytes come down the wire
    /// for a part.  Because of how concurrent tasks are executed, it's possible that part 10 of an
    /// object downloads first, before part 0.  This event fires as soon as part 10 has finished
    /// downloading, but the started event won't fire until part 0 finishes.
    ///
    /// For an event that is guaranteed to fire after `started` and before `completed`, see
    /// `input_part_downloaded`
    fn input_part_unordered_downloaded(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        part_number: usize,
        part_size: usize,
    ) {
    }

    /// Part of the data of one of the input objects was downloaded from object storage.
    ///
    /// unlike `input_part_unordered_downloaded`, this event doesn't fire the moment the part is
    /// downloaded over the wire, but only when that part becomes available on the stream of input
    /// object parts in order.  For rendering a progress bar of the progress of downloading an
    /// object, this is what you want, but it might be the case that this part actually was
    /// downloaded into memory several seconds ago, or possibly longer for slow networks.
    fn input_part_downloaded(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        part_number: usize,
        part_size: usize,
    ) {
    }

    /// An entire input object has been downloaded successfully
    fn input_object_download_completed(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: u64,
    ) {
    }

    /// All input objects have now been downloaded.
    ///
    /// That doesn't mean the work is done; there can still be ongoing tasks either writing some of
    /// that downloaded data to the tar builder, or uploading writes to the tar archive to object
    /// storage.
    fn input_objects_download_completed(&self, total_bytes: u64, duration: Duration) {}

    /// The tar archive has been initialized but not yet written to.
    ///
    /// The `total_*` args refer to the number and total size of all input objects which are to be
    /// written to this archive.  It's hard to predict the actual tar archive size without getting
    /// into the weeds of the tar archive format, but an estimated size is provided to help scale
    /// progress bars in the UI.
    fn archive_initialized(
        &self,
        total_objects: usize,
        total_bytes: u64,
        estimated_archive_size: u64,
    ) {
    }

    /// Part of the data of one of the input objects has been written to the tar archive
    ///
    /// That doesn't mean the data written has been uploaded to remote object storage yet, it could
    /// still be buffered locally.
    fn archive_part_written(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        part_number: usize,
        part_size: usize,
    ) {
    }

    /// An entire input object was written successfully to a tar archive.
    ///
    /// `byte_offset` is the byte offset in the archive stream where the data for this object
    /// starts.  Data is guaranteed to be stored in one contiguous sequence, therefore it's
    /// possible to find the data for this object from the `byte_offset` and `size` parameters.
    ///
    /// That doesn't mean the data written has been uploaded to remote object storage yet, it could
    /// still be buffered locally.
    fn archive_object_written(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        timestamp: DateTime<Utc>,
        byte_offset: u64,
        size: u64,
    ) {
    }

    /// The tar builder has written some bytes to the [`std::io::Write`] instance that it's layered
    /// on top of.
    ///
    /// This event happens after a `tar_archive_part_written` event and sees the total number of
    /// bytes written, including any tar headers.
    ///
    /// ## Notes
    ///
    /// This event can be reported from a synchronous context, because it's captured at the level
    /// of the [`std::io::Write`] implementation itself.
    fn archive_bytes_written(&self, bytes_written: usize) {}

    /// The tar archive has been completed, and will see no further writes.
    ///
    /// There may still be upload activity in process, uploading previous tar writes to object
    /// storage from a local buffer.
    fn archive_writes_completed(&self, total_bytes_written: u64) {}

    /// Some bytes have been uploaded to the tar archive in object storage.
    ///
    /// `bytes_uploaded` is not a total; it's the amount of bytes uploaded just now by the caller.
    /// The receiver of this event will need to maintain a running total if one is desired.
    ///
    /// If the tar archive is not being directed to object storage, then this event will never fire
    fn archive_bytes_uploaded(&self, bytes_uploaded: usize) {}

    /// The tar archive's previously completed writes have all been flushed from their buffers and
    /// uploaded to object storage (or a file or a stream dependng on where the tar archive is
    /// located).
    ///
    /// This is the final event that can happen.  Once this event fires, the job is done.
    ///
    /// If the tar archive is not being directed to object storage, then this event will never fire
    fn archive_upload_completed(&self, size: u64, duration: Duration) {}
}

/// A job which will create a new tar archive from object store inputs.
#[derive(Debug)]
pub struct CreateArchiveJob {
    config: Config,
    target: TargetArchive,
    inputs: Vec<InputObject>,
    allow_empty: bool,
}

impl CreateArchiveJob {
    /// The total number of bytes to read from all objects
    pub fn total_bytes(&self) -> u64 {
        self.inputs.iter().map(|input| input.size).sum()
    }

    /// The total number of objects included in this archive
    pub fn total_objects(&self) -> usize {
        self.inputs.len()
    }

    /// Alternative to [`Self::run`] which doesn't require a [`ProgressCallback`] implementation,
    /// for callers that do not care about progress information.
    pub async fn run_without_progress(self, abort: impl Future<Output = ()>) -> Result<()> {
        // A dummy impl of ProgressCallback that doesn't do anything with any of the progress
        // updates
        struct NoProgress {}
        impl CreateProgressCallback for NoProgress {}

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
        Progress: CreateProgressCallback + 'static,
    {
        let progress: Arc<dyn CreateProgressCallback> = Arc::new(progress);
        let total_bytes = self.total_bytes();
        let total_objects = self.total_objects();

        progress.input_objects_download_starting(total_objects, total_bytes);

        // There must be at least one object otherwise it doesn't make sense to proceed
        if total_objects == 0 && !self.allow_empty {
            return crate::error::NoInputsSnafu {}.fail();
        }

        // Estimate the size of the tar archive we're going to create.  It will be the
        // combined size of all objects in the archive, plus approx 512 bytes of overhead
        // per object
        let approx_archive_size = total_bytes + (total_objects as u64 * 512);

        // Construct a writer for the target archive.
        //
        // This is made complex by the fact that we want to use the same code whether the target
        // archive is itself in object storage, or a file on disk, or an arbitrary stream (like
        // stdout or stderr).  Of those, object storage is a special case because it's not enough
        // to just get an `AsyncWrite` instance, we also need a way to monitor the background task
        // that does the actual uploading to S3, and at the end to block until all queued S3
        // uploads actually finish.
        //
        // So when you find yourself wondering "OMG why all this complexity it's just a writer",
        // it's because we allow to write the tar archive directly back to object storage.  Is that
        // a capbility so useful that it's worth the hassle?  I think so.
        let (writer, result_receiver): (
            Box<dyn AsyncWrite + Send + Unpin>,
            Option<oneshot::Receiver<Result<u64>>>,
        ) = match self.target {
            TargetArchive::ObjectStorage(url) => {
                // Validate the URL and get the components of the URL in the bargain
                let objstore = ObjectStorageFactory::from_url(self.config.clone(), &url).await?;

                let (bucket, key, _) = objstore.parse_url(&url).await?;

                // The key is a required component here
                let key = key.ok_or_else(|| {
                    crate::error::ArchiveUrlInvalidSnafu { url: url.clone() }.build()
                })?;

                // Create a writer that will upload all written data to this object
                let (bytes_writer, mut progress_receiver, result_receiver) = bucket
                    .create_object_writer(key, Some(approx_archive_size))
                    .await?;

                // Make a background task that will pull updates from the process stream and post
                // them to the progress callback
                let progress = progress.clone();

                tokio::spawn(async move {
                    while let Some(bytes_uploaded) = progress_receiver.recv().await {
                        progress.archive_bytes_uploaded(bytes_uploaded);
                    }
                });

                (Box::new(bytes_writer), Some(result_receiver))
            }
            TargetArchive::File(path) => {
                let bytes_writer = tokio::fs::File::create(&path)
                    .await
                    .with_context(|_| crate::error::WritingArchiveFileSnafu { path })?;

                (Box::new(bytes_writer), None)
            }
            TargetArchive::Writer(writer) => (writer, None),
        };

        // Create the tar archive itself
        let blocking_writer = crate::async_bridge::async_write_as_writer(writer);
        let tar_builder = TarBuilderWrapper::new(blocking_writer, progress.clone());

        progress.archive_initialized(total_objects, total_bytes, approx_archive_size);

        // Break up the input objects into one or more individual tasks (so we can use multipart
        // download for the large objects), which we will then evaluate in parallel.
        #[allow(clippy::needless_collect)] // collect is needed to avoid lifetime issues with `self`
        let parts = self
            .inputs
            .into_iter()
            .flat_map(|input_object| input_object.into_parts(&self.config))
            .collect::<Vec<_>>();

        // For every one of the parts of the input objects, make a separate future that will handle
        // downloading that part
        let progress_clone = progress.clone();
        let part_futs = parts.into_iter().map(move |part| {
            let progress = progress_clone.clone();

            async move {
                let data = part
                    .input_object
                    .bucket
                    .read_object_part(
                        part.input_object.key.clone(),
                        part.input_object.version_id.clone(),
                        part.byte_range.clone(),
                    )
                    .await?;

                progress.input_part_unordered_downloaded(
                    part.input_object.bucket.name(),
                    &part.input_object.key,
                    part.input_object.version_id.as_deref(),
                    part.part_number,
                    (part.byte_range.end - part.byte_range.start) as usize,
                );

                Ok((part, data))
            }
        });

        // Turn these futures into a Stream that yields each input part as it is downloaded.
        // The `buffered` function does a lot of heavy lifting; it polls the specified number of
        // futures, yielding them in the order in which they appear in the stream.  This is how we
        // control the concurrency level, since only polled futures actually execute.
        let mut parts_stream =
            futures::stream::iter(part_futs).buffered(self.config.max_concurrent_requests);

        // The stream `parts_stream` will only poll the futures when we call `next()` on it.  But
        // that's not really what we want.  We want the part download futures to be working
        // constantly, yielding downloaded parts in order.  So we need to spawn another async task
        // whose job is to constantly poll the stream, and push completed parts into a tokio
        // channel which will buffer the completed parts, so that the code below will always have a
        // part to work with.
        let (parts_sender, mut parts_receiver) =
            tokio::sync::mpsc::channel(self.config.max_concurrent_requests);
        {
            let progress = progress.clone();

            tokio::spawn(async move {
                let mut total_bytes_downloaded = 0u64;
                let input_objects_download_started = Instant::now();

                while let Some(result) = parts_stream.next().await {
                    // Part downloads are yield from the stream in the order in which they appeared,
                    // which means all parts for an object proceed from 0 to the last one, in order.
                    // If the download of an entire object has completed, report it here
                    if let Ok((part, _data)) = &result {
                        total_bytes_downloaded += part.byte_range.end - part.byte_range.start;

                        if part.part_number == 0 {
                            // This is the start of an input object download
                            progress.input_object_download_started(
                                part.input_object.bucket.name(),
                                &part.input_object.key,
                                part.input_object.version_id.as_deref(),
                                part.input_object.size,
                            );
                        }

                        progress.input_part_downloaded(
                            part.input_object.bucket.name(),
                            &part.input_object.key,
                            part.input_object.version_id.as_deref(),
                            part.part_number,
                            (part.byte_range.end - part.byte_range.start) as usize,
                        );

                        if part.byte_range.end == part.input_object.size {
                            // This is the final part
                            progress.input_object_download_completed(
                                part.input_object.bucket.name(),
                                &part.input_object.key,
                                part.input_object.version_id.as_deref(),
                                part.input_object.size,
                            );
                        }
                    }

                    if (parts_sender.send(result).await).is_err() {
                        // The receiver was dropped, which probably means an error ocurred and we are
                        // no longer processing parts, so nothing further to do
                        debug!("parts channel is closed; aborting feeder task");
                        break;
                    }
                }

                if !parts_sender.is_closed() {
                    progress.input_objects_download_completed(
                        total_bytes_downloaded,
                        input_objects_download_started.elapsed(),
                    );
                }
            });
        }

        // Process the completed input object parts and write them to the tar archive.
        // For what should be obvious reasons, the writing of data to the tar archive must be done
        // serially, even though we downloaded the data in parallel, and the stream that the tar
        // Builder writes to will upload the written data in paralell also.

        // To keep track of when the writes to the tar archive started.  Technically what we
        // actually want to know is when the *upload* to S3 starts, which can be a bit after the
        // writes start since the write buffer needs to fill up first.  However this is close
        // enough.
        let mut tar_archive_writes_started: Option<Instant> = None;

        loop {
            // The next part must be part 0 of a new object
            match parts_receiver.recv().await {
                None => {
                    // All parts have been read.  We're done.
                    debug!("Completed processing of all input objects");
                    break;
                }

                Some(result) => {
                    // The first part of a new object has been downloaded
                    let (mut part, data) = result?;

                    assert_eq!(0, part.part_number, "BUG: the parts are completing out of order or there's a logic error in this loop");

                    debug!(key = %part.input_object.key, size = part.input_object.size, "Reading object and writing to tar archive");

                    // This is a new object which means we need to write it to the tar archive.
                    // But we only have the first part, how can we write to tar now?  Don't we need
                    // to buffer in memory until the whole object is read?
                    //
                    // NO! `tar::Builder::append_data` takes a `Read` instance which it will use to read the data
                    // before writing it to the tar stream.  Here we'll construct just such a
                    // `Read` impl, which is fed by a tokio channel receiver.  The other end of
                    // that receiver is held by us, polling the `parts_stream` and feeding each of
                    // the parts we read into the channel.  These things can happen in parallel
                    // because the call to `append_data` is blocking, so anyway it needs to be run
                    // in a separate async task by `spawn_blocking`.
                    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<bytes::Bytes>>(1);
                    let blocking_reader = crate::async_bridge::stream_as_reader(
                        tokio_stream::wrappers::ReceiverStream::new(receiver),
                    );
                    let tar_builder = tar_builder.clone();
                    let mut header = tar::Header::new_gnu();
                    header.set_size(part.input_object.size);
                    // TODO: translate the object timestamp into a UNIX timestamp and set mtime
                    // here
                    header.set_cksum();

                    let object_path = part.input_object.key.clone();
                    let append_fut =
                        tar_builder.spawn_append_data(header, object_path, blocking_reader);

                    // The `append_data` call is now running in a worker thread.  It will keep
                    // running until the `blocking_reader`'s `read` method indicates EOF, and that
                    // won't happen until this `sender` gets dropped.  So process all of the parts
                    // for this input object now, writing them into the channel which will
                    // ultimately yield them to `blocking_reader` and thus to the `append_data`
                    // method running in the worker thread.
                    //
                    // If for some reason there's an error in the `append_data` operation, which
                    // could happen if there's a problem reading data or a problem writing to the
                    // tar writer, then the append future might exit early, dropping the receiver
                    // and causing `sender.send` to fail.  If that happens, the actual cause of the
                    // error isn't in the error from `send`, but in th eresult of `append_fut`.  So
                    // a failure to write to the sender is a sign we should stop what we're doing
                    // and wait for the appender to stop
                    let mut appender_aborted = false;
                    if (sender.send(Ok(data)).await).is_ok() {
                        if tar_archive_writes_started.is_none() {
                            // Record this instant when writes first started
                            tar_archive_writes_started = Some(Instant::now());
                        }

                        progress.archive_part_written(
                            part.input_object.bucket.name(),
                            &part.input_object.key,
                            part.input_object.version_id.as_deref(),
                            part.part_number,
                            (part.byte_range.end - part.byte_range.start) as usize,
                        );

                        while part.byte_range.end < part.input_object.size {
                            let (next_part, data) =
                                parts_receiver.recv().await.unwrap_or_else(|| {
                                    panic!(
                                        "BUG: stream ended prematurely after part {} of input {}",
                                        part.part_number, part.input_object.key
                                    )
                                })?;

                            assert_eq!(next_part.input_object.key, part.input_object.key);
                            assert_eq!(next_part.part_number, part.part_number + 1);
                            assert_eq!(next_part.byte_range.start, part.byte_range.end);

                            if (sender.send(Ok(data)).await).is_err() {
                                appender_aborted = true;
                                break;
                            } else {
                                progress.archive_part_written(
                                    next_part.input_object.bucket.name(),
                                    &next_part.input_object.key,
                                    next_part.input_object.version_id.as_deref(),
                                    next_part.part_number,
                                    (next_part.byte_range.end - next_part.byte_range.start)
                                        as usize,
                                );
                            }

                            part = next_part;
                        }
                    } else {
                        appender_aborted = true;
                    }

                    // All of the data for this object has been passed to the `blocking_reader`
                    // that `append_data` is reading.  Now we just wait for it to finish
                    drop(sender);
                    let data_range = append_fut.await?;

                    assert_eq!(part.input_object.size, data_range.end - data_range.start,
                               "BUG: reported data range doesn't match the expected size of the object's data");

                    // Normally this is what we want to hear, but if the appender dropped
                    // the channel that means we weren't able to send all of the parts to
                    // the tar appender task so we expect a failure here
                    assert!(
                        !appender_aborted,
                        "BUG: data channel for writing to tar archive was closed without any error"
                    );

                    progress.archive_object_written(
                        part.input_object.bucket.name(),
                        &part.input_object.key,
                        part.input_object.version_id.as_deref(),
                        part.input_object.timestamp,
                        data_range.start,
                        part.input_object.size,
                    );

                    debug!(key = %part.input_object.key,
                        size = part.input_object.size,
                        final_part_number = part.part_number,
                        "Streamed object data into tar archive");
                }
            }
        }

        // Finalize the tar archive
        let bytes_written = tar_builder.finish_and_close().await?;

        progress.archive_writes_completed(bytes_written);

        // If the target archive was also an object on object storage, then there's still a
        // background async task that is taking the bytes written by the tar Builder, buffering
        // them into chunks equal to the multipart chunk size, and uploading them in parallel.
        // Obviously we're not actually done until this has completed successfully.
        if let Some(result_receiver) = result_receiver {
            match result_receiver.await {
                Ok(Ok(bytes_written)) => {
                    debug!(
                        bytes_written,
                        "Upload of tar archive to object storage completed"
                    );
                    let elapsed = tar_archive_writes_started
                        .expect("BUG: is set unconditionally during tar writes")
                        .elapsed();
                    progress.archive_upload_completed(bytes_written, elapsed);

                    Ok(())
                }
                Ok(Err(error)) => {
                    // The async task that uploads multipart chunks to object storage failed with
                    // some kind of error.
                    // TODO: if that task actually failed, we usually won't see it here, because the
                    // `Write` impl passed to `tar::Builder` will have failed a write operation, so
                    // the `spawn_append_data` future will have failed, probably with some generic
                    // IO error like BrokenPipe.  We should handle that failure and, before
                    // returning whatever error that future fails with, attempt to pull a result
                    // from this receiver since that will be the actual reason for the failure.
                    error!(?error, "The async upload task which uploads the tar archive to object storage reported an error");
                    Err(error)
                }
                Err(_) => {
                    // The sender that matches this receiver was dropped.  That can only be due to
                    // a panic in the worker task
                    crate::error::AsyncTarWriterPanicSnafu {}.fail()
                }
            }
        } else {
            // Playing on easy mode, the tar archive is written locally so the `flush()` done in
            // `finish_and_close` is all we need
            Ok(())
        }
    }
}
