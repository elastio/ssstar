//! Implementation of the operation which creates a tar archive from inputs stored in object
//! storage.
use crate::objstore::{Bucket, ObjectStorage, ObjectStorageFactory};
use crate::tar::TarBuilderWrapper;
use crate::{Config, Result, S3TarError};
use futures::StreamExt;
use itertools::Itertools;
use snafu::prelude::*;
use std::future::Future;
use std::io::Write;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWrite;
use tracing::{debug, instrument};
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

impl CreateArchiveInput {
    /// Given the already-parsed bucket component of an input URL, and the path part, determine
    /// what kind of input selector this is and return the corresponding value.
    ///
    /// The "path" here is everything after the `s3://bucket/` part of the URL.  It could be empty
    /// or contain a prefix or object name or glob.
    fn parse_path(bucket: Box<dyn Bucket>, path: &str) -> Result<Self> {
        if path.is_empty() || path == "/" {
            // There's nothing here just a bucket
            Ok(Self {
                bucket,
                selector: ObjectSelector::Bucket,
            })
        } else if path.contains('*')
            || path.contains('?')
            || path.contains('[')
            || path.contains(']')
        {
            // It looks like there's a glob here.  The actual parsing of the glob needs to be done
            // by the object store impl itself though
            Ok(Self {
                bucket,
                selector: ObjectSelector::Glob {
                    pattern: path.to_string(),
                },
            })
        } else if path.ends_with('/') {
            // Looks like a prefix
            Ok(Self {
                bucket,
                selector: ObjectSelector::Prefix {
                    prefix: path.to_string(),
                },
            })
        } else {
            // The only remaining possibility is that it's a single object key
            Ok(Self {
                bucket,

                selector: ObjectSelector::Object {
                    key: path.to_string(),

                    // For now this will always be None.
                    // TODO: How can the version ID be specified in the S3 URL?
                    version_id: None,
                },
            })
        }
    }

    /// Evaluate the input against the actual object store API and return all objects that
    /// corresond to this input.
    ///
    /// This could be a long-running operation if a bucket or prefix is specified which contains
    /// hundreds of thousands or millions of objects.  Note that when using a glob, all objects in
    /// the bucket are enumerated even if the glob pattern itself has a constant prefix.
    #[instrument(err)]
    async fn into_input_objects(self) -> Result<Vec<InputObject>> {
        // Enumerating objects is an object storage implementation-specific operation
        debug!("Listing all object store objects that match this archive input");
        let input_objects = self.bucket.list_matching_objects(self.selector).await?;
        debug!(
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
    objstore_factory: Arc<ObjectStorageFactory>,
    target: TargetArchive,
    inputs: Vec<CreateArchiveInput>,
}

impl CreateArchiveJobBuilder {
    /// Initialize a new create archive job builder, but don't yet start the job.
    pub fn new(config: Config, target: TargetArchive) -> Self {
        Self {
            objstore_factory: ObjectStorageFactory::instance(config.clone()),
            config,
            target,
            inputs: vec![],
        }
    }

    /// Add one input URL to the job, validating the URL as part of the process.
    ///
    /// Before adding this input, the URL will be parsed to extract the bucket name and object key,
    /// then the object storage API will be queried to verify that the bucket is valid and
    /// accessible, and metadata about the input will be gathered.
    pub async fn add_input(&mut self, input: &Url) -> Result<()> {
        debug!(url = %input, "Adding archive input");

        // From the URL determine what object storage provider to use for this particular input
        let objstore = self.objstore_factory.from_url(input).await?;

        // Validate the bucket and extract it from the URL
        let bucket = objstore.extract_bucket_from_url(input).await?;
        debug!(url = %input, ?bucket, "Confirmed bucket access for input");

        // Parse the path component of the URL into an archive input
        let input = CreateArchiveInput::parse_path(bucket, input.path())?;

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

        let input_futs = self
            .inputs
            .into_iter()
            .map(move |input| input.into_input_objects());

        let mut inputs = futures::future::try_join_all(input_futs)
            .await?
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
            objstore_factory: self.objstore_factory,
            target: self.target,
            inputs,
        })
    }
}

/// A trait which callers can implement to get detailed progress updates as archive creation is
/// progressing.
pub trait ProgressCallback: Sync + Send {
    /// Part of the data of one of the input objects was downloaded from object storage, and is
    /// about to be written to the tar archive
    fn input_part_downloaded(
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
    /// That doesn't mean the data written has been uploaded to remote object storage yet, it could
    /// still be buffered locally.
    fn input_object_written(&self, bucket: &str, key: &str, version_id: Option<&str>, size: u64) {}
}

/// A job which will create a new tar archive from object store inputs.
#[derive(Debug)]
pub struct CreateArchiveJob {
    config: Config,
    objstore_factory: Arc<ObjectStorageFactory>,
    target: TargetArchive,
    inputs: Vec<InputObject>,
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
        impl ProgressCallback for NoProgress {}

        self.run(abort, NoProgress {}).await
    }

    /// Run the job, returning only when the job has run to completion (or failed)
    ///
    /// If the `abort` future is completed, it's a signal that the job should be aborted.
    /// Existing transfers will be abandoned and queued transfers will be dropped, then this method
    /// returns an abort error.
    pub async fn run<Abort, Progress>(self, abort: Abort, progress: Progress) -> Result<()>
    where
        Abort: Future<Output = ()>,
        Progress: ProgressCallback + 'static,
    {
        let total_bytes = self.total_bytes();
        let total_objects = self.total_objects();

        // Construct a writer for the target archive.
        let writer: Box<dyn AsyncWrite + Send + Unpin> = match self.target {
            TargetArchive::ObjectStorage(url) => {
                // Validate the URL and get a Bucket object in the bargain
                let objstore = self.objstore_factory.from_url(&url).await?;
                let bucket = objstore.extract_bucket_from_url(&url).await?;

                // Estimate the size of the tar archive we're going to create.  It will be the
                // combined size of all objects in the archive, plus approx 512 bytes of overhead
                // per object
                let approx_archive_size = total_bytes + (total_objects as u64 * 512);

                // Create a writer that will upload all written data to this object
                Box::new(
                    bucket
                        .create_object_writer(url.path().to_string(), Some(approx_archive_size))
                        .await?,
                )
            }
            TargetArchive::File(path) => Box::new(
                tokio::fs::File::create(&path)
                    .await
                    .with_context(|_| crate::error::WritingArchiveFileSnafu { path })?,
            ),
            TargetArchive::Writer(writer) => writer,
        };

        // Create the tar archive itself
        let blocking_writer = crate::async_bridge::async_write_as_writer(writer);
        // This mutex isn't actually needed but we operate on this tar builder in a spawned
        // blocking task and the Rust compiler can't tell that we never access this builder from
        // multiple threads at the same time.
        let tar_builder = TarBuilderWrapper::new(tar::Builder::new(blocking_writer));

        // Break up the input objects into one or more individual tasks (so we can use multipart
        // download for the large objects), which we will then evaluate in parallel.
        let parts = self
            .inputs
            .into_iter()
            .flat_map(|input_object| input_object.into_parts(&self.config))
            .collect::<Vec<_>>();

        // For every one of the parts of the input objects, make a separate future that will handle
        // downloading that part
        let progress = Arc::new(progress);
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

                progress.input_part_downloaded(
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
        tokio::spawn(async move {
            while let Some(result) = parts_stream.next().await {
                if let Err(_) = parts_sender.send(result).await {
                    // The receiver was dropped, which probably means an error ocurred and we are
                    // no longer processing parts, so nothing further to do
                    debug!("parts channel is closed; aborting feeder task");
                    break;
                }
            }
        });

        // Process the completed input object parts and write them to the tar archive.
        // For what should be obvious reasons, the writing of data to the tar archive must be done
        // serially, even though we downloaded the data in parallel, and the stream that the tar
        // Builder writes to will upload the written data in paralell also.
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
                    // NO! `append_data` takes a `Read` instance which it will use to read the data
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
                    sender.send(Ok(data)).await;

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

                        sender.send(Ok(data)).await;

                        part = next_part;
                    }

                    // All of the data for this object has been passed to the `blocking_reader`
                    // that `append_data` is reading.  Now we just wait for it to finish
                    drop(sender);
                    append_fut.await?;

                    progress.input_object_written(
                        part.input_object.bucket.name(),
                        &part.input_object.key,
                        part.input_object.version_id.as_deref(),
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
        tar_builder.finish_and_close().await?;

        // Next steps: the writer is probably still busily uploading data.  Need a way to get
        // visibility into that and block until the upload is done.
        todo!()
    }
}
