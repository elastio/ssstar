//! Implementation of the operation which creates a tar archive from inputs stored in object
//! storage.
use crate::objstore::{Bucket, ObjectStorage, ObjectStorageFactory};
use crate::{Config, Result, S3TarError};
use itertools::Itertools;
use snafu::prelude::*;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tracing::{debug, instrument};
use url::Url;

/// Represents where we will write the target archive
#[derive(Clone)]
pub enum TargetArchive {
    /// Write the tar archive to object storage at the specified URL.
    ///
    /// The URL must specify a bucket and a complete object name.
    ObjectStorage(Url),

    /// Write the tar archive to the local filesystem
    File(PathBuf),

    /// Write the tar archive to some arbitrary [`tokio::io::AsyncWrite`] impl.
    Writer(Arc<dyn AsyncWrite>),
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
        if let TargetArchive::ObjectStorage(url) = &self.target {
            // Validate this URL
            todo!()
        }

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
                x.bucket.name() == y.bucket.name() && x.key == y.key && x.timestamp == y.timestamp
            })
            .collect::<Vec<_>>();

        Ok(CreateArchiveJob {
            config: self.config,
            target: self.target,
            inputs,
        })
    }
}

/// A job which will create a new tar archive from object store inputs.
#[derive(Debug)]
pub struct CreateArchiveJob {
    config: Config,
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

    /// Run the job, returning only when the job has run to completion (or failed)
    ///
    /// If the `abort` future is completes, it's a signal that the job should be aborted.
    /// Existing transfers will be abandoned and queued transfers will be dropped, then this method
    /// returns an abort error.
    pub async fn run(self, abort: impl Future<Output = ()>, progress: ()) -> Result<()> {
        todo!()
    }
}
