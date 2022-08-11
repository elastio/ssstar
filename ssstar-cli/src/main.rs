use clap::{ArgGroup, Parser, Subcommand};
use std::path::PathBuf;
use url::Url;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Operation to perform
    #[clap(subcommand)]
    command: Command,

    #[clap(flatten)]
    globals: Globals,
}

/// Arguments that apply regardless of command
#[derive(Parser, Debug)]
struct Globals {
    /// Use a custom S3 endpoint instead of AWS.
    ///
    /// Use this to operate on a non-Amazon S3-compatible service.  If this is set, the AWS region
    /// is ignored.
    #[clap(long, global = true, value_name = "URL")]
    s3_endpoint: Option<Url>,

    /// Enable verbose log output
    #[clap(short = 'v', long, conflicts_with = "quiet", global = true)]
    verbose: bool,

    /// Be quiet, suppress almost all output (except errors)
    #[clap(short = 'q', long, conflicts_with = "verbose", global = true)]
    quiet: bool,

    /// The chunk size that ssstar uses for multipart transfers of individual files.
    ///
    /// Multipart transfers will be used for objects larger than `multipart_threshold`.
    ///
    /// Can be specified as an integer, ie "1000000", or with a suffix ie "10MB".
    ///
    /// Note that the maximum number of chunks in an upload is 10,000, so for very large objects
    /// this chunk size may be overridden if it's smaller than 1/10,000th of the size of the
    /// object.
    #[clap(long, default_value = "8MB", global = true)]
    multipart_chunk_size: byte_unit::Byte,

    /// The size threshold ssstar uses for multipart transfers of individual objects.
    ///
    /// If an object is this size of larger, then it will be transfered in chunks of
    /// `multipart_chunk_size` bytes each.
    ///
    /// Can be specified as an integer, ie "1000000", or with a suffix ie "10MB"
    #[clap(long, default_value = "8MB", global = true)]
    multipart_threshold: byte_unit::Byte,

    /// The maximum number of concurrent requests to the bucket when performing transfers.
    ///
    /// In case of multipart transfers, each chunk counts as a separate request.
    ///
    /// A higher number of concurrent requests may be necessary in order to saturate very fast
    /// connections to S3, but this will also increase RAM usage during the transfer.
    #[clap(long, default_value = "10", global = true)]
    max_concurrent_requests: u64,

    /// The maximum number of tasks in the task queue.
    ///
    /// In case of multipart transfers, each chunk counts as a separate task.
    #[clap(long, default_value = "1000", global = true)]
    max_queue_size: u64,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create a new tar archive from S3 objects
    #[clap(group(ArgGroup::new("output").required(true)))]
    Create {
        /// Write the tar archive to a file.
        #[clap(short = 'f', long, value_parser, group = "output")]
        file: Option<PathBuf>,

        /// Write the tar archive to an S3 object.
        ///
        /// The URL should specify the S3 bucket as well as the object name.  This URL will be used
        /// verbatim without any processing by ssstar.
        #[clap(short = 's', long, value_parser, group = "output", value_name = "URL")]
        s3: Option<Url>,

        /// Write the tar archive to stdout
        #[clap(short = 't', long, group = "output")]
        stdout: bool,

        /// One or more S3 URLs pointing to a bucket, a prefix, a specific object, or a glob.
        ///
        /// Each URL must be a URL starting with `s3://`, and can specify just a bucket, a bucket
        /// and an object path, or globs.
        ///
        /// EXAMPLES:
        ///
        /// s3://foo/         - Read all objects from the bucket `foo`
        ///
        /// s3://foo/bar/     - Read all objects from the bucket `foo` with the prefix `bar/`
        ///
        /// s3://foo/bar      - Read the object `bar` in the bucket `foo`
        ///
        /// s3://foo/*.txt    - Read the all objects in the root of bucket `foo` with extension `.txt`
        ///
        /// s3://foo/**/*.txt - Read the all objects in any directory of bucket `foo` with
        ///                     extension `.txt``
        ///
        /// NOTE: When specifying globs, make sure to enclose the entire URL in "", otherwise your
        /// shell might expand the globs locally and produce unintended results.
        #[clap(value_parser, required = true, value_name = "URLS")]
        objects: Vec<Url>,
    },

    /// Extract a tar archive, storing the results in S3
    #[clap(group(ArgGroup::new("input").required(true)))]
    Extract {
        /// Read the tar archive to extract from a file
        #[clap(short = 'f', long, value_parser, group = "input")]
        file: Option<PathBuf>,

        /// Read the tar archive to extract from an S3 object
        ///
        /// The URL should specify the S3 bucket as well as the object name.  This URL will be used
        /// verbatim without any processing by ssstar.
        #[clap(short = 's', long, value_parser, group = "input", value_name = "URL")]
        s3: Option<Url>,

        /// Read the tar archive to extract from stdin
        #[clap(short = 't', long, group = "input")]
        stdin: bool,

        /// URL of S3 bucket (and optional prefix) to which archive will be extracted.
        ///
        /// Every file in the archive will be written as an S3 object with this URL prepended.
        ///
        /// For example if the URL is s3://foo/bar/, and the tar archive contains a single file
        /// a/b/c/d.txt, then the S3 object s3://foo/bar/a/b/c/d.txt will be created.  Note that a
        /// trailing "/" isn't implied.  To continue the previous example, if you specify the URL
        /// s3://foo/bar (without the trailing '/'), then the object s3://foo/bara/b/c/d.txt is
        /// created.
        #[clap(value_parser, value_name = "URL")]
        target: Url,
    },
}

fn main() {
    let args = Args::parse();

    println!("Hello, world!");

    println!("{:#?}", args);
}
