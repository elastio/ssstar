#![doc = include_str!("../README.md")]

use clap::{ArgGroup, Parser, Subcommand};
use ssstar::{CreateArchiveJobBuilder, ExtractArchiveJobBuilder, SourceArchive, TargetArchive};
use std::path::PathBuf;
use tracing::debug;
use url::Url;

mod progress;

// NB: can't use git info here because when this crate is published to crates.io there is no
// git metadata
const VERSION_DETAILS: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    "target ",
    env!("VERGEN_CARGO_TARGET_TRIPLE"),
    ")"
);

#[derive(Parser, Debug)]
#[clap(author, version = VERSION_DETAILS, about, long_about = None)]
struct Args {
    /// Operation to perform
    #[clap(subcommand)]
    command: Command,

    #[clap(flatten)]
    global: Globals,
}

/// Arguments that apply regardless of command
#[derive(Parser, Debug)]
struct Globals {
    /// Enable verbose log output
    #[clap(short = 'v', long, conflicts_with = "quiet", global = true)]
    verbose: bool,

    /// Be quiet, suppress almost all output (except errors)
    #[clap(short = 'q', long, conflicts_with = "verbose", global = true)]
    quiet: bool,

    /// The number of async task worker threads in the thread pool.
    ///
    /// This value has a reasonable default selected based on the specifications of the system on
    /// which it runs and should never be overridden except in rare cases.  If you're not sure what
    /// to set this to, don't set it at all.
    #[clap(long, global = true)]
    worker_threads: Option<usize>,

    /// The maximum number of threads used to run blocking tasks.
    ///
    /// This value has a reasonable default selected based on the specifications of the system on
    /// which it runs and should never be overridden except in rare cases.  If you're not sure what
    /// to set this to, don't set it at all.
    #[clap(long, global = true)]
    max_blocking_threads: Option<usize>,

    #[clap(flatten)]
    config: ssstar::Config,
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

        /// Strip this many path components (separated by `/`) from objects in archive prior to
        /// extracting.
        ///
        /// For example if this value is 2, and the archive contains an object `foo/bar/baz/boo`,
        /// then the resulting target URL for that object will be the concatenation of `target` and
        /// `baz/boo`; the first two path components `foo` and `bar` are stripped.
        //#[clap(long)]
        //strip_components: Option<usize>,

        /// Optional filters to limit the objects extracted from the archive.
        ///
        /// These can be exact object names, prefixes, or globs.  Filters are evaluated with an OR
        /// operator, so an object is extracted if it matches any one of the filters specified.  If
        /// no filters are specified, the entire contents of the archive are restored.
        ///
        /// EXAMPLES:
        ///
        /// `foo` - Extract the object called `foo` at the root level, no prefix
        ///
        /// `bar/baz` - Extract the object `bar/baz`
        ///
        /// `bar/` - Extract all objects that have a prefix `bar/` (and possibly additional
        /// prefixes following `bar/`)
        ///
        /// `**` - Extract everything in the archive.  This is the default so no need to specify
        /// explicitly.
        ///
        /// `bar/**/*.txt` - Extract everything under the `bar/` prefix, recursively, with names
        /// that end in `.txt`.
        #[clap(value_name = "FILTER_EXPR")]
        filters: Vec<String>,
    },
}

impl Command {
    async fn run(self, globals: &Globals) -> ssstar::Result<()> {
        match self {
            Self::Create {
                file,
                stdout,
                s3,
                objects,
            } => {
                let target = if let Some(path) = file {
                    TargetArchive::File(path)
                } else if let Some(url) = s3 {
                    TargetArchive::ObjectStorage(url)
                } else if stdout {
                    TargetArchive::Writer(Box::new(tokio::io::stdout()))
                } else {
                    unreachable!(
                        "BUG: clap should require the user to specify exactly one of these options"
                    );
                };

                let mut builder = CreateArchiveJobBuilder::new(globals.config.clone(), target);

                let builder =
                    progress::with_spinner(globals, "Validating input URLs...", async move {
                        for url in objects {
                            builder.add_input(&url).await?;
                        }

                        Ok(builder)
                    })
                    .await?;

                let job = progress::with_spinner(
                    globals,
                    "Counting input objects to archive to tar...",
                    async move { builder.build().await },
                )
                .await?;

                progress::run_create_job(globals, job).await?;

                Ok(())
            }
            Self::Extract {
                file,
                s3,
                stdin,
                target,
                //strip_components,
                filters,
            } => {
                let source = if let Some(path) = file {
                    SourceArchive::File(path)
                } else if let Some(url) = s3 {
                    SourceArchive::ObjectStorage(url)
                } else if stdin {
                    SourceArchive::Reader(Box::new(std::io::stdin()))
                } else {
                    unreachable!(
                        "BUG: clap should require the user to specify exactly one of these options"
                    );
                };

                let mut builder =
                    ExtractArchiveJobBuilder::new(globals.config.clone(), source, target).await?;

                for filter in filters {
                    builder.add_filter(filter)?;
                }

                let job =
                    progress::with_spinner(globals, "Checking source archive...", async move {
                        builder.build().await
                    })
                    .await?;

                progress::run_extract_job(globals, job).await?;

                Ok(())
            }
        }
    }
}

fn main() -> color_eyre::Result<()> {
    let args = Args::parse();

    // If verbose output is enabled, enabling logging.
    // If not, log events will be ignored
    if args.global.verbose || std::env::var("RUST_LOG").is_ok() {
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::{fmt, EnvFilter};

        // Configure a custom event formatter
        //
        // "Why can't we have easy to read, local timestamps instead of UTC timestamps that confuse
        // everyone who doesn't live at UTC+0?"
        //
        // Good question.  Sit down and grab a drink, dear reader, and I'll explain the tale of woe
        // which has befallen time handling in Rust:
        //
        // It turns out there's no 100% reliable way to get the UTC offset of the local timezone on
        // POSIX systems.  So all your life, when you've seen a local timestamp in a log or a
        // directory listing or something, you've actually just been incredibly fortunate that a
        // highly improbable series of events haven't taken place to render the calculation of the
        // local timezone offset invalid.
        //
        // Upon learning this, the developers of the otherwise-excellent (and foundational) `time`
        // crate in Rust were horrified, and (IMHO) overreacted with such fervent zeal for the
        // eradication of undefined behavior in their Rust code that they completely disabled the
        // computation of local time from UTC.
        //
        // In what perhaps they thought was a reasonable compromise between pathological risk
        // aversion and devil-may-care recklessness, they provided an escape hatch: if you control
        // the binary compilation process you can add to `RUSTFLAGS` a magical incantation to
        // re-enable this cursed functionality (and thereby take your life into your own hands).
        // https://github.com/time-rs/time/commit/ec4057d6b8606ec7485380328cf3baedc9b60e54
        // Unfortunately this is a cartoonishly unworkable solution, as it ensures that any `cargo
        // install`-powered software distribution is impossible without educating all of your users
        // on this `RUSTFLAGS` thing.
        //
        // So, dear reader, if you enable verbose logging in ssstar and are trying to reason about
        // when things happened on your environment, be sure you come prepared with knowledge of
        // your local UTC offset (presumably obtained by unsound means), and practice your mental
        // arithmetic to translate the UTC timestamps we produce into times that make sense for
        // you.  And take heart, for you are free from potentially unsound behavior while running
        // ssstar.
        let format = fmt::layer()
            .with_level(true) // include level in output
            .with_target(true) // targets aren't that useful but filters operate on targets so they're important to know
            .with_thread_ids(false) // thread IDs are useless when using async code
            .with_thread_names(false) // same with thread names
            // .with_timer(fmt::time::LocalTime::rfc_3339())
            .with_writer(std::io::stderr);

        // Get the log filter from the RUST_LOG env var, or if not set use a reasonable default
        let filter = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new("h2=warn,hyper=info,rustls=info,aws=info,debug"))
            .unwrap();

        // Create a `fmt` subscriber that uses our custom event format, and set it
        // as the default.
        tracing_subscriber::registry()
            .with(filter)
            .with(format)
            .init();
    }

    // Report panics with the prettier color-eyre
    color_eyre::install().unwrap();

    // Set up the tokio runtime
    let mut builder = tokio::runtime::Builder::new_multi_thread();

    debug!(?args.global.worker_threads, ?args.global.max_blocking_threads, "Initializing tokio runtime");

    if let Some(worker_threads) = args.global.worker_threads {
        builder.worker_threads(worker_threads);
    }
    if let Some(max_blocking_threads) = args.global.max_blocking_threads {
        builder.max_blocking_threads(max_blocking_threads);
    }

    let rt = builder.enable_all().build().unwrap();

    let result: color_eyre::Result<()> = rt.block_on(async move {
        args.command.run(&args.global).await?;

        color_eyre::Result::<()>::Ok(())
    });

    result
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn verify_cli() {
        use clap::CommandFactory;
        Args::command().debug_assert()
    }
}
