## ssstar

Highly concurrent archiving of S3 objects to and from tar archives.

---

This is the Rust library crate which powers the `ssstar` CLI.  If you're looking for the
`ssstar` command line utility, see the [`ssstar-cli`](https://crates.io/crates/ssstar-cli)
crate.

To create a tar archive containing S3 objects, instantiate a `CreateArchiveJob`:

```rust,no_run
# use ssstar::*;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
// Write the archive to a local file
let target = TargetArchive::File("test.tar".into());

let mut builder = CreateArchiveJobBuilder::new(Config::default(), target);

// Archive all of the objects in this bucket
builder.add_input(&"s3://my-bucket".parse()?).await?;

let job = builder.build().await?;

job.run_without_progress(futures::future::pending()).await?;

# Ok(())
# }
```

Target archives can be written to a local file, an S3 bucket, or an arbitrary Tokio `AsyncWrite` implementation.  See
[`TargetArchive`] for more details.

Restoring a tar archive to object storage is similarly straightforward:


```rust,no_run
# use ssstar::*;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
// Read the archive from a local file
let source = SourceArchive::File("test.tar".into());

// Extract the archive to an S3 bucket, prepending a `foo/` prefix to every file path in
// the archive
let target = "s3://my-bucket/foo/".parse::<url::Url>()?;

let mut builder = ExtractArchiveJobBuilder::new(Config::default(), source, target).await?;

// Extract only text files, in any directory, from the archive
builder.add_filter("**/*.txt")?;

let job = builder.build().await?;

job.run_without_progress(futures::future::pending()).await?;

# Ok(())
# }
```
