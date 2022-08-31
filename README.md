ssstar
------------
ssstar is a Rust library crate as well as a command-line tool to create and extract `tar`-compatible archives containing
objects stored in S3 or S3-compatible storage.  It works similarly to GNU `tar`, and produces archives that are 100%
compatible with `tar`, though it uses different command line arguments.

[![Crates.io](https://img.shields.io/crates/v/ssstar.svg)](https://crates.io/crates/ssstar)
[![Docs.rs](https://docs.rs/ssstar/badge.svg)](https://docs.rs/ssstar)
[![CI](https://github.com/elastio/ssstar/workflows/CI/badge.svg)](https://github.com/elastio/ssstar/actions)
[![Coverage Status](https://coveralls.io/repos/github/elastio/ssstar/badge.svg?branch=master)](https://coveralls.io/github/elastio/ssstar?branch=master)
![Crates.io](https://img.shields.io/crates/l/ssstar)

`ssstar` provides a cross-platform Rust-powered CLI as well as a Rust library crate that lets you create tar archives
containing objects from S3 and S3-compatible object storage, regardless of size.  `ssstar` applies concurrency
aggressively, and uses a streaming design which means even multi-TB objects can be processed with minimal memory
utilization.  The resulting tar archive can itself be uploaded to object storage, written to a local file, or written
to `stdout` and piped to another command line tool.

We built `ssstar` so our customers using the `elastio` cloud native backup and recovery CLI could backup and restore S3
buckets directly into and from [Elastio](https://elastio.com/) vaults, however we made the tool generic enough that it can be used by itself
whenever you need to package one or more S3 objects into a tarball.

## Usage (without Elastio)

To create a tar archive, you specify S3 buckets, objects, entire prefixes, or globs, as well as where you want the tar
archive to be written:

```shell

# Archive an entire bucket and write the tar archive to another bucket
ssstar create \
  s3://my-source-bucket \
  --s3 s3://my-destination-bucket/backup.tar

# Archive all objects in the `foo/` prefix (non-recursive) and write the tar archive to a local file
ssstar create \
  s3://my-source-bucket/foo/ \
  --file ./backup.tar

# Archive some specific objects identified by name, and write the tar archive to stdout and pipe that to
# gzip
ssstar create \
  s3://my-source-bucket/object1 s3://my-source-bucket/object2 \
  --stdout | gzip > backup.tar.gz

# Archive all objects matching a glob, and write the tar archive to another bucket
ssstar create \
  "s3://my-source-bucket/foo/**" \
  --s3 s3://my-destination-bucket/backup.tar
```

You can pass multiple inputs to `ssstar create`, using a mix of entire buckets, prefixes, specific objects, and globs.
Just make sure that when you use globs you wrap them in quotes, otherwise your shell may try to evaluate them.  For
example:

```shell
# Archive a bunch of different inputs, writing the result to a file
ssstar create \
  s3://my-source-bucket/                  \ # <-- include all objects in `my-source-bucket`
  s3://my-other-bucket/foo/               \ # <-- include all objects in `foo/` (non-recursive)
  s3://my-other-bucket/bar/boo            \ # <-- include the object with key `bar/boo`
  "s3://yet-another-bucket/logs/2022*/**" \ # <-- recursively include all objects in any prefix `logs/2022*`
  --file ./backup.tar                     # <-- this is the path where the tar archive will be written
```

To extract a tar archive and write the contents directly to S3 objects, you specify where to find the tar archive,
optional filters to filter what is extracted, and the S3 bucket and prefix to which to extract the contents.

A simple example:

```shell
# Extract a local tar archive to the root of an S3 bucket `my-bucket`
ssstar extract --file ./backup.tar s3://my-bucket
```

Each file in the tar archive will be written to the bucket `my-bucket`, with the object key equal to the file path
within the archive.  For example if the archive contains a file `foo/bar/baz.txt`, that file will be written to
`s3://my-bucket/foo/bar/baz.txt`.

You can provide not just a target bucket but also a prefix as well, e.g.:

```shell
# Extract a local tar archive to the prefix `restored/` of an S3 bucket `my-bucket`
ssstar extract --file ./backup.tar s3://my-bucket/restored/
```

In that case, if the tar archive contains a file `foo/bar/baz.txt`, it will be written to
`s3://my-bucket/restored/foo/bar/baz.txt`.  *NOTE*: In S3, prefixes don't necessarily end in `/`; if you don't provide
the trailing `/` character to the S3 URL passed to `ssstar extract`, it will not be added for you!  Instead you'll get
something like `s3://my-bucket/restoredfoo/bar/baz`, which may or may not be what you actually want!

If you don't want to extract the full contents of the archive, you can specify one or more filters.  These can be exact
file paths, directory paths ending in `/`, or globs.  For example:

```shell
ssstar extract --file ./backup.tar \
  foo/bar/baz.txt          \ # <-- extract the file `foo/bar/baz.txt` if it's present in the archive
  boo/                     \ # <-- extract all files in the `boo` directory (recursive)
  "baz/**/*.txt"           \ # <-- extract any `.txt` file anywhere in `baz/`, recursively
  s3://my-bucket/restored/   # <-- write all matching files to the `restored/` prefix in `my-bucket`
```

## Usage (with Elastio)

To use with Elastio, create archives with the `--stdout` option and pipe to `elastio stream backup`, and restore them by piping
`elastio stream restore` to `ssstar extract` with the `--stdin` option.  For example:


```shell
# Backup an entire S3 bucket `my-source-bucket` to the default Elastio vault:
ssstar create s3://my-source-bucket/ --stdout \
  | elastio stream backup --hostname-override my-source-bucket
```

```shell
# Restore a recovery point with ID `$RP_ID` from Elastio to the `my-destination-bucket` bucket:
elastio stream restore --rp-id $RP_ID \
  | ssstar extract --stdin s3://my-destination-bucket
```

For more about using the Elastio CLI, see the [Elastio CLI docs](https://docs.elastio.com/src/elastio-cli/elastio-cli-overview.html)

## Advanced CLI Options

Run `ssstar create --help` and `ssstar extract --help` to get the complete CLI usage documentation for archive creation
and extraction, respectively.  There are a few command line options that are particularly likely to be of interest:

### Using a custom S3 endpoint

`ssstar` is developed and tested against AWS S3, however it should work with any object storage system that provides an
S3-compatible API.  In particular, most of the automated tests our CI system runs actually use [Minio](https://min.io)
and not the real S3 API.  To use `ssstar` with an S3-compatible API, use the `--s3_endpoint` option.  For example, if
you have a Minio server running at `127.0.7.1:30000`, using default `minioadmin` credentials, you can use it with
`ssstar` like this:

```shell
ssstar --s3-endpoint http://127.0.0.1:30000 \
  --aws-access-key-id minioadmin --aws-secret-access-key minio-admin \
  ...
```

### Controlling Concurrency

The `--max-concurrent-requests` argument controls how many concurrent S3 API operations will be performed in each stage
of the archive creation or extraction process.  The default is 10, because that is what the AWS CLI uses.  However if
you are running `ssstar` on an EC2 instance with multi-gigabit Ethernet connectivity to S3, 10 concurrent requests may
not be enough to saturate the network connection.  Experiment with larger values to see if you experience faster
transfer times with more concurrency.

## Usage (in a Rust project)

The library crate [`ssstar`](https://crates.io/crates/ssstar) is the engine that powers the `ssstar` CLI.  When we wrote
`ssstar` we deliberately kept all of the functionality in a library crate with a thin CLI wrapper on top, because
`ssstar` is being used internally in Elastio to power our upcoming S3 backup feature.  You too can integrate `ssstar`
functionality into your Rust application.  Just add `ssstar` as a dependency in your `Cargo.toml`:

```toml
[dependencies]
ssstar = "0.1.2"
```

See the [docs.rs](https://docs.rs/ssstar) documentation for `ssstar` for more details and some examples.  You can also
look at the `ssstar` CLI code [`ssstar-cli/main.rs`](`main.rs`) to see how we implemented our CLI in terms of the
`ssstar` library crate.

## :construction: Under Construction, Experimental

`ssstar` is still under active development and should not yet be used in production.

To install on your local system, use `cargo install`, ie:

```shell
cargo install --git https://github.com/elastio/ssstar.git --rev b9cf76ca3 --locked --force ssstar-cli
```

This uses the most recent commit that @anelson has designated as "stable"-ish.  You can drop the `--rev` argument and
get the latest `master`, but this is more likely to be broken.

After this command succeeds you will have `ssstar-cli` installed locally.  Normally on Linux systems this goes to
`~/.cargo/bin`, so if this isn't in your `PATH` you must add it.

Once you do so, run `ssstar create --help` to see how to perform the archive creation operation.

Here's an example CLI that @anelson uses.  This won't work unless you have configured credentials into the
`anelson-isolated` AWS account, but it's an example of the syntax:

```shell
ssstar create \
  s3://elastio-vault-default-8ibrn2zg6/vault.json \
  s3://elastio-vault-default-8ibrn2zg6/fixed:65536/metadata/chunks/db/ \
  s3://elastio-vault-default-8ibrn2zg6/fixed:65536/metadata/extents/db/ \
  s3://elastio-vault-anelson-test-a045e7928d \
  "s3://elastio-account-level-stack-tfstate42168d51-qjatbtl9y9ve/**" \
  --s3 s3://elastio-vault-default-8ibrn2zg6/test.tar
```

## Installation

### Cargo

* Install the rust toolchain in order to have cargo installed by following
  [this](https://www.rust-lang.org/tools/install) guide.
* run `cargo install ssstar-cli --locked`

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

See [CONTRIBUTING.md](CONTRIBUTING.md).
