//! Integration tests exercising the various implementations of [`ssstar::ObjectStorage`] and
//! [`ssstar::Bucket`].
//!
//! Some of these require live object storage accounts with a bucket pre-made.  Most of those will
//! only work when run from the Elastio Github Actions build or on an Elastio developer's system
//! with necessary credentials.  Those that are restricted that way are marked as `ignore` so that
//! they must be explicitly invoked.
//!
//! The tests that can run on any dev system either use the local filesystem, or
//! [minio](https://min.io), the later of which obviously assuming that minio is installed on the
//! local system

mod minio;
mod s3;
