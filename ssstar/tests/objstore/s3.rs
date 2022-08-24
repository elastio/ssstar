//! Tests which are intended to run against real live S3 APIs.
//!
//! These tests are limited to only those test cases that can't be exercised with minio, because
//! running live S3 tests is both more expensive to do, and also inconvenient for contributors
//! since these tests assume they can access Elastio-owned buckets specifically created for the
//! purposes of running these tests.

// TODO: do at least one minimal smoke test against the real S3, including exercising correct
// behavior when the bucket is in a different region than the AWS config specifies by default
