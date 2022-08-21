//! Tests that exercise interaction with a live object store, but which can be performed against a
//! local Minio S3-compatible object storage server for greater convenience and lower cost.
//!
//! Virtually every test case can be validated against Minio, since we're not testing Minio's
//! compatibility with S3 but rather the correct behavior of our code when talking to S3 or any
//! S3-compatible endpoint (minio included).
use crate::{progress::TestCreateProgressCallback, Result};
use assert_matches::assert_matches;
use ssstar::S3TarError;
use ssstar_testing::{minio, test_data};
use std::collections::HashMap;
use std::{path::Path, sync::Arc};
use url::Url;

/// Set up the ssstar config to use the specified Minio server
fn config_for_minio(server: &minio::MinioServer) -> ssstar::Config {
    let mut config = ssstar::Config::default();

    config.aws_region = Some("us-east-1".to_string());
    config.aws_access_key_id = Some("minioadmin".to_string());
    config.aws_secret_access_key = Some("minioadmin".to_string());
    config.s3_endpoint = Some(server.endpoint_url());

    config
}

fn tar_in_bucket(bucket: &str, key: &str) -> (Url, ssstar::TargetArchive) {
    let url: Url = format!("s3://{}/{}", bucket, key).parse().unwrap();
    let target_archive = ssstar::TargetArchive::ObjectStorage(url.clone());

    (url, target_archive)
}

/// Macro which helps to make tests that have a repetitive structure but vary only in certain
/// parameters.
///
/// In general for most test cases we want to run the same test with a few variations:
/// - Versioning on the bucket enabled and disabled
/// - Target as object store URL, file, and arbitrary writer
///
/// Rather than go wild on copy-pasta, this macro lets us define tests in terms of the set of
/// objects that should be in the bucket, the input paths to pass to the create job, and the
/// expected objects in the resulting tarball.  The macro will generate a ton of reptitive code to
/// implement the appropriate tests with all necessary variations.
macro_rules! create_and_verify_test {
    ($test_name:ident {
        @bucket_contents: [$($test_data_key:expr => $test_data_size:expr),+],
        @input_paths: [$($path:expr),+],
        @expected_objects: [$($expected_key:expr),+]
    }) => {
        mod $test_name {
            use super::*;

            async fn test_setup(enable_versioning: bool) -> Result<(Arc<minio::MinioServer>, String)> {
                let server = minio::MinioServer::get().await?;

                // Test names need to be valid Rust identifiers so they will have "_" chars but
                // astonishngly S3 doesn't allow the "_" char in bucket names!
                let bucket_name = stringify!($test_name).replace("_", "-");

                let bucket = server.create_bucket(bucket_name, enable_versioning).await?;

                Ok(
                    (server, bucket)
                )
            }

            async fn test_data(server: &minio::MinioServer, bucket: &str) -> Result<HashMap<String, test_data::TestObjectWithData>> {
                test_data::make_test_data(
                    &server.aws_client().await?,
                    bucket,
                    vec![
                    $(
                        test_data::TestObject::new($test_data_key, $test_data_size),
                    ),+
                    ],
                )
                .await
            }

            async fn make_job(server: &minio::MinioServer, bucket: &str, target_archive: ssstar::TargetArchive) -> Result<ssstar::CreateArchiveJob> {
                let mut builder =
                    ssstar::CreateArchiveJobBuilder::new(config_for_minio(server), target_archive);
                $(
                    builder.add_input(&format!(concat!("s3://{}", $path), bucket).parse().unwrap()).await?;
                )+

                let job = builder.build().await?;

                Ok(job)
            }

            async fn run_job(job: ssstar::CreateArchiveJob) -> Result<()> {
                let progress = TestCreateProgressCallback::new();

                job.run(futures::future::pending(), progress.clone())
                    .await?;

                progress.sanity_check_updates();

                Ok(())
            }

            async fn validate_archive_contents(test_data: HashMap<String, test_data::TestObjectWithData>, tar_tmp_dir: &Path) -> Result<()> {
                let expected_objects = [$($expected_key),+];
                test_data::validate_test_data_in_dir(&test_data, tar_tmp_dir, expected_objects).await?;

                Ok(())
            }

            /// The bucket with the input objects is not versioned, and the resulting tar archive
            /// is stored in the S3 bucket
            #[test]
            fn unversioned_bucket_tar_in_bucket() -> Result<()> {
                ssstar_testing::logging::test_with_logging(async move {
                    let (server, bucket) = test_setup(false).await?;

                    let (tar_url, target_archive) = tar_in_bucket(&bucket, "test.tar");
                    let test_data = test_data(&server, &bucket).await?;

                    let job = make_job(&server, &bucket, target_archive).await?;

                    run_job(job).await?;

                    let tar_tmp_dir = ssstar_testing::tar::extract_tar_archive_from_s3_url(
                        &server.aws_client().await?,
                        &tar_url,
                    )
                    .await?;

                    validate_archive_contents(test_data, tar_tmp_dir.path()).await?;
                    Ok(())
                })
            }

            /// The bucket with the input objects is versioned, and the resulting tar archive
            /// is stored in the S3 bucket
            #[test]
            fn versioned_bucket_tar_in_bucket() -> Result<()> {
                ssstar_testing::logging::test_with_logging(async move {
                    let (server, bucket) = test_setup(true).await?;

                    let (tar_url, target_archive) = tar_in_bucket(&bucket, "test.tar");
                    let test_data = test_data(&server, &bucket).await?;

                    let job = make_job(&server, &bucket, target_archive).await?;

                    run_job(job).await?;

                    let tar_tmp_dir =
                        ssstar_testing::tar::extract_tar_archive_from_s3_url(&server.aws_client().await?, &tar_url)
                            .await?;

                    validate_archive_contents(test_data, tar_tmp_dir.path()).await?;
                    Ok(())
                })
            }
        }
    }
}

create_and_verify_test! {
    dumb_example {
        @bucket_contents: ["foo" => "10MiB"],
        @input_paths: ["/"],
        @expected_objects: ["foo"]
    }
}

/// Attempt to ingest an empty bucket.
///
/// When there are no matching input objects it should be an error
#[test]
fn empty_bucket() -> Result<()> {
    ssstar_testing::logging::test_with_logging(async move {
        let server = minio::MinioServer::get().await?;
        let bucket = server.create_bucket("empty", false).await?;
        let (_, target_archive) = tar_in_bucket(&bucket, "test.tar");

        let job = ssstar::CreateArchiveJobBuilder::new(config_for_minio(&server), target_archive)
            .build()
            .await?;

        let progress = TestCreateProgressCallback::new();

        let result = job.run(futures::future::pending(), progress.clone()).await;

        assert_matches!(result, Err(S3TarError::NoInputs));

        Ok(())
    })
}

/// Create an archive of a single small object
#[test]
fn single_small_object() -> Result<()> {
    ssstar_testing::logging::test_with_logging(async move {
        let server = minio::MinioServer::get().await?;
        let bucket = server.create_bucket("single-small-object", false).await?;
        let (tar_url, target_archive) = tar_in_bucket(&bucket, "test.tar");

        let test_data = test_data::make_test_data(
            &server.aws_client().await?,
            &bucket,
            vec![
                test_data::TestObject::new("test", "1MiB"),
                test_data::TestObject::new("excluded", "1MiB"),
            ],
        )
        .await?;

        let mut builder =
            ssstar::CreateArchiveJobBuilder::new(config_for_minio(&server), target_archive);
        builder.add_input(&test_data["test"].url).await?;
        let job = builder.build().await?;

        let progress = TestCreateProgressCallback::new();

        job.run(futures::future::pending(), progress.clone())
            .await?;

        progress.sanity_check_updates();

        let tar_tmp_dir = ssstar_testing::tar::extract_tar_archive_from_s3_url(
            &server.aws_client().await?,
            &tar_url,
        )
        .await?;

        test_data::validate_test_data_in_dir(&test_data, tar_tmp_dir.path(), ["test"]).await?;

        Ok(())
    })
}
