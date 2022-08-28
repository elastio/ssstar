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
use std::path::PathBuf;
use std::{path::Path, sync::Arc};
use tempdir::TempDir;
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

fn tar_in_file() -> (TempDir, PathBuf, ssstar::TargetArchive) {
    let tempdir = TempDir::new("ssstar-tests-tar").unwrap();
    let path = tempdir.path().join("test.tar");

    let target_archive = ssstar::TargetArchive::File(path.clone());

    (tempdir, path, target_archive)
}

/// Same as `tar_in_file` except the file is exposed as an arbitrary `AsyncWrite` instead
async fn tar_in_writer() -> (TempDir, PathBuf, ssstar::TargetArchive) {
    let (tempdir, path, _) = tar_in_file();

    let target_archive =
        ssstar::TargetArchive::Writer(Box::new(tokio::fs::File::create(&path).await.unwrap()));

    (tempdir, path, target_archive)
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
                        test_data::TestObject::new($test_data_key, $test_data_size)
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

            /// The bucket with the input objects is versioned, and the resulting tar archive
            /// is stored in a temp dir on the filesystem
            #[test]
            fn versioned_bucket_tar_in_file() -> Result<()> {
                ssstar_testing::logging::test_with_logging(async move {
                    let (server, bucket) = test_setup(true).await?;

                    let (_tempdir, tar_path, target_archive) = tar_in_file();
                    let test_data = test_data(&server, &bucket).await?;

                    let job = make_job(&server, &bucket, target_archive).await?;

                    run_job(job).await?;

                    let tar_tmp_dir =
                        ssstar_testing::tar::extract_tar_archive_from_file(&tar_path)
                            .await?;

                    validate_archive_contents(test_data, tar_tmp_dir.path()).await?;
                    Ok(())
                })
            }

            /// The bucket with the input objects is versioned, and the resulting tar archive
            /// is stored in a temp dir on the filesystem but is passed to the create job as an
            /// `AsyncWrite` impl not a file
            #[test]
            fn versioned_bucket_tar_in_writer() -> Result<()> {
                ssstar_testing::logging::test_with_logging(async move {
                    let (server, bucket) = test_setup(true).await?;

                    let (_tempdir, tar_path, target_archive) = tar_in_writer().await;
                    let test_data = test_data(&server, &bucket).await?;

                    let job = make_job(&server, &bucket, target_archive).await?;

                    run_job(job).await?;

                    let tar_tmp_dir =
                        ssstar_testing::tar::extract_tar_archive_from_file(&tar_path)
                            .await?;

                    validate_archive_contents(test_data, tar_tmp_dir.path()).await?;
                    Ok(())
                })
            }
        }
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

/// Any input path that doesn't match any objects should cause an error, even if other input paths
/// are provided that do match
#[test]
fn input_path_matches_nothing() -> Result<()> {
    ssstar_testing::logging::test_with_logging(async move {
        let server = minio::MinioServer::get().await?;
        let bucket = server
            .create_bucket("input_path_matches_nothing", false)
            .await?;
        let (_, target_archive) = tar_in_bucket(&bucket, "test.tar");
        let _test_data = test_data::make_test_data(
            &server.aws_client().await?,
            &bucket,
            vec![test_data::TestObject::new("test", "1KiB")],
        )
        .await?;

        let mut builder =
            ssstar::CreateArchiveJobBuilder::new(config_for_minio(&server), target_archive);
        builder
            .add_input(&format!("s3://{}/test", bucket).parse().unwrap())
            .await?;
        builder
            .add_input(&format!("s3://{}/foo/**", bucket).parse().unwrap())
            .await?;

        // Building the list of input objects happens in the `build` call, so this is where the
        // error is expected
        let result = builder.build().await;

        assert_matches!(result, Err(S3TarError::SelectorMatchesNoObjects { .. }));

        Ok(())
    })
}

// The following tests all follow the same structure and thus are generated by a macro, but involve
// different permutations of object sizes and input paths to exercise different aspects of the
// archive creation code

// A bucket with one small object matched by the input path of the entire bucket
create_and_verify_test! {
    single_small_object_input_bucket {
        @bucket_contents: ["test" => "1KiB"],
        @input_paths: ["/"],
        @expected_objects: ["test"]
    }
}

// A bucket with one small object matched by the input path of the prefix of the object
create_and_verify_test! {
    single_small_object_input_prefix {
        @bucket_contents: ["prefix/test" => "1KiB"],
        @input_paths: ["/prefix/"],
        @expected_objects: ["prefix/test"]
    }
}

// A bucket with multiple small objects with and without prefixes, with input paths that match
// some but not all of the objects in the bucket
create_and_verify_test! {
    multiple_small_objects_multiple_inputs {
        @bucket_contents: [
            "test" => "1KiB",
            "prefix1/test" => "1KiB",
            "prefix2/test" => "1KiB",
            "prefix3/test" => "1KiB",
            "prefix3/prefix4/test" => "1KiB"
        ],
        @input_paths: ["/test", "/prefix1/", "/prefix3/"],
        @expected_objects: ["test", "prefix1/test", "prefix3/test"]
    }
}

// When the total size of the archive exceeds 8MB, the target archive will be written to S3 as a
// multi-part archive.  That's a very different code path.
create_and_verify_test! {
    small_objects_multipart_tar_archive {
        @bucket_contents: [
            "test1" => "1MiB",
            "test2" => "2MiB",
            "test3" => "3MiB",
            "test4" => "4MiB",
            "test5" => "5MiB"
        ],
        @input_paths: ["/"],
        @expected_objects: ["test1", "test2", "test3", "test4", "test5"]
    }
}

// When the size of an input object is larger than 8MB, that object is transfered in multiple parts
create_and_verify_test! {
    multipart_objects_multipart_tar_archive {
        @bucket_contents: [
            "test1" => "8MiB",
            "test2" => "9MiB",
            "test3" => "10MiB",
            "test4" => "11MiB",
            "test5" => "12MiB"
        ],
        @input_paths: ["/"],
        @expected_objects: ["test1", "test2", "test3", "test4", "test5"]
    }
}

// With a lot of small objects, it will fill up the queue with objects to process and exert
// back-pressure on the transfer task
create_and_verify_test! {
    many_small_objects {
        @bucket_contents: [
            "test1" => "1KiB",
            "test2" => "1KiB",
            "test3" => "1KiB",
            "test4" => "1KiB",
            "test5" => "1KiB",
            "test6" => "1KiB",
            "test7" => "1KiB",
            "test8" => "1KiB",
            "test9" => "1KiB",
            "test10" => "1KiB",
            "test11" => "1KiB",
            "test12" => "1KiB",
            "test13" => "1KiB",
            "test14" => "1KiB",
            "test15" => "1KiB",
            "test16" => "1KiB",
            "test17" => "1KiB",
            "test18" => "1KiB",
            "test19" => "1KiB",
            "test20" => "1KiB"
        ],
        @input_paths: ["/"],
        @expected_objects: [
            "test1",
            "test2",
            "test3",
            "test4",
            "test5",
            "test6",
            "test7",
            "test8",
            "test9",
            "test10",
            "test11",
            "test12",
            "test13",
            "test14",
            "test15",
            "test16",
            "test17",
            "test18",
            "test19",
            "test20"
        ]
    }
}
