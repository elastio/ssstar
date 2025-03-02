//! Tests that exercise interaction with a live object store, but which can be performed against a
//! local Minio S3-compatible object storage server for greater convenience and lower cost.
//!
//! Virtually every test case can be validated against Minio, since we're not testing Minio's
//! compatibility with S3 but rather the correct behavior of our code when talking to S3 or any
//! S3-compatible endpoint (minio included).
use crate::{
    Result,
    progress::{TestCreateProgressCallback, TestExtractProgressCallback},
};
use assert_matches::assert_matches;
use ssstar::S3TarError;
use ssstar_testing::{minio, test_data};
use std::collections::HashMap;
use std::path::PathBuf;
use std::{path::Path, sync::Arc};
use tempfile::TempDir;
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

/// Tests which exercise the create path without performing any extraction
mod create {
    use super::*;

    fn tar_in_bucket(bucket: &str, key: &str) -> (Url, ssstar::TargetArchive) {
        let url: Url = format!("s3://{}/{}", bucket, key).parse().unwrap();
        let target_archive = ssstar::TargetArchive::ObjectStorage(url.clone());

        (url, target_archive)
    }

    fn tar_in_file() -> (TempDir, PathBuf, ssstar::TargetArchive) {
        let tempdir = TempDir::new().unwrap();
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

            let job =
                ssstar::CreateArchiveJobBuilder::new(config_for_minio(&server), target_archive)
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
}

/// Test the extract functionality.  This necessarily requires performing create operations first
/// so there's something to extract, but the behavior of the create operation isn't tested.
mod extract {
    use super::*;

    /// Macro which helps to make tests that have a repetitive structure but vary only in certain
    /// parameters.
    ///
    /// This generates tests which create an archive in a bucket with some specific test data, then
    /// restores that archive to another bucket with some filters, and verifies the results both in
    /// terms of the objects from the archive which are extracted, their prefix (if any), and their
    /// contents relative to the test data that served as the input.
    macro_rules! extract_test {
    ($test_name:ident {
        @bucket_contents: [$($test_data_key:expr => $test_data_size:expr),+],
        @filters: [$($filter:expr),*],
        @expected_objects: [$($expected_key:expr),*]
    }) => {
        mod $test_name {
            use super::*;

            /// Set up a test with two buckets, one for the source and another as the target to
            /// extract into
            async fn test_setup(enable_versioning: bool) -> Result<(Arc<minio::MinioServer>, String, String)> {
                let server = minio::MinioServer::get().await?;

                // Test names need to be valid Rust identifiers so they will have "_" chars but
                // astonishngly S3 doesn't allow the "_" char in bucket names!
                let source_bucket_name = concat!(stringify!($test_name), "-src").replace("_", "-");
                let target_bucket_name = concat!(stringify!($test_name), "-tgt").replace("_", "-");

                let source_bucket = server.create_bucket(source_bucket_name, enable_versioning).await?;
                let target_bucket = server.create_bucket(target_bucket_name, enable_versioning).await?;

                Ok(
                    (server, source_bucket, target_bucket)
                )
            }

            /// Create an archive containing all of the test data, stored in the specified bucket.
            ///
            /// Return the URL of the tar archive for use extracting in tests.
            async fn create_archive_on_s3(server: &minio::MinioServer, bucket: &str) -> Result<Url> {
                let url: Url = format!("s3://{}/test.tar", bucket).parse().unwrap();
                let target_archive = ssstar::TargetArchive::ObjectStorage(url.clone());

                let mut builder =
                    ssstar::CreateArchiveJobBuilder::new(config_for_minio(server), target_archive);

                // Always add the entire bucket so all test data are included in the archive
                builder.add_input(&format!("s3://{}/", bucket).parse().unwrap()).await?;

                let job = builder.build().await?;
                let progress = TestCreateProgressCallback::new();

                job.run(futures::future::pending(), progress.clone())
                    .await?;

                progress.sanity_check_updates();

                Ok(url)
            }

            /// Create an archive containing all of the test data in the test bucket, stored in a temp directory
            /// created for that purpose
            async fn create_archive_on_disk(server: &minio::MinioServer, bucket: &str) -> Result<(TempDir, PathBuf)> {
                let temp_dir = TempDir::new().unwrap();
                let tar_path = temp_dir.path().join("test.tar");
                let target_archive = ssstar::TargetArchive::File(tar_path.clone());

                let mut builder =
                    ssstar::CreateArchiveJobBuilder::new(config_for_minio(server), target_archive);

                // Always add the entire bucket so all test data are included in the archive
                builder.add_input(&format!("s3://{}/", bucket).parse().unwrap()).await?;

                let job = builder.build().await?;
                let progress = TestCreateProgressCallback::new();

                job.run(futures::future::pending(), progress.clone())
                    .await?;

                progress.sanity_check_updates();

                Ok((temp_dir, tar_path))
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

            async fn make_job(server: &minio::MinioServer,
                source_archive: ssstar::SourceArchive,
                target_bucket: &str,
                target_prefix: &str
                ) -> Result<ssstar::ExtractArchiveJob> {
                #[allow(unused_mut)] // `mut` is unnecessary only if there are no filters
                let mut builder =
                    ssstar::ExtractArchiveJobBuilder::new(config_for_minio(server),
                        source_archive,
                        format!("s3://{}/{}", target_bucket, target_prefix).parse().unwrap())
                    .await?;
                $(
                    builder.add_filter($filter)?;
                )*

                let job = builder.build().await?;

                Ok(job)
            }

            async fn run_job(job: ssstar::ExtractArchiveJob) -> Result<()> {
                let progress = TestExtractProgressCallback::new();

                job.run(futures::future::pending(), progress.clone())
                    .await?;

                progress.sanity_check_updates();

                Ok(())
            }

            async fn validate_bucket_contents(server: &minio::MinioServer,
                test_data: HashMap<String, test_data::TestObjectWithData>,
                bucket: &str,
                prefix: &str
            ) -> Result<()> {
                let expected_objects = [$($expected_key),*];

                let client = server.aws_client().await?;
                test_data::validate_test_data_in_s3(&client,
                    &test_data,
                    bucket,
                    prefix,
                    expected_objects).await?;

                Ok(())
            }

            /// Extract from a tar in a bucket to another bucket with no prefix
            #[test]
            fn extract_from_archive_on_bucket_to_bucket_root() -> Result<()> {
                ssstar_testing::logging::test_with_logging(async move {
                    let (server, source_bucket, target_bucket) = test_setup(true).await?;
                    let test_data = test_data(&server, &source_bucket).await?;
                    let source_archive_url = create_archive_on_s3(&server, &source_bucket).await?;
                    let source_archive = ssstar::SourceArchive::ObjectStorage(source_archive_url);

                    let job = make_job(&server,
                        source_archive,
                        &target_bucket,
                        "").await?;

                    run_job(job).await?;

                    validate_bucket_contents(&server, test_data, &target_bucket, "").await?;
                    Ok(())
                })
            }

            /// Extract from a tar on the local filesystem to a bucket with no prefix
            #[test]
            fn extract_from_archive_file_to_bucket_root() -> Result<()> {
                ssstar_testing::logging::test_with_logging(async move {
                    let (server, source_bucket, target_bucket) = test_setup(true).await?;
                    let test_data = test_data(&server, &source_bucket).await?;
                    let (_temp_dir, tar_path) = create_archive_on_disk(&server, &source_bucket).await?;
                    let source_archive = ssstar::SourceArchive::File(tar_path);

                    let job = make_job(&server,
                        source_archive,
                        &target_bucket,
                        "").await?;

                    run_job(job).await?;

                    validate_bucket_contents(&server, test_data, &target_bucket, "").await?;
                    Ok(())
                })
            }

            /// Extract from a tar exposed as a generic `Read` to a bucket with no prefix
            #[test]
            fn extract_from_archive_reader_to_bucket_root() -> Result<()> {
                ssstar_testing::logging::test_with_logging(async move {
                    let (server, source_bucket, target_bucket) = test_setup(true).await?;
                    let test_data = test_data(&server, &source_bucket).await?;
                    let (_temp_dir, tar_path) = create_archive_on_disk(&server, &source_bucket).await?;
                    let reader = std::fs::File::open(&tar_path)?;
                    let source_archive = ssstar::SourceArchive::Reader(Box::new(reader));

                    let job = make_job(&server,
                        source_archive,
                        &target_bucket,
                        "").await?;

                    run_job(job).await?;

                    validate_bucket_contents(&server, test_data, &target_bucket, "").await?;
                    Ok(())
                })
            }
        }
    }
}
    /// Attempt to extract a non-existent archive
    #[test]
    fn empty_bucket() -> Result<()> {
        ssstar_testing::logging::test_with_logging(async move {
            let server = minio::MinioServer::get().await?;
            let bucket = server.create_bucket("empty", false).await?;
            let source_archive = ssstar::SourceArchive::ObjectStorage(
                format!("s3://{}/test.tar", bucket).parse().unwrap(),
            );
            let target: Url = format!("s3://{}/target/", bucket).parse().unwrap();

            let result = ssstar::ExtractArchiveJobBuilder::new(
                config_for_minio(&server),
                source_archive,
                target,
            )
            .await?
            .build()
            .await;

            assert_matches!(result, Err(S3TarError::ObjectNotFound { .. }));

            Ok(())
        })
    }

    // Perform a simple test with a single object and no filters
    extract_test! {
        small_object_no_filters {
            @bucket_contents: ["test" => "1KiB"],
            @filters: [],
            @expected_objects: ["test"]
        }
    }

    // Archive with one file large enough that it will be restored with multipart
    extract_test! {
        large_object_no_filters {
            @bucket_contents: ["test" => "20MiB"],
            @filters: [],
            @expected_objects: ["test"]
        }
    }

    // Filter the extraction by specific named objects
    extract_test! {
        filter_by_object_name {
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
            @filters: ["test1", "test5", "test9"],
            @expected_objects: ["test1", "test5", "test9"]
        }
    }

    // Filter the extraction by specific prefixes
    extract_test! {
        filter_by_prefix {
            @bucket_contents: [
                "foo/test1" => "1KiB",
                "foo/test2" => "1KiB",
                "foo/test3" => "1KiB",
                "foo/test4" => "1KiB",
                "foo/test5" => "1KiB",
                "foo/test6" => "1KiB",
                "foo/test7" => "1KiB",
                "foo/test8" => "1KiB",
                "foo/test9" => "1KiB",
                "foo/test10" => "1KiB",
                "bar/test11" => "1KiB",
                "bar/test12" => "1KiB",
                "bar/test13" => "1KiB",
                "bar/test14" => "1KiB",
                "bar/test15" => "1KiB",
                "bar/test16" => "1KiB",
                "bar/test17" => "1KiB",
                "bar/test18" => "1KiB",
                "bar/test19" => "1KiB",
                "bar/test20" => "1KiB"
            ],
            @filters: ["bar/"],
            @expected_objects: [
                "bar/test11",
                "bar/test12",
                "bar/test13",
                "bar/test14",
                "bar/test15",
                "bar/test16",
                "bar/test17",
                "bar/test18",
                "bar/test19",
                "bar/test20"
            ]
        }
    }

    // Filter the extraction by specific globs
    extract_test! {
        filter_by_glob {
            @bucket_contents: [
                "foo/test1.txt" => "1KiB",
                "foo/test2.dat" => "1KiB",
                "foo/test3.bin" => "1KiB",
                "foo/test4.txt" => "1KiB",
                "foo/test5.doc" => "1KiB",
                "foo/test6.sql" => "1KiB",
                "foo/test7.txt" => "1KiB",
                "foo/test8.dat" => "1KiB",
                "foo/test9.foo" => "1KiB",
                "foo/test10.bar" => "1KiB",
                "bar/test11.bin" => "1KiB",
                "bar/test12.doc" => "1KiB",
                "bar/test13.sql" => "1KiB",
                "bar/test14.txt" => "1KiB",
                "bar/test15.dat" => "1KiB",
                "bar/test16.foo" => "1KiB",
                "bar/test17.html" => "1KiB",
                "bar/test18.xml" => "1KiB",
                "bar/test19.sav" => "1KiB",
                "bar/test20.wav" => "1KiB"
            ],
            @filters: ["**/*.txt", "bar/*.bin"],
            @expected_objects: [
                "foo/test1.txt",
                "foo/test4.txt",
                "foo/test7.txt",
                "bar/test11.bin",
                "bar/test14.txt"
            ]
        }
    }
}
