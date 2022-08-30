//! Tests which are intended to run against real live S3 APIs.
//!
//! These tests are limited to only those test cases that can't be exercised with minio, because
//! running live S3 tests is both more expensive to do, and also inconvenient for contributors
//! since these tests assume they can access Elastio-owned buckets specifically created for the
//! purposes of running these tests.
use crate::{
    progress::{TestCreateProgressCallback, TestExtractProgressCallback},
    Result,
};
use aws_config::meta::region::RegionProviderChain;
use aws_types::region::Region;
use ssstar::{Config, CreateArchiveJobBuilder, TargetArchive};
use ssstar_testing::test_data;
use url::Url;

// Details about a real live S3 bucket which can be used for S3 integration tests.
// This bucket is in the `assuriodev` AWS account and is only accessible to Elastio developers.
// To Elastio Developers: Set the AWS_PROFILE env var to the name of the AWS config profile that
// you use to access `assuriodev`.
// To non-Elastio developers: If you're trying to run these tests without access to Elastio AWS infra,
// you need to override this to a bucket that you do have access to.
//
// This bucket has a lifecycle policy configured that deletes all objects after 24 hours, so we
// don't have to worry about cleaning up results of tests.  If you do override this to point to
// another bucket, make sure it has the same lifecycle policy or you might get a surprise S3 bill.
const TEST_BUCKET: &str = dotenv!("TEST_S3_BUCKET");
const TEST_BUCKET_REGION: &str = dotenv!("TEST_S3_BUCKET_REGION");

async fn create_s3_client() -> Result<aws_sdk_s3::Client> {
    let region_provider = RegionProviderChain::first_try(Region::new(TEST_BUCKET_REGION));

    let aws_config_builder = aws_config::from_env().region(region_provider);

    let aws_config = aws_config_builder.load().await;

    let s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

    Ok(aws_sdk_s3::Client::from_conf(s3_config_builder.build()))
}

async fn run_test(test_data: Vec<test_data::TestObject>) -> Result<()> {
    let client = create_s3_client().await?;

    let (prefix, test_data) = test_data::prepend_unique_prefix(test_data);

    let test_data = test_data::make_test_data(&client, TEST_BUCKET, test_data).await?;

    // Ingest this test data, which is inside this unique prefix so we won't see any other test
    // data written to this bucket
    let archive_url: Url = format!("s3://{TEST_BUCKET}/{prefix}test.tar").parse()?;
    let target_archive = TargetArchive::ObjectStorage(archive_url.clone());
    let mut builder = CreateArchiveJobBuilder::new(Config::default(), target_archive);
    builder
        .add_input(&format!("s3://{TEST_BUCKET}/{prefix}**").parse()?)
        .await?;
    let job = builder.build().await?;

    let progress = TestCreateProgressCallback::new();
    job.run(futures::future::pending(), progress.clone())
        .await?;
    progress.sanity_check_updates();

    // There should be a SHA256 hash computed on the tar archive.  Minio doesn't support that
    // but live S3 does
    let archive_key = archive_url.path().strip_prefix("/").unwrap();
    let object = client
        .head_object()
        .bucket(TEST_BUCKET)
        .key(archive_key)
        .checksum_mode(aws_sdk_s3::model::ChecksumMode::Enabled)
        .send()
        .await?;

    assert!(object.checksum_sha256().is_some());

    // Archive is created; now extract it
    let source_archive = ssstar::SourceArchive::ObjectStorage(archive_url);

    // Even though the test data all have a shared prefix unique to this test, we need to extract
    // the archive containing those test data into a separate unique prefix, because the test data
    // validation code will subtract whatever prefix we specify from the object key because it
    // assumes the prefix is the prefix used when extracting, not a prefix which was shared by the
    // test data objects at archive creation time.
    let restore_prefix = format!("restore-{prefix}");
    let builder = ssstar::ExtractArchiveJobBuilder::new(
        Config::default(),
        source_archive,
        format!("s3://{TEST_BUCKET}/{restore_prefix}").parse()?,
    )
    .await?;

    let job = builder.build().await?;
    let progress = TestExtractProgressCallback::new();

    job.run(futures::future::pending(), progress.clone())
        .await?;

    progress.sanity_check_updates();

    test_data::validate_test_data_in_s3(
        &client,
        &test_data,
        TEST_BUCKET,
        &restore_prefix,
        test_data
            .keys()
            .into_iter()
            .map(|key| key.to_string())
            .collect::<Vec<_>>(),
    )
    .await?;

    Ok(())
}

/// A few small objects so the objects and the archive will both be under the multipart threshold
#[test]
#[ignore = "Run explicitly with --ignored if you're sure you have setup S3 access correctly"]
fn small_objects_archive_in_bucket() -> Result<()> {
    ssstar_testing::logging::test_with_logging(async move {
        let test_data = vec![
            test_data::TestObject::new("test1", "1KiB"),
            test_data::TestObject::new("test2", "1KiB"),
            test_data::TestObject::new("foo/test3", "1KiB"),
        ];

        run_test(test_data).await
    })
}

/// A bunch of objects which are each under the multipart threshold, but together they will produce
/// an archive that is over the multipart threshold
#[test]
#[ignore = "Run explicitly with --ignored if you're sure you have setup S3 access correctly"]
fn small_objects_multipart_archive_in_bucket() -> Result<()> {
    ssstar_testing::logging::test_with_logging(async move {
        let test_data = vec![
            test_data::TestObject::new("test1", "1MiB"),
            test_data::TestObject::new("test2", "2MiB"),
            test_data::TestObject::new("foo/test1", "2MiB"),
            test_data::TestObject::new("foo/test2", "2MiB"),
            test_data::TestObject::new("bar/test1", "2MiB"),
        ];

        run_test(test_data).await
    })
}

/// A mix of small and large objects, such that some of the objects themselves are over the
/// multipart threshold and thus will be read and extracted using the multi-part implementation
#[test]
#[ignore = "Run explicitly with --ignored if you're sure you have setup S3 access correctly"]
fn multipart_objects_multipart_archive_in_bucket() -> Result<()> {
    ssstar_testing::logging::test_with_logging(async move {
        let test_data = vec![
            test_data::TestObject::new("test1", "1MiB"),
            test_data::TestObject::new("test2", "9MiB"),
            test_data::TestObject::new("foo/test1", "8MiB"),
            test_data::TestObject::new("foo/test2", "9MiB"),
            test_data::TestObject::new("bar/test1", "10"),
        ];

        run_test(test_data).await
    })
}
