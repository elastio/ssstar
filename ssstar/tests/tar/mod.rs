//! Helpers dealing with tar archives
use crate::Result;
use tempdir::TempDir;

/// Extract a tar archive that is specified by a `TargetArchive` enum.
///
/// Note that the variant `Writer` isn't supported since there's no way to read from the custom
/// arbitrary writer.
pub async fn extract_tar_archive_for_target(
    client: &aws_sdk_s3::Client,
    target: &ssstar::TargetArchive,
) -> Result<TempDir> {
    match target {
        ssstar::TargetArchive::ObjectStorage(url) => {
            ssstar_testing::tar::extract_tar_archive_from_s3_url(client, url).await
        }
        ssstar::TargetArchive::File(path) => {
            ssstar_testing::tar::extract_tar_archive_from_reader(tokio::fs::File::open(path).await?)
                .await
        }

        ssstar::TargetArchive::Writer(_) => {
            panic!("Can't read from a writer")
        }
    }
}
