//! Test helpers for working with tar archives, such as extracting them to temp dirs so their
//! contents can be validated against expected test data.
use crate::Result;
use std::path::Path;
use tempdir::TempDir;
use tokio::io::AsyncRead;
use url::Url;

/// Given an async reader that will expose the contents of the tar archive, extract that archive to
/// a temp directory and return the temp directory itself
pub async fn extract_tar_archive_from_reader<R: AsyncRead + Unpin + Send + 'static>(
    reader: R,
) -> Result<TempDir> {
    // The tar crate operates only on blocking I/O so we'll need a bridge
    let bridge = tokio_util::io::SyncIoBridge::new(reader);

    let temp_dir = tempdir::TempDir::new("ssstar-testing-tar")?;
    let path = temp_dir.path().to_owned();

    tokio::task::spawn_blocking(move || {
        let mut archive = tar::Archive::new(bridge);

        // There's a `unpack` method that extracts the entire thing all at once but let's print out
        // info about each file in the archive to make troubleshooting tests easier
        println!("Tar archive contents extracing to {}:", path.display());
        for result in archive.entries()? {
            let mut entry = result?;
            let entry_path = entry.path()?.to_path_buf();

            println!("  {} ({} bytes)", entry_path.display(), entry.size());

            assert!(!path.join(&entry_path).exists());
            assert!(entry.unpack_in(&path)?, "unpack_in returned false");
            assert!(path.join(&entry_path).exists());
        }

        Result::<_>::Ok(())
    })
    .await??;

    Ok(temp_dir)
}

/// Extract a tar archive from S3-compatible object storage
pub async fn extract_tar_archive_from_s3_url(
    client: &aws_sdk_s3::Client,
    url: &Url,
) -> Result<TempDir> {
    let bucket = url.host().unwrap().to_string();
    let key = url.path().strip_prefix('/').unwrap();

    let response = client.get_object().bucket(bucket).key(key).send().await?;

    let bytestream = response.body;

    // Package this byte stream into `AsyncRead` which we already know how to handle
    let reader = bytestream.into_async_read();

    extract_tar_archive_from_reader(reader).await
}

/// Extract a tar archive from a local file
pub async fn extract_tar_archive_from_file(path: &Path) -> Result<TempDir> {
    let file = tokio::fs::File::open(path).await?;

    // Tokio `File` types implement `AsyncRed`
    extract_tar_archive_from_reader(file).await
}
