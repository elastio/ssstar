//! Create test data in S3-compatible object storage
use crate::Result;
use aws_sdk_s3::{types::ByteStream, Client};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use rand::prelude::*;
use sha2::Digest;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    path::Path,
};
use tokio::io::AsyncReadExt;
use url::Url;

/// Max concurrent S3 operations when dealing with test data
const MAX_CONCURRENCY: usize = 10;

#[derive(Clone, Debug)]
pub struct TestObject {
    pub key: String,
    pub size: usize,
}

impl TestObject {
    /// Make a new test object spec with the size specified as a string so we can use
    /// human-friendly units like "10 KB" or "20 MiB"
    pub fn new(key: impl Into<String>, size: impl AsRef<str>) -> Self {
        let key = key.into();

        let size = byte_unit::Byte::from_str(size).unwrap();

        Self {
            key,
            size: size.get_bytes() as usize,
        }
    }
}

/// The same test object spec as in [`TestObject`], but with the data that is written to the object
/// as well
#[derive(Clone, Debug)]
pub struct TestObjectWithData {
    pub key: String,
    pub url: Url,
    pub data: Vec<u8>,
    pub hash: [u8; 32],
}

/// Generate one or more test objects in a bucket.
///
/// Each object has a size specified.  Random data will be generated for each object.
///
/// The return value upon success is the same test objects specified on input, but with test data
/// included.  The key to the hash table is the object key
pub async fn make_test_data(
    client: &Client,
    bucket: &str,
    objects: impl IntoIterator<Item = TestObject>,
) -> Result<HashMap<String, TestObjectWithData>> {
    let create_futs = objects.into_iter().map(|test_object| async move {
        let data =
            make_test_data_object(client, bucket, &test_object.key.clone(), test_object.size)
                .await?;

        Result::<_>::Ok((test_object.key, data))
    });

    // Run these futures in parallel as part of a stream
    let mut test_data_stream = futures::stream::iter(create_futs).buffer_unordered(MAX_CONCURRENCY);

    let mut test_objects = HashMap::new();

    while let Some(result) = test_data_stream.next().await {
        let (key, data) = result?;

        let mut hasher = sha2::Sha256::new();
        hasher.update(&data);
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&hasher.finalize());

        let object = TestObjectWithData {
            url: format!("s3://{}/{}", bucket, key).parse().unwrap(),
            key: key.clone(),
            data,
            hash,
        };
        assert!(
            test_objects.insert(object.key.clone(), object).is_none(),
            "BUG: test data contains the same key '{}' more than once",
            key
        );
    }

    Ok(test_objects)
}

/// Generate test data for and upload a single test object
pub async fn make_test_data_object(
    client: &Client,
    bucket: &str,
    key: &str,
    size: usize,
) -> Result<Vec<u8>> {
    let mut rand = rand::thread_rng();
    let mut data = vec![0u8; size];

    rand.fill(&mut data[..]);

    client
        .put_object()
        .bucket(bucket)
        .key(key.to_string())
        .body(ByteStream::from(Bytes::from(data.clone())))
        .send()
        .await?;

    Result::<_>::Ok(data)
}

/// Validate the test data in a hash map against files in a directory where the file paths relative
/// to `path` correspond to the keys in [`TestObjectWithData`].
///
/// This assumes that the tar archive has been extracted to local storage somewhere for validation
/// purposes.
#[track_caller]
pub async fn validate_test_data_in_dir<Keys, Item>(
    test_data: &HashMap<String, TestObjectWithData>,
    path: &Path,
    expected_keys: Keys,
) -> Result<()>
where
    Keys: IntoIterator<Item = Item>,
    Item: Into<Cow<'static, str>>,
{
    // First recursively list all files in `path` (with their paths relative to `path` so it will
    // match the corresponding object store keys) and verify the files on disk and the expected
    // test data objects match exactly
    println!(
        "Test data dir  {} contains the following files:",
        path.display()
    );
    let files = walkdir::WalkDir::new(path)
        .into_iter()
        .filter(|result| {
            // We are not interested in directories, just files
            if let Ok(entry) = &result {
                !entry.file_type().is_dir()
            } else {
                // Errors should always be passed on to the next stage so they get reported
                true
            }
        })
        .map(|result| {
            let entry = result?;

            let relative_path = entry.path().strip_prefix(path)?.to_owned();
            println!(
                "  {} ({} bytes)",
                relative_path.display(),
                entry.path().metadata()?.len()
            );
            Result::<_>::Ok(relative_path)
        })
        .collect::<Result<Vec<_>>>()?;

    // Verify all of the expected keys are actually present in the `test_data` hash table, and make
    // a new hash table of just the expected keys
    let mut expected_test_data: HashMap<Cow<'static, str>, &TestObjectWithData> = expected_keys.into_iter()
        .map(|item| {
            let key = item.into();
            let data = test_data.get(key.as_ref())
                .unwrap_or_else(|| panic!("BUG: test specifies expected key '{key}' but the `test_data` collection doesn't have such an entry"));

            (key, data)
        })
        .collect();

    // Make a set of all expected object keys, and go file by file making sure there was an object key with
    // the same name, then remove it from the set so that we can also list excess object keys that
    // don't correspond to any files
    let mut expected_keys = expected_test_data
        .keys()
        .map(|key| key.to_string())
        .collect::<HashSet<_>>();

    for relative_path in files {
        let key = relative_path.to_string_lossy();
        let test_data = expected_test_data.remove(key.as_ref()).unwrap_or_else(|| {
            // There was no object with this name
            panic!(
                "Tar archive contains file `{}` which is not among the expected test data",
                relative_path.display()
            );
        });
        expected_keys.remove(key.as_ref());

        // Verify the file contents matches the expected data
        let mut file = tokio::fs::File::open(path.join(&relative_path)).await?;
        let metadata = file.metadata().await?;
        let mut data = Vec::with_capacity(metadata.len() as usize);

        file.read_to_end(&mut data).await?;

        let mut hasher = sha2::Sha256::new();
        hasher.update(&data);
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&hasher.finalize());

        assert_eq!(
            hash,
            test_data.hash,
            "File '{}' (key '{}') hash doesn't match expected value",
            relative_path.display(),
            key
        );
    }

    if !expected_keys.is_empty() {
        // Some test data objects were not present in the path
        panic!(
            "One or more test data objects were not found in the archive: {}",
            expected_keys.into_iter().collect::<Vec<_>>().join(",")
        )
    }

    Ok(())
}

/// Validate the test data in a hash map against an S3 bucket to which an archive containing the
/// test data has been extracted.
#[track_caller]
pub async fn validate_test_data_in_s3<Keys, Item>(
    client: &aws_sdk_s3::Client,
    test_data: &HashMap<String, TestObjectWithData>,
    bucket: &str,
    prefix: &str,
    expected_keys: Keys,
) -> Result<()>
where
    Keys: IntoIterator<Item = Item>,
    Item: Into<Cow<'static, str>>,
{
    // First list all objects in the bucket and prefix
    let mut objects = HashMap::new();

    let pages = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    // Translate this stream of pages of object listings into a stream of AWS SDK
    // 'Object' structs so we can process them one at a time
    let mut pages = pages.map(|result| {
        let page = result?;
        let result: Result<Vec<aws_sdk_s3::model::Object>> = Ok(page.contents.unwrap_or_default());

        result
    });

    while let Some(result) = pages.next().await {
        for object in result? {
            objects.insert(object.key().unwrap().to_owned(), object);
        }
    }

    // Verify all of the expected keys are actually present in the `test_data` hash table, and make
    // a new hash table of just the expected keys
    let mut expected_test_data: HashMap<Cow<'static, str>, &TestObjectWithData> = expected_keys.into_iter()
        .map(|item| {
            let key = item.into();
            let data = test_data.get(key.as_ref())
                .unwrap_or_else(|| panic!("BUG: test specifies expected key '{key}' but the `test_data` collection doesn't have such an entry"));

            (key, data)
        })
        .collect();

    // Make a set of all expected object keys, and go object by object making sure there was an object key with
    // the same name, then remove it from the set so that we can also list excess object keys that
    // don't correspond to any objects
    let mut expected_keys = expected_test_data
        .keys()
        .map(|key| key.to_string())
        .collect::<HashSet<_>>();

    for (key, object) in objects {
        // In the test data hashmap, keys are identified without whatever prefix was used when
        // extracting, so use that form here
        let relative_key = key.strip_prefix(prefix).unwrap();

        let test_data = expected_test_data.remove(relative_key).unwrap_or_else(|| {
            // There was no object with this name
            panic!(
                "Bucket contains object `{}` which is not among the expected test data",
                relative_key
            );
        });
        expected_keys.remove(relative_key);

        // Verify the object contents matches the expected data.
        // This doesn't require reading the data because the extract operation enables SHA256 hash
        // computation
        let object_hash = {
            let object = client
                .head_object()
                .bucket(bucket)
                .key(&key)
                .checksum_mode(aws_sdk_s3::model::ChecksumMode::Enabled)
                .send()
                .await?;

            let object = dbg!(object);

            object.checksum_sha256().map(|hash| hash.to_owned())
        };

        let hash = match object_hash {
            Some(object_hash) => {
                // The hash is expressed as a base64 encoded string.
                // Decode into a propery hash and then compare
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&base64::decode(object_hash).unwrap());
                hash
            }
            None => {
                // XXX: Actually the above statement would be correct, except that minio still doesn't
                // support the enhanced checksum algorithms that S3 does.
                //
                // https://github.com/minio/minio/issues/14885
                //
                // As of Late August 2022, it was something they seem to be working on but it's not
                // available yet.  Fuck!
                //
                // Fall back to reading the entire object contents
                // While minio doesn't support the AWS checksum algorithms we have to compute the checksum
                // by downloading the data
                let response = client.get_object().bucket(bucket).key(&key).send().await?;

                let mut body = response.body;
                let mut hasher = sha2::Sha256::new();
                while let Some(bytes) = body.try_next().await? {
                    hasher.update(bytes);
                }
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&hasher.finalize());
                hash
            }
        };

        assert_eq!(
            hash, test_data.hash,
            "S3 object '{}' (key '{}') hash doesn't match expected value",
            relative_key, key
        );
    }

    if !expected_keys.is_empty() {
        // Some test data objects were not present in the path
        panic!(
            "One or more test data objects were not found in the archive: {}",
            expected_keys.into_iter().collect::<Vec<_>>().join(",")
        )
    }

    Ok(())
}
