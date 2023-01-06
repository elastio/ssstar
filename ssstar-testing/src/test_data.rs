//! Create test data in S3-compatible object storage
use crate::Result;
use aws_sdk_s3::{types::ByteStream, Client};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use rand::prelude::*;
use regex::Regex;
use sha2::Digest;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    path::Path,
};
use tokio::io::AsyncReadExt;
use tracing::{debug, instrument};
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

/// Generate a unique prefix ending in a `/` character, and prepend it to the `key` in a collection
/// of [`TestObject`]s.
///
/// Returns the unique prefix an an iterator that yields the modified test objects.
///
/// This is useful when running tests against a real S3 bucket, where multiple runs of the same
/// test may write to the bucket so each test's object keys must be unique
pub fn prepend_unique_prefix(
    objects: impl IntoIterator<Item = TestObject>,
) -> (String, impl IntoIterator<Item = TestObject>) {
    let prefix = format!("{:08x}/", rand::thread_rng().next_u32());

    let objects = {
        let prefix = prefix.clone();

        objects.into_iter().map(move |mut object| {
            object.key = format!("{}{}", prefix, object.key);

            object
        })
    };

    (prefix, objects)
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
    let mut expected_test_data: HashMap<String, &TestObjectWithData> = expected_keys.into_iter()
        .map(|item| {
            let key = item.into();
            let data = test_data.get(key.as_ref())
                .unwrap_or_else(|| panic!("BUG: test specifies expected key '{key}' but the `test_data` collection doesn't have such an entry"));

            // On Windows, the file paths listed in `files` will use `\` path separators, but our
            // tests always specify expected keys using `/` separators.  Rewrite the expected keys
            // here
            #[cfg(windows)]
            let key = key.replace('/', "\\");

            #[cfg(not(windows))]
            let key = key.to_string();

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
#[instrument(err, skip_all, fields(bucket, prefix))]
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

    for (key, _object) in objects {
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
        //
        // This usually requires reading the entire object back and calculating the hash, even
        // though the extract operation populates the SHA256 hash header.  That's not enough to
        // avoid having to read the whole data because:
        //
        // - For multi-part uploads, the SHA256 checksum that gets reported is a hash of the hashes
        // of all of the parts, not the hash of the contents.  When we generate test data we don't
        // know how it will be broken up into parts, so we dont' know these per-part hashes.  That
        // means for the biggest of objects (by nature multipart uploads are performed on objects
        // large enough to exceed the multipart threshold) we still have to download and recompute
        // the contents
        // - As of January 2023, the latest MinIO is a new and interesting kind of broken.  It
        // computes SHA256 hashes for multipart uploads, but it does it in a way that doesn't match
        // the AWS behavior, so rather than try to guess which S3 impl this is and calculate the
        // hash in the appropriate way, it's easier to just download the object and compute its
        // hash.
        let hash = {
            let response = client.get_object().bucket(bucket).key(&key).send().await?;

            let mut body = response.body;
            let mut hasher = sha2::Sha256::new();
            while let Some(bytes) = body.try_next().await? {
                hasher.update(bytes);
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&hasher.finalize());
            hash
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
