//! Create test data in S3-compatible object storage
use crate::Result;
use aws_sdk_s3::{types::ByteStream, Client};
use bytes::Bytes;
use futures::StreamExt;
use rand::prelude::*;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    path::Path,
};
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
        let object = TestObjectWithData {
            url: format!("s3://{}/{}", bucket, key).parse().unwrap(),
            key: key.clone(),
            data,
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
    let expected_test_data: HashMap<Cow<'static, str>, &TestObjectWithData> = expected_keys.into_iter()
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

    for file in files {
        let key = file.to_string_lossy();
        if !expected_keys.remove(key.as_ref()) {
            // There was no object with this name
            panic!(
                "Tar archive contains file `{}` which is not among the expected test data",
                file.display()
            );
        }
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
