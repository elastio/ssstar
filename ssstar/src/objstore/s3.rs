use super::{Bucket, ObjectStorage};
use crate::{create, Config, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_smithy_http::endpoint::Endpoint;
use aws_types::region::Region;
use futures::{Stream, StreamExt};
use snafu::{prelude::*, IntoError};
use std::{any::Any, sync::Arc};
use tracing::debug;
use url::Url;

/// Implementation of [`ObjectStorage`] for S3 and S3-compatible APIs
#[derive(Clone)]
pub(super) struct S3 {
    inner: Arc<S3Inner>,
}
struct S3Inner {
    config: Config,
    client: aws_sdk_s3::Client,
}

impl S3 {
    pub(super) async fn new(config: Config) -> Self {
        Self {
            inner: Arc::new(S3Inner {
                client: make_s3_client(&config, None).await,
                config,
            }),
        }
    }
}

#[async_trait::async_trait]
impl ObjectStorage for S3 {
    async fn extract_bucket_from_url(&self, url: &Url) -> Result<Box<dyn Bucket>> {
        // S3 URLs are of the form:
        // s3://bucket/path
        // In URL terms, the `bucket` part is considered the host name.
        let bucket = url
            .host_str()
            .ok_or_else(|| crate::error::MissingBucketSnafu { url: url.clone() }.build())?;

        Ok(Box::new(S3Bucket::new(self, bucket).await?))
    }
}

impl std::fmt::Debug for S3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3")
    }
}

#[derive(Clone)]
struct S3Bucket {
    inner: Arc<S3BucketInner>,
}
struct S3BucketInner {
    name: String,

    objstore: S3,

    /// Flag indicating if the S3 versioning feature is enabled on this bucket.
    ///
    /// If versioning is enabled, ssstar will use it to ensure the version of the object discovered
    /// initially when enumerating archive inputs is the same version actually added to the
    /// archive.
    ///
    /// If versioning isn't enabled, it's not possible to provide this guarantee, and in can happen
    /// that an object is overwritten from the time the create operation is initiated to the time
    /// the object is finally read.
    versioning_enabled: bool,

    /// The region this bucket is located in, if it's different from the region specified in the
    /// AWS SDK config.
    ///
    /// If a bucket is in a different region, then we need to use a different
    /// [`aws_sdk_s3::Client`] instance to talk to the S3 APIs when dealing with that bucket.
    region: Option<String>,

    /// The client to use to operate on this bucket.
    client: aws_sdk_s3::Client,
}

impl S3Bucket {
    /// Construct a new instance and validate that the current client has access to the bucket.
    ///
    /// If there is no access to the bucket then fail with an error
    async fn new(objstore: &S3, name: &str) -> Result<Self> {
        debug!(bucket = name, "Validating access to bucket");

        let mut client = objstore.inner.client.clone();

        // If the bucket is in a different region, `head_bucket` will fail and the error will
        // include a header telling us the correct region.  Look for that and handle it properly.
        let region = if let Some(region) = Self::validate_access_and_region(&client, name).await? {
            // This bucket is in a different region.  Oops.
            debug!(bucket = name, %region, "Bucket is in another region; repeating access validation in the correct region");

            client = make_s3_client(&objstore.inner.config, region.clone()).await;

            // Repeat the validation again.
            // This can fail if we don't have access, but if it reports again that the region is
            // wrong then something has gone really wrong, or (more likely) there's a bug in our
            // code.
            assert_eq!(
                Self::validate_access_and_region(&client, name).await?,
                None,
                "S3 has already redirected us to another region once before"
            );

            Some(region)
        } else {
            // Bucket is in the default region so no override needed
            None
        };

        debug!(bucket = name, ?region, "Access to bucket is confirmed");

        // Now query if versioning is enabled on the bucket.  If it is we need to know now as it
        // will change how we perform the create archive operation

        let versioning = client
            .get_bucket_versioning()
            .bucket(name)
            .send()
            .await
            .with_context(|_| crate::error::GetBucketVersioningSnafu {
                bucket: name.to_string(),
            })?;

        let versioning_enabled = match versioning.status {
            Some(status) => status == aws_sdk_s3::model::BucketVersioningStatus::Enabled,
            None => false,
        };

        debug!(
            bucket = name,
            versioning_enabled, "Checked if bucket versioning is enabled"
        );

        Ok(Self {
            inner: Arc::new(S3BucketInner {
                name: name.to_string(),
                objstore: objstore.clone(),
                versioning_enabled,
                region,
                client,
            }),
        })
    }

    /// Attempt to downcast a dyn trait `Bucket` implementation into an instance of this type.
    ///
    /// Panics with a meaningful error if `me` isn't an instance of `S3Bucket`
    fn from_dyn_trait(me: &dyn Bucket) -> &Self {
        me.as_any()
            .downcast_ref::<Self>()
            .expect("BUG: attempt to use a different impl of `Bucket` in place of `S3Bucket`")
    }

    /// Perform a HEAD operation on an object to get its current version ID if versioning is
    /// enabled on the bucket.
    ///
    /// Returns `None` if versioning is disabled.
    async fn get_object_version_id(&self, key: &str) -> Result<Option<String>> {
        if self.inner.versioning_enabled {
            let metadata = self
                .inner
                .client
                .head_object()
                .bucket(&self.inner.name)
                .key(Self::url_path_to_s3_path(key))
                .send()
                .await
                .with_context(|_| crate::error::HeadObjectSnafu {
                    bucket: self.inner.name.clone(),
                    key: key.to_string(),
                })?;

            Ok(metadata.version_id().map(|id| id.to_string()))
        } else {
            Ok(None)
        }
    }

    /// Given a paginated stream of S3 object listings, generate a vector of
    /// [`'create::InputObject`] instances describing the objects.
    ///
    /// This will perform additional S3 API calls if needed to get the object's version ID.  To the
    /// extent possible these calls will be performed in parallel to reduce latency, up to the
    /// maximum concurrency specified in [`crate::Config::max_concurrent_requests`]
    async fn objects_to_input_objects(
        &self,
        mut pages: impl Stream<Item = Result<Vec<aws_sdk_s3::model::Object>>> + Unpin,
    ) -> Result<Vec<create::InputObject>> {
        // Helpfully, the AWS Rust SDK provides conversions from their own internal DateTime type
        // to Chrono.
        use aws_smithy_types_convert::date_time::DateTimeExt;

        let mut input_objects = Vec::new();

        while let Some(result) = pages.next().await {
            let objects = result?;

            // Process the objects in this page of listings in parallel, since the call
            // to get the object version ID can have surprisingly high latency
            let object_futs = objects.into_iter().map(|object| {
                let bucket = self.inner.name.clone();
                let key = object
                    .key()
                    .expect("BUG: all objects have keys")
                    .to_string();

                let mut input_object = create::InputObject {
                    bucket: dyn_clone::clone_box(self),
                    key: key.clone(),
                    version_id: None,
                    size: object.size() as u64,
                    timestamp: object
                        .last_modified()
                        .expect("Objects always have a last modified time")
                        .to_chrono_utc(),
                };

                async move {
                    // If versioning is enabled at the bucket level, make another API call to
                    // get the object's version.  It's a pity that the list operation doesn't
                    // include the version ID
                    input_object.version_id = self.get_object_version_id(&key).await?;

                    Ok(input_object)
                }
            });

            // Use the buffer combinator to evaluate futures in parallel up to a maximum
            // degree of parallelism.  A listing of objects can contain up to 1000 items,
            // but if we hit S3 with 1000 parallel API calls we're likely to get throttled
            let mut objects_stream = futures::stream::iter(object_futs)
                .buffer_unordered(self.inner.objstore.inner.config.max_concurrent_requests);

            while let Some(result) = objects_stream.next().await {
                input_objects.push(result?);
            }
        }

        Ok(input_objects)
    }

    /// Perform a HEAD on the bucket to check access.
    ///
    /// If the HEAD check passes, it means the client's configured region is correct, the
    /// configured credentials have access to the bucket, and all is well.  In that case this
    /// function returns `Ok(None)`
    ///
    /// If the HEAD check fails with an error that indicates the bucket is in a different region,
    /// then this will return `Ok(Some($region))`, and the check should be repeated again in that
    /// region.
    ///
    /// If the HEAD check fails for any other error, most likely because the bucket doesn't exist
    /// or the credentials don't have access to it, then this returns the corresponding error.
    async fn validate_access_and_region(
        client: &aws_sdk_s3::Client,
        name: &str,
    ) -> Result<Option<String>> {
        if let Err(e) = client.head_bucket().bucket(name).send().await {
            if let aws_sdk_s3::types::SdkError::ServiceError { raw, .. } = &e {
                println!("{:#?}", raw);

                let response = raw.http();
                if response.status() == http::StatusCode::MOVED_PERMANENTLY {
                    if let Some(value) = response.headers().get("x-amz-bucket-region") {
                        if let Ok(region) = value.to_str() {
                            // This is AWS's way of telling us we have the right bucket, but it is in
                            // another region so we should use the appropriate region endpoint
                            return Ok(Some(region.to_string()));
                        }
                    }
                }
            };

            Err(crate::error::BucketInvalidOrNotAccessibleSnafu {
                bucket: name.to_string(),
            }
            .into_error(e))
        } else {
            Ok(None)
        }
    }

    /// Paths from URLs like `s3://bucket/prefix/object` always start with `/`, but that's not
    /// actually part of the S3 object key.  Fix such paths.
    ///
    /// Technically, the URL path *is* started by `/`, but S3's API doesn't work that way, it
    /// regards the `/` as a separator or delimiter which splits the bucket name and the object
    /// key.
    fn url_path_to_s3_path(key: &str) -> &str {
        if let Some(stripped) = key.strip_prefix('/') {
            stripped
        } else {
            key
        }
    }
}

#[async_trait::async_trait]
impl Bucket for S3Bucket {
    fn as_any(&self) -> &(dyn Any + Sync + Send) {
        self
    }

    fn objstore(&self) -> Box<dyn ObjectStorage> {
        dyn_clone::clone_box(&self.inner.objstore)
    }

    fn name(&self) -> &str {
        &self.inner.name
    }

    async fn list_matching_objects(
        &self,
        selector: create::ObjectSelector,
    ) -> Result<Vec<create::InputObject>> {
        // Helpfully, the AWS Rust SDK provides conversions from their own internal DateTime type
        // to Chrono.
        use aws_smithy_types_convert::date_time::DateTimeExt;

        match selector {
            create::ObjectSelector::Object { key, version_id } => {
                // This is the easy case.  The user has explicitly specified a single object.
                debug!(%key, bucket = %self.inner.name, "Archive input matches a single S3 object");

                let metadata = self
                    .inner
                    .client
                    .head_object()
                    .bucket(&self.inner.name)
                    .key(Self::url_path_to_s3_path(&key))
                    .set_version_id(version_id)
                    .send()
                    .await
                    .with_context(|_| crate::error::HeadObjectSnafu {
                        bucket: self.inner.name.clone(),
                        key: key.clone(),
                    })?;

                Ok(vec![create::InputObject {
                    bucket: dyn_clone::clone_box(self),
                    key,
                    version_id: metadata.version_id().map(|id| id.to_string()),
                    size: metadata.content_length() as u64,
                    timestamp: metadata
                        .last_modified()
                        .expect("Objects always have a last modified time")
                        .to_chrono_utc(),
                }])
            }
            create::ObjectSelector::Prefix { prefix } => {
                // Enumerate all objects within this prefix
                debug!(bucket = %self.inner.name, %prefix, "Archive input matches all S3 objects with a certain prefix");

                // Use the paginated API to automatically handle dealing with continuation tokens
                let pages = self
                    .inner
                    .client
                    .list_objects_v2()
                    .bucket(&self.inner.name)
                    .prefix(Self::url_path_to_s3_path(&prefix))
                    .into_paginator()
                    .send();

                // Translate this stream of pages of object listings into a stream of AWS SDK
                // 'Object' structs so we can process them one at a time
                let objects = pages.map(|result| {
                    let page = result.with_context(|_| crate::error::ListObjectsInPrefixSnafu {
                        bucket: self.inner.name.clone(),
                        prefix: prefix.clone(),
                    })?;

                    // NOTE: the `contents()` accessor returns a slice, but the `contents` field is
                    // actually public (but hidden from docs).  I'm probably not supposed to use
                    // this, and maybe it'll break in a future release, but for now this is much
                    // preferable because it means I can yield the owned `Vec` and not a ref which
                    // would not be possible to process as part of an async stream.
                    let result: Result<Vec<aws_sdk_s3::model::Object>> =
                        Ok(page.contents.expect("Object listings always have contents"));

                    result
                });

                let input_objects = self.objects_to_input_objects(objects).await?;

                debug!(input_objects = input_objects.len(), bucket = %self.inner.name, %prefix, "Matched all S3 objects with a certain prefix");

                Ok(input_objects)
            }
            create::ObjectSelector::Bucket => {
                // This is a simpler case of the above match arm, where the prefix is empty
                // (meaning all objects match)
                // Enumerate all objects within this prefix
                debug!(
                    bucket = %self.inner.name,
                    "Archive input matches everything in an S3 bucket"
                );

                // Use the paginated API to automatically handle dealing with continuation tokens
                let pages = self
                    .inner
                    .client
                    .list_objects_v2()
                    .bucket(&self.inner.name)
                    .into_paginator()
                    .send();

                // Translate this stream of pages of object listings into a stream of AWS SDK
                // 'Object' structs so we can process them one at a time
                let objects = pages.map(|result| {
                    let page = result.with_context(|_| crate::error::ListObjectsInBucketSnafu {
                        bucket: self.inner.name.clone(),
                    })?;

                    // NOTE: the `contents()` accessor returns a slice, but the `contents` field is
                    // actually public (but hidden from docs).  I'm probably not supposed to use
                    // this, and maybe it'll break in a future release, but for now this is much
                    // preferable because it means I can yield the owned `Vec` and not a ref which
                    // would not be possible to process as part of an async stream.
                    //
                    // NOTE 2: `contents` can actually be `None` if there are no objects in the
                    // bucket.
                    let result: Result<Vec<aws_sdk_s3::model::Object>> =
                        Ok(page.contents.unwrap_or_default());

                    result
                });

                let input_objects = self.objects_to_input_objects(objects).await?;

                debug!(
                    input_objects = input_objects.len(),
                    bucket = %self.inner.name,
                    "Matched all S3 objects in an S3 bucket"
                );

                Ok(input_objects)
            }
            create::ObjectSelector::Glob { pattern } => {
                // This is kind of a variation on the Bucket match arm.  List everything in the
                // bucket just like it does, but filter each individual object to see if it matches
                // the glob expression

                debug!(
                    bucket = %self.inner.name,
                    glob = %pattern,
                    "Archive input matches objects in a bucket which match a glob pattern"
                );

                // Before parsing the glob pattern, it needs to be translated from URL path to an
                // object name, as all other object keys are
                let s3_pattern = Self::url_path_to_s3_path(&pattern);
                let pattern = glob::Pattern::new(s3_pattern).with_context(|_| {
                    crate::error::InvalidGlobPatternSnafu {
                        pattern: s3_pattern.to_string(),
                    }
                })?;

                // Use the paginated API to automatically handle dealing with continuation tokens
                let pages = self
                    .inner
                    .client
                    .list_objects_v2()
                    .bucket(&self.inner.name)
                    .into_paginator()
                    .send();

                // Translate this stream of pages of object listings into a stream of AWS SDK
                // 'Object' structs so we can process them one at a time
                let objects = pages.map(|result| {
                    let page = result.with_context(|_| crate::error::ListObjectsInBucketSnafu {
                        bucket: self.inner.name.clone(),
                    })?;

                    // Apply the glob to the vec of Object entries, filtering out any whose
                    // complete object key doesn't match the glob
                    //
                    // As the bucket can be empty, we have to account for the possibility that
                    // `contents` is `None`
                    let contents = page
                        .contents
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|object| {
                            // I think objects always have a key, the fact that this is `Option` is
                            // just an artifact of the machine-generated Rust bindings
                            let key = object.key().expect("Objects must have keys");

                            pattern.matches(key)
                        })
                        .collect::<Vec<_>>();

                    Ok(contents)
                });

                let input_objects = self.objects_to_input_objects(objects).await?;

                debug!(
                    input_objects = input_objects.len(),
                    bucket = %self.inner.name,
                    glob = %pattern,
                    "Matched all S3 objects in an S3 bucket which match the specified glob"
                );

                Ok(input_objects)
            }
        }
    }
}

impl std::fmt::Debug for S3Bucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Bucket")
            .field("name", &self.inner.name)
            .field("versioning_enabled", &self.inner.versioning_enabled)
            .field("region", &self.inner.region)
            .field("client", &"<...>")
            .finish()
    }
}

/// Create a new AWS SDK S3 client, using either an explicit region or the default configuration
/// deduced from the environment
async fn make_s3_client(config: &Config, region: impl Into<Option<String>>) -> aws_sdk_s3::Client {
    let region = region.into();

    let region_provider = if let Some(region) = region {
        RegionProviderChain::first_try(Region::new(region))
    } else {
        // No explicit region; use the environment
        RegionProviderChain::default_provider().or_else("us-east-1")
    };
    let aws_config = aws_config::from_env().region(region_provider).load().await;

    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Some(s3_endpoint) = &config.s3_endpoint {
        // AWS SDK uses the `Uri` type in `http`.  There doesn't seem to be an easy way to
        // convert between the two...
        let uri: http::Uri = s3_endpoint.to_string().parse().unwrap_or_else(|e| {
            panic!(
                "BUG: URL '{}' could not be converted into Uri: {}",
                s3_endpoint, e
            )
        });

        s3_config_builder = s3_config_builder.endpoint_resolver(Endpoint::immutable(uri));
    }
    let aws_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    aws_client
}

#[cfg(test)]
mod tests {
    use super::*;
}
