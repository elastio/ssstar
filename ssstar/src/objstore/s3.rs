use super::ObjectStorage;
use crate::{create, Config, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_smithy_http::endpoint::Endpoint;
use futures::{Stream, StreamExt};
use snafu::prelude::*;
use std::sync::Arc;
use tracing::debug;
use url::Url;

/// Implementation of [`ObjectStorage`] for S3 and S3-compatible APIs
#[derive(Debug)]
pub(super) struct S3 {
    config: Config,
    client: aws_sdk_s3::Client,
}

impl S3 {
    pub(super) async fn new(config: Config) -> Self {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
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

        Self {
            config,
            client: aws_client,
        }
    }

    /// Perform a HEAD operation on an object to get its current version ID if versioning is
    /// enabled on the bucket.
    ///
    /// Returns `None` if versioning is disabled.
    async fn get_object_version_id(
        &self,
        bucket: &Arc<create::Bucket>,
        key: &str,
    ) -> Result<Option<String>> {
        if bucket.versioning_enabled {
            let metadata = self
                .client
                .head_object()
                .bucket(&bucket.name)
                .key(Self::url_path_to_s3_path(key))
                .send()
                .await
                .with_context(|_| crate::error::HeadObjectSnafu {
                    bucket: bucket.name.clone(),
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
        bucket: &Arc<create::Bucket>,
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
                let bucket = bucket.clone();
                let key = object
                    .key()
                    .expect("BUG: all objects have keys")
                    .to_string();

                let mut input_object = create::InputObject {
                    bucket: bucket.clone(),
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
                    input_object.version_id = self.get_object_version_id(&bucket, &key).await?;

                    Ok(input_object)
                }
            });

            // Use the buffer combinator to evaluate futures in parallel up to a maximum
            // degree of parallelism.  A listing of objects can contain up to 1000 items,
            // but if we hit S3 with 1000 parallel API calls we're likely to get throttled
            let mut objects_stream = futures::stream::iter(object_futs)
                .buffer_unordered(self.config.max_concurrent_requests);

            while let Some(result) = objects_stream.next().await {
                input_objects.push(result?);
            }
        }

        Ok(input_objects)
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
impl ObjectStorage for S3 {
    async fn extract_bucket_from_url(&self, url: &Url) -> Result<String> {
        // S3 URLs are of the form:
        // s3://bucket/path
        // In URL terms, the `bucket` part is considered the host name.
        let bucket = url
            .host_str()
            .ok_or_else(|| crate::error::MissingBucketSnafu { url: url.clone() }.build())?;

        debug!(bucket, "Validating access to bucket");

        self.client
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
            .with_context(|_| crate::error::BucketInvalidOrNotAccessibleSnafu {
                bucket: bucket.to_string(),
            })?;

        debug!(bucket, "Access to bucket is confirmed");

        Ok(bucket.to_string())
    }

    async fn is_bucket_versioning_enabled(&self, bucket: &str) -> Result<bool> {
        let versioning = self
            .client
            .get_bucket_versioning()
            .bucket(bucket)
            .send()
            .await
            .with_context(|_| crate::error::GetBucketVersioningSnafu {
                bucket: bucket.to_string(),
            })?;

        let versioning_enabled = match versioning.status {
            Some(status) => status == aws_sdk_s3::model::BucketVersioningStatus::Enabled,
            None => false,
        };

        debug!(
            bucket,
            versioning_enabled, "Checked if bucket versioning is enabled"
        );

        Ok(versioning_enabled)
    }

    async fn list_matching_objects(
        &self,
        input: create::CreateArchiveInput,
    ) -> Result<Vec<create::InputObject>> {
        // Helpfully, the AWS Rust SDK provides conversions from their own internal DateTime type
        // to Chrono.
        use aws_smithy_types_convert::date_time::DateTimeExt;

        match input {
            create::CreateArchiveInput::Object {
                key,
                version_id,
                bucket,
            } => {
                // This is the easy case.  The user has explicitly specified a single object.
                debug!(%key, bucket = %bucket.name, "Archive input matches a single S3 object");

                let metadata = self
                    .client
                    .head_object()
                    .bucket(&bucket.name)
                    .key(Self::url_path_to_s3_path(&key))
                    .set_version_id(version_id)
                    .send()
                    .await
                    .with_context(|_| crate::error::HeadObjectSnafu {
                        bucket: bucket.name.clone(),
                        key: key.clone(),
                    })?;

                Ok(vec![create::InputObject {
                    bucket,
                    key,
                    version_id: metadata.version_id().map(|id| id.to_string()),
                    size: metadata.content_length() as u64,
                    timestamp: metadata
                        .last_modified()
                        .expect("Objects always have a last modified time")
                        .to_chrono_utc(),
                }])
            }
            create::CreateArchiveInput::Prefix { prefix, bucket } => {
                // Enumerate all objects within this prefix
                debug!(bucket = %bucket.name, %prefix, "Archive input matches all S3 objects with a certain prefix");

                // Use the paginated API to automatically handle dealing with continuation tokens
                let pages = self
                    .client
                    .list_objects_v2()
                    .bucket(&bucket.name)
                    .prefix(Self::url_path_to_s3_path(&prefix))
                    .into_paginator()
                    .send();

                // Translate this stream of pages of object listings into a stream of AWS SDK
                // 'Object' structs so we can process them one at a time
                let objects = pages.map(|result| {
                    let page = result.with_context(|_| crate::error::ListObjectsInPrefixSnafu {
                        bucket: bucket.name.clone(),
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

                let input_objects = self.objects_to_input_objects(&bucket, objects).await?;

                debug!(input_objects = input_objects.len(), bucket = %bucket.name, %prefix, "Matched all S3 objects with a certain prefix");

                Ok(input_objects)
            }
            create::CreateArchiveInput::Bucket(bucket) => {
                // This is a simpler case of the above match arm, where the prefix is empty
                // (meaning all objects match)
                // Enumerate all objects within this prefix
                debug!(bucket = %bucket.name, "Archive input matches everything in an S3 bucket");

                // Use the paginated API to automatically handle dealing with continuation tokens
                let pages = self
                    .client
                    .list_objects_v2()
                    .bucket(&bucket.name)
                    .into_paginator()
                    .send();

                // Translate this stream of pages of object listings into a stream of AWS SDK
                // 'Object' structs so we can process them one at a time
                let objects = pages.map(|result| {
                    let page = result.with_context(|_| crate::error::ListObjectsInBucketSnafu {
                        bucket: bucket.name.clone(),
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

                let input_objects = self.objects_to_input_objects(&bucket, objects).await?;

                debug!(input_objects = input_objects.len(), bucket = %bucket.name, "Matched all S3 objects in an S3 bucket");

                Ok(input_objects)
            }
            create::CreateArchiveInput::Glob { pattern, bucket } => {
                // This is kind of a variation on the Bucket match arm.  List everything in the
                // bucket just like it does, but filter each individual object to see if it matches
                // the glob expression
                todo!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
