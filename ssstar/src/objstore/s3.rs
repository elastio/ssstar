use super::{Bucket, ObjectStorage};
use crate::{create, Config, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Credentials;
use aws_smithy_http::endpoint::Endpoint;
use aws_types::region::Region;
use futures::{Stream, StreamExt};
use snafu::{prelude::*, IntoError};
use std::{any::Any, ops::Range, sync::Arc};
use tokio::io::DuplexStream;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, instrument, warn, Instrument};
use url::Url;

const APP_NAME: &str = concat!(env!("CARGO_PKG_NAME"), "-", env!("VERGEN_BUILD_SEMVER"),);

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
                let _bucket = self.inner.name.clone();
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

    /// Upload the object identified by `key` using the S3 multipart upload APIs
    ///
    /// The chunks to upload are obtained from a writer task and exposed via `chunks_receiver`.
    ///
    /// The `progress_sender` should be sent an update whenever a chunk is successfully uploaded.
    /// The update is just the number of bytes uploaded for that chunk.  Note that some callers
    /// don't care about updates and will drop the corresponding progress receiver, so a failure to
    /// send on this channel should be ignored.
    #[instrument(skip(self, chunks_receiver, progress_sender), fields(bucket = %self.inner.name))]
    async fn multipart_object_writer(
        &self,
        key: String,
        upload_id: String,
        chunks_receiver: mpsc::Receiver<crate::writers::MultipartChunk>,
        progress_sender: mpsc::UnboundedSender<u64>,
    ) -> Result<u64> {
        // `Receiver` can be made to implement `Stream` which will let us move the heavy lifting
        // off onto `futures`
        let chunks = tokio_stream::wrappers::ReceiverStream::new(chunks_receiver);

        let chunk_futs = chunks.map(|chunk| {
            // Our chunking code numbers multipart chunks from 0, but the S3 API expects them
            // to be numbered from 1
            let part_number = chunk.part_number + 1;
            let chunk_size = chunk.data.len();

            let span = tracing::debug_span!("upload chunk", part_number, chunk_size);
            let me = self.clone();
            let key = key.clone();
            let upload_id = upload_id.clone();
            let progress_sender = progress_sender.clone();

            async move {
                debug!("Uploading multi-part chunk");

                // TODO: compute SHA-256 hash of chunk and include in upload

                let response = me
                    .inner
                    .client
                    .upload_part()
                    .bucket(me.inner.name.clone())
                    .key(&key)
                    .upload_id(upload_id)
                    .part_number(part_number as i32)
                    .body(aws_sdk_s3::types::ByteStream::from(chunk.data))
                    .send()
                    .await
                    .with_context(|_| crate::error::UploadPartSnafu {
                        bucket: me.inner.name.clone(),
                        key: key.clone(),
                        part_number,
                    })?;

                let e_tag = response
                    .e_tag()
                    .expect("BUG: uploaded part missing etag")
                    .to_string();

                debug!(%e_tag, "Uploaded multi-part chunk");

                let _ = progress_sender.send(chunk_size as u64);

                // Once all of the uploads are done we must provide the information about each part
                // to the CompleteMultipartUpload call, so retain the key bits here
                let completed_part = aws_sdk_s3::model::CompletedPart::builder()
                    .e_tag(e_tag)
                    .part_number(part_number as i32)
                    .build();

                Ok((chunk_size, completed_part))
            }
            .instrument(span)
        });

        debug!("Commencing multi-part upload");

        // Use the magic of `buffer_unordered` to poll these chunk uploading futures up to a
        // maximum concurrency level to honor the configured max parallel requests
        let mut uploaded_chunk_sizes =
            chunk_futs.buffer_unordered(self.inner.objstore.inner.config.max_concurrent_requests);

        let mut total_bytes = 0u64;
        let mut total_parts = 0usize;
        let mut completed_parts = Vec::new();

        while let Some(result) = uploaded_chunk_sizes.next().await {
            let (chunk_size, completed_part) = result?;

            total_bytes += chunk_size as u64;
            total_parts += 1;

            completed_parts.push(completed_part);
        }

        debug!(
            total_parts,
            total_bytes, "All parts uploaded; completing multi-part upload"
        );

        // AWS is so lazy that they not only require we specify all of the parts we uploaded (even
        // though they are all tied together with a unique upload ID), we also have to sort them in
        // order of part number.  AWS could trivially do that on their side, even in what I imagine
        // is their incredibly gnarly Java codebase, but they dont'.
        completed_parts.sort_unstable_by_key(|part| part.part_number());

        self.inner
            .client
            .complete_multipart_upload()
            .bucket(self.inner.name.clone())
            .key(key.clone())
            .upload_id(upload_id.clone())
            .multipart_upload(
                aws_sdk_s3::model::CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .await
            .with_context(|_| crate::error::CompleteMultipartUploadSnafu {
                bucket: self.inner.name.clone(),
                key: key.clone(),
            })?;

        Ok(total_bytes)
    }

    /// Upload the object identified by `key` using the S3 upload API that takes a single binary
    /// payload for the entire object.
    ///
    /// The chunk to upload is obtained from a writer task and exposed via `chunks_receiver`.
    ///
    /// The `progress_sender` should be sent an update whenever a chunk is successfully uploaded.
    /// The update is just the number of bytes uploaded for that chunk.  Note that some callers
    /// don't care about updates and will drop the corresponding progress receiver, so a failure to
    /// send on this channel should be ignored.
    #[instrument(skip(self, chunks_receiver, progress_sender), fields(bucket = %self.inner.name))]
    async fn unipart_object_writer(
        &self,
        key: String,
        chunks_receiver: oneshot::Receiver<bytes::Bytes>,
        progress_sender: mpsc::UnboundedSender<u64>,
    ) -> Result<u64> {
        // It seems a bit clumsy to do this single chunk upload in an async background task instead
        // of just doing it directly in this method, but the same code in `create` and `extract`
        // needs to work with both small objects that aren't big enough to qualify for multi-part,
        // and larger objects that do need it.  So we get to do this trival upload in a round-about
        // way
        let bytes = chunks_receiver.await.map_err(|_| {
            crate::error::UnipartUploadAbandonedSnafu {
                bucket: self.inner.name.clone(),
                key: key.clone(),
            }
            .build()
        })?;
        let total_bytes = bytes.len() as u64;

        debug!(total_bytes, "Uploading unipart object");

        self.inner
            .client
            .put_object()
            .bucket(self.inner.name.clone())
            .key(key.clone())
            .body(aws_sdk_s3::types::ByteStream::from(bytes))
            .send()
            .await
            .with_context(|_| crate::error::PutObjectSnafu {
                bucket: self.inner.name.clone(),
                key: key.clone(),
            })?;

        let _ = progress_sender.send(total_bytes);

        Ok(total_bytes)
    }

    /// Given a potentially huge range of bytes, break it up into small pieces according to the
    /// multipart config
    fn split_range_into_multipart(&self, range: Range<u64>) -> impl Iterator<Item = Range<u64>> {
        let config = &self.inner.objstore.inner.config;
        let threshold = config.multipart_threshold.get_bytes() as u64;
        let chunk_size = config.multipart_chunk_size.get_bytes() as u64;

        struct PartIterator {
            threshold: u64,
            chunk_size: u64,
            range: Range<u64>,
            next_offset: u64,
        }

        impl Iterator for PartIterator {
            type Item = Range<u64>;

            fn next(&mut self) -> Option<Self::Item> {
                if self.next_offset < self.range.end {
                    if self.range.end - self.range.start < self.threshold {
                        // This range isn't big enough to justify splitting into multiple parts.
                        // Return a single part and nothing more
                        self.next_offset = self.range.end;
                        Some(self.range.clone())
                    } else {
                        let chunk_len = self.chunk_size.min(self.range.end - self.next_offset);
                        let next_range = self.next_offset..self.next_offset + chunk_len;
                        self.next_offset += chunk_len;

                        Some(next_range)
                    }
                } else {
                    None
                }
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                // Approximate the expected size based on the range and the thunk size
                let remaining = self.range.end - self.next_offset;
                let chunks = (remaining + self.chunk_size - 1) / self.chunk_size;

                (chunks as usize, Some(chunks as usize))
            }
        }

        PartIterator {
            threshold,
            chunk_size,
            range,
            next_offset: 0,
        }
        .fuse()
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
        use aws_sdk_s3::types::SdkError;
        use aws_smithy_types_convert::date_time::DateTimeExt;

        match selector {
            create::ObjectSelector::Object { key, version_id } => {
                // This is the easy case.  The user has explicitly specified a single object.
                let key = Self::url_path_to_s3_path(&key);

                debug!(key, bucket = %self.inner.name, "Archive input matches a single S3 object");

                let result = self
                    .inner
                    .client
                    .head_object()
                    .bucket(&self.inner.name)
                    .key(key)
                    .set_version_id(version_id)
                    .send()
                    .await;

                let metadata = match result {
                    Err(err) => {
                        // If the error here is that the object is not found, throw that specific
                        // error as it provides more meaningful context then the generic
                        // `HeadObjectSnafu` error
                        if let SdkError::ServiceError {
                            err: service_err, ..
                        } = &err
                        {
                            if service_err.is_not_found() {
                                return Err(crate::error::ObjectNotFoundSnafu {
                                    bucket: self.inner.name.clone(),
                                    key: key.to_string(),
                                }
                                .into_error(snafu::NoneError));
                            }
                        }

                        // A non-service error, or a service error that isn't not found, should be
                        // reported as a generic HeadObject error
                        return Err(crate::error::HeadObjectSnafu {
                            bucket: self.inner.name.clone(),
                            key: key.to_string(),
                        }
                        .into_error(err));
                    }
                    Ok(metadata) => metadata,
                };

                Ok(vec![create::InputObject {
                    bucket: dyn_clone::clone_box(self),
                    key: key.to_string(),
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
                let prefix = Self::url_path_to_s3_path(&prefix);
                debug!(bucket = %self.inner.name, prefix, "Archive input matches all S3 objects with a certain prefix");

                // The callers of this method should have already parsed the URL and determine this
                // is a prefix by the use of the trailing `/` character.
                assert!(prefix.ends_with('/'),
                    "BUG: It should not be possible to create a Prefix selector unless the prefix ends with `/`, but the caller passed prefix '{prefix}'");

                // Use the paginated API to automatically handle dealing with continuation tokens
                let pages = self
                    .inner
                    .client
                    .list_objects_v2()
                    .bucket(&self.inner.name)
                    .prefix(prefix)
                    .delimiter("/")
                    .into_paginator()
                    .send();

                // Translate this stream of pages of object listings into a stream of AWS SDK
                // 'Object' structs so we can process them one at a time
                let objects = pages.map(|result| {
                    let page = result.with_context(|_| crate::error::ListObjectsInPrefixSnafu {
                        bucket: self.inner.name.clone(),
                        prefix: prefix.to_string(),
                    })?;

                    // NOTE: the `contents()` accessor returns a slice, but the `contents` field is
                    // actually public (but hidden from docs).  I'm probably not supposed to use
                    // this, and maybe it'll break in a future release, but for now this is much
                    // preferable because it means I can yield the owned `Vec` and not a ref which
                    // would not be possible to process as part of an async stream.
                    //
                    // NOTE: if there are no objects in this page, like when listing a prefix that
                    // contains no objects (only child prefixes) then this will be None.  But this
                    // isn't the right place to report an error, since there might be other pages
                    // that do have contents.
                    let result: Result<Vec<aws_sdk_s3::model::Object>> =
                        Ok(page.contents.unwrap_or_default());

                    result
                });

                let input_objects = self.objects_to_input_objects(objects).await?;

                debug!(input_objects = input_objects.len(), bucket = %self.inner.name, prefix, "Matched all S3 objects with a certain prefix");

                // If no objects matched then that's an error.
                if input_objects.is_empty() {
                    Err(crate::error::PrefixNotFoundOrEmptySnafu {
                        bucket: self.inner.name.clone(),
                        prefix: prefix.to_string(),
                    }
                    .into_error(snafu::NoneError))
                } else {
                    Ok(input_objects)
                }
            }
            create::ObjectSelector::Bucket => {
                // This is a very simple case, it includes all objects in the bucket
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

                // Before parsing the glob pattern, it needs to be translated from URL path to an
                // object name, as all other object keys are
                let pattern = Self::url_path_to_s3_path(&pattern);

                debug!(
                    bucket = %self.inner.name,
                    glob = pattern,
                    "Archive input matches objects in a bucket which match a glob pattern"
                );

                // As an optimization, figure out what part of the pattern string is just a regular
                // string prefix, and when the pattern matching expressiosn start.  That will let
                // us query the S3 API for only the objects that have a prefix that will match the
                // pattern, saving iteration time on buckets with very large amounts of
                // non-matching objects
                let prefix = {
                    // We compute which part of the pattern is just a literal string with no match
                    // characters by converting the pattern into an escaped string with all match
                    // expression characters escaped.  That obviously will be different than the
                    // original pattern expression.  So the longest common prefix between the two
                    // is the part that has no match pattern characters
                    let escaped = glob::Pattern::escape(&pattern);

                    longest_common_prefix(&pattern, &escaped)
                        .map(|s| s.to_owned())
                        .unwrap_or_default()
                };

                let pattern = glob::Pattern::new(pattern).with_context(|_| {
                    crate::error::InvalidGlobPatternSnafu {
                        pattern: pattern.to_string(),
                    }
                })?;

                // To make sure the glob matching behaviors like it does in unix shells, require
                // that `/` path separator chars must be matched by literal `/` and will never be
                // matched by a `*` or `?`.  Without this, `prefix1/*` will match an object
                // `prefix1/prefix2/test` which is absolutely not how UNIX shell globbing works
                let match_options = glob::MatchOptions {
                    require_literal_separator: true,
                    ..Default::default()
                };

                // Use the paginated API to automatically handle dealing with continuation tokens
                let pages = self
                    .inner
                    .client
                    .list_objects_v2()
                    .bucket(&self.inner.name)
                    .prefix(prefix)
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

                            pattern.matches_with(key, match_options.clone())
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

    #[instrument(skip(self))]
    async fn read_object_part(
        &self,
        key: String,
        version_id: Option<String>,
        byte_range: Range<u64>,
    ) -> Result<bytes::Bytes> {
        debug!("Reading partial object");

        let key = Self::url_path_to_s3_path(&key);

        let response = self
            .inner
            .client
            .get_object()
            .bucket(&self.inner.name)
            .key(key)
            .range(format!("bytes={}-{}", byte_range.start, byte_range.end - 1))
            .set_version_id(version_id.clone())
            .send()
            .await
            .with_context(|_| crate::error::GetObjectSnafu {
                bucket: self.inner.name.clone(),
                key: key.to_string(),
                version_id: version_id.clone(),
            })?;

        let bytes =
            response
                .body
                .collect()
                .await
                .with_context(|_| crate::error::ReadByteStreamSnafu {
                    bucket: self.inner.name.clone(),
                    key: key.to_string(),
                })?;

        Ok(bytes.into_bytes())
    }

    #[instrument(skip(self))]
    async fn read_object(
        &self,
        key: String,
        version_id: Option<String>,
        byte_range: Range<u64>,
    ) -> Result<mpsc::Receiver<Result<bytes::Bytes>>> {
        debug!("Reading object");

        let key = Self::url_path_to_s3_path(&key).to_string();

        // Split up this range of bytes (which might cover the entire object), so that the
        // multipart config is honored and we can download the object's data in parallel
        let parts = self.split_range_into_multipart(byte_range);

        // Make a separate future to download each of these ranges
        let read_futs = {
            let key = key.clone();
            let me = self.clone();

            parts.map(move |range| {
                let key = key.clone();
                let version_id = version_id.clone();
                let me = me.clone();

                async move { me.read_object_part(key, version_id, range).await }
            })
        };

        // Make the iterator of futures into a stream which yields the result of each future in
        // the order they appear in the iterator, polling multiple futures in parallel each time the stream
        // is read
        let mut read_stream = futures::stream::iter(read_futs)
            .buffered(self.inner.objstore.inner.config.max_concurrent_requests);

        // Run a background async task that will continuously poll this stream (and thus run up to
        // `max_concurrent_requests` futures at a time), posting the results to a mpsc queue the
        // receiver of which will be returned to the caller
        let (sender, receiver) =
            mpsc::channel(self.inner.objstore.inner.config.max_concurrent_requests);

        tokio::spawn(async move {
            while let Some(result) = read_stream.next().await {
                if (sender.send(result).await).is_err() {
                    // The receiver is dropped, which means no one is listening anymore so stop
                    // working on the downloads
                    break;
                }
            }
            debug!(%key, "Read object async task exiting");
        });

        Ok(receiver)
    }

    #[instrument(skip(self))]
    async fn create_object_writer(
        &self,
        key: String,
        size_hint: Option<u64>,
    ) -> Result<(
        DuplexStream,
        mpsc::UnboundedReceiver<u64>,
        oneshot::Receiver<Result<u64>>,
    )> {
        // S3 requires that multi-part be initialized in advance, then each individual part can be
        // uploaded in whatever order is convenient
        //
        // One constraint is that the total number of parts must be no more than 10,000.  That's
        // why we need a size hint; if using the configured chunk size would produce close to or
        // more than 10K parts, then we need to use a larger chunk size.
        let key = Self::url_path_to_s3_path(&key).to_string();
        let config = &self.inner.objstore.inner.config;
        let multipart_chunk_size = config.multipart_chunk_size.get_bytes() as usize;

        let chunk_size = match size_hint {
            None => {
                // Hope that the final size of the object will be small enough that the configured
                // chunk size is larger than 1/10,000th of the size of the whole object, but assume
                // it will be large enough that we should use multipart
                Some(multipart_chunk_size)
            }
            Some(size_hint) => {
                if size_hint > 5 * (1024 * 1024 * 1024 * 1024u64) {
                    // This is larger than the maximum allowed object size on S3
                    return crate::error::ObjectTooLargeSnafu {
                        bucket: self.inner.name.clone(),
                        key: key.to_string(),
                        size: size_hint,
                    }
                    .fail();
                }

                if size_hint > config.multipart_threshold.get_bytes() as u64 {
                    // Object will be large enough to justify using multipart
                    // Assuming the size hint is the upper bound of what's possible, how many parts
                    // will the configured chunk size produce?
                    if (size_hint + multipart_chunk_size as u64 - 1) / multipart_chunk_size as u64
                        <= 10_000
                    {
                        // Object is small enough the requested chunk size can be used
                        Some(multipart_chunk_size)
                    } else {
                        // Wow this is a very large object.  We're going to have to override the
                        // chunk size to keep the object count under 10K
                        let new_chunk_size = (size_hint + 9_999) / 10_000;

                        warn!(%key, size_hint, multipart_chunk_size, new_chunk_size,
                            "New object size is so large that the requested chunk size will be overridden to keep the total chunk count under 10K");

                        Some(new_chunk_size as usize)
                    }
                } else {
                    // This object's expected size is so small there's no reason to do multipart at
                    // all
                    None
                }
            }
        };

        // Create the writer and execute the worker task to process the data written to the stream.
        //
        // There are two variations, one multi-part the other uni-part
        let (progress_sender, progress_receiver) = mpsc::unbounded_channel();
        let (result_sender, result_receiver) = oneshot::channel();

        match chunk_size {
            Some(chunk_size) => {
                let response = self
                    .inner
                    .client
                    .create_multipart_upload()
                    .bucket(&self.inner.name)
                    .key(key.clone())
                    .send()
                    .await
                    .with_context(|_| crate::error::CreateMultipartUploadSnafu {
                        bucket: self.inner.name.clone(),
                        key: key.to_string(),
                    })?;
                let upload_id = response
                    .upload_id()
                    .expect("BUG: multi-part uploads always have upload ID")
                    .to_string();

                let (bytes_writer, chunks_receiver) =
                    crate::writers::multipart(chunk_size, config.max_concurrent_requests);

                let me = self.clone();
                let key = key.to_string();

                // Start a background task that will receive multi-part chunks on `chunks_receiver`
                // and write them in parallel to S3
                tokio::spawn(async move {
                    let result = me
                        .multipart_object_writer(
                            key.clone(),
                            upload_id.clone(),
                            chunks_receiver,
                            progress_sender,
                        )
                        .await;

                    if let Err(e) = &result {
                        // Before reporting this error, clean up the remains of the multi-part
                        // upload
                        error!(?e, bucket = %me.inner.name, %key, %upload_id,
                            "Multi-part upload failed; aborting multi-part upload on server side");

                        if let Err(e) = me
                            .inner
                            .client
                            .abort_multipart_upload()
                            .bucket(me.inner.name.clone())
                            .key(key.clone())
                            .upload_id(upload_id.clone())
                            .send()
                            .await
                        {
                            error!(?e, bucket = %me.inner.name, %key, %upload_id,
                            "Error aborting multi-part upload.  This will remain on the server forever unless there's a lifecycle policy configured");
                        }
                    }

                    let _ = result_sender.send(result);
                });

                Ok((bytes_writer, progress_receiver, result_receiver))
            }
            None => {
                // No need for multi-part here
                let (bytes_writer, chunks_receiver) =
                    crate::writers::unipart(config.multipart_threshold.get_bytes() as usize);

                let me = self.clone();

                tokio::spawn(async move {
                    let _ = result_sender.send(
                        me.unipart_object_writer(key.to_string(), chunks_receiver, progress_sender)
                            .await,
                    );
                });

                Ok((bytes_writer, progress_receiver, result_receiver))
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
    } else if let Some(region) = config.aws_region.as_deref() {
        RegionProviderChain::first_try(Region::new(region.to_string()))
    } else {
        // No explicit region; use the environment
        RegionProviderChain::default_provider().or_else("us-east-1")
    };

    let mut aws_config_builder = aws_config::from_env().region(region_provider);

    if let (Some(aws_access_key_id), Some(aws_secret_access_key)) = (
        config.aws_access_key_id.as_deref(),
        config.aws_secret_access_key.as_deref(),
    ) {
        aws_config_builder = aws_config_builder.credentials_provider(Credentials::from_keys(
            aws_access_key_id,
            aws_secret_access_key,
            None,
        ));
    }

    let aws_config = aws_config_builder.load().await;

    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config)
        .app_name(aws_config::AppName::new(APP_NAME).expect("BUG: hard-coded app name is invalid"));
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

    aws_sdk_s3::Client::from_conf(s3_config_builder.build())
}

/// Find the longest common prefix shared by two string slices.
fn longest_common_prefix<'a, 'b>(a: &'a str, b: &'a str) -> Option<&'a str> {
    if a.is_empty() {
        return None;
    }

    for (a_idx, a_char) in a.chars().enumerate() {
        if let Some(b_char) = b.chars().nth(a_idx) {
            if b_char != a_char {
                return Some(&a[..a_idx]);
            }
        } else {
            return Some(&a[..a_idx]);
        }
    }

    // The entirety of `a` is a shared prefix
    Some(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use color_eyre::Result;
    use ssstar_testing::{minio, test_data};

    /// Set up the ssstar config to use the specified Minio server
    fn config_for_minio(server: &minio::MinioServer) -> crate::Config {
        let mut config = crate::Config::default();

        config.aws_region = Some("us-east-1".to_string());
        config.aws_access_key_id = Some("minioadmin".to_string());
        config.aws_secret_access_key = Some("minioadmin".to_string());
        config.s3_endpoint = Some(server.endpoint_url());

        config
    }

    /// Make an [`S3Bucket`] instance which talks to a bucket stored on a local Minio server
    async fn open_bucket(server: &minio::MinioServer, bucket: &str) -> Result<S3Bucket> {
        let s3 = S3::new(config_for_minio(server)).await;

        Ok(S3Bucket::new(&s3, bucket).await?)
    }

    #[test]
    fn app_name_is_valid() {
        // Make sure the compile time-generated app name is actually valid
        aws_config::AppName::new(APP_NAME)
            .unwrap_or_else(|_| panic!("App Name '{}' is invalid", APP_NAME));
    }

    /// Test various permutations of `list_matching_objects` against a Minio S3 bucket
    #[test]
    fn list_matching_objects_with_various_selectors() -> Result<()> {
        ssstar_testing::logging::test_with_logging(async move {
            let server = minio::MinioServer::get().await?;
            let bucket = server.create_bucket("list_matching_objects", true).await?;
            let test_data = test_data::make_test_data(
                &server.aws_client().await?,
                &bucket,
                vec![
                    test_data::TestObject::new("test", "1KiB"),
                    test_data::TestObject::new("prefix1/test", "1KiB"),
                    test_data::TestObject::new("prefix2/test", "1KiB"),
                    test_data::TestObject::new("prefix3/test", "1KiB"),
                    test_data::TestObject::new("prefix3/prefix4/test", "1KiB"),
                ],
            )
            .await?;
            let s3bucket = open_bucket(&server, &bucket).await?;

            // an object selector that explicitly matches any of these objects should, obviously,
            // produce that object and only that object
            for object in test_data.keys() {
                let objects = s3bucket
                    .list_matching_objects(create::ObjectSelector::Object {
                        key: object.to_string(),
                        version_id: None,
                    })
                    .await?;

                assert_eq!(1, objects.len(),
                "Object selector for object '{}' should produce exactly one match for that precise object",
                object);

                let input_object = objects.first().unwrap();
                assert_eq!(object, &input_object.key);
            }

            // An object selector that specifies a non-existent object should fail with a not found
            // error
            let result = s3bucket
                .list_matching_objects(create::ObjectSelector::Object {
                    key: "doesnt-exist/".to_string(),
                    version_id: None,
                })
                .await;

            assert_matches!(result, Err(crate::S3TarError::ObjectNotFound { .. }));

            // Listing a prefix as an object should fail with a specific NotFound error if there is
            // no object by that name
            for prefix in ["prefix1/", "prefix3/", "prefix3/prefix4/"] {
                let result = s3bucket
                    .list_matching_objects(create::ObjectSelector::Object {
                        key: prefix.to_string(),
                        version_id: None,
                    })
                    .await;

                assert_matches!(result, Err(crate::S3TarError::ObjectNotFound { .. }));
            }

            // Listing a prefix that doesn't exist should also fail with a NotFound error
            let result = s3bucket
                .list_matching_objects(create::ObjectSelector::Prefix {
                    prefix: "doesnt-exist/".to_string(),
                })
                .await;

            assert_matches!(result, Err(crate::S3TarError::PrefixNotFoundOrEmpty { .. }));

            // Listing the entire bucket should produce all objects regardless of prefix
            let objects = s3bucket
                .list_matching_objects(create::ObjectSelector::Bucket)
                .await?;

            assert_eq!(objects.len(), test_data.len());

            for object in objects {
                assert!(test_data.contains_key(&object.key));
            }

            // Listing a specific prefix should match objects in that prefix, but it should not
            // match objects in a child prefix.  so matching for `prefix3` should match
            // `prefix3/test`, but not `prefix3/prefix4/test`
            let objects = s3bucket
                .list_matching_objects(create::ObjectSelector::Prefix {
                    prefix: "prefix3/".to_string(),
                })
                .await?;

            assert_eq!(1, objects.len());
            assert_eq!("prefix3/test", &objects.first().unwrap().key);

            let objects = s3bucket
                .list_matching_objects(create::ObjectSelector::Prefix {
                    prefix: "prefix3/prefix4/".to_string(),
                })
                .await?;

            assert_eq!(1, objects.len());
            assert_eq!("prefix3/prefix4/test", &objects.first().unwrap().key);

            // Exercise glob patterns.

            // The complete wildcard pattern matches everything
            let objects = s3bucket
                .list_matching_objects(create::ObjectSelector::Glob {
                    pattern: "**".to_string(),
                })
                .await?;

            assert_eq!(test_data.len(), objects.len());
            for test_data_key in test_data.keys() {
                assert!(objects
                    .iter()
                    .any(|input_object| &input_object.key == test_data_key));
            }

            // The `**` matches any prefix, and any object
            let objects = s3bucket
                .list_matching_objects(create::ObjectSelector::Glob {
                    pattern: "prefix3/**".to_string(),
                })
                .await?;

            assert_eq!(2, objects.len());
            assert!(objects
                .iter()
                .any(|input_object| &input_object.key == "prefix3/prefix4/test"));
            assert!(objects
                .iter()
                .any(|input_object| &input_object.key == "prefix3/test"));

            // The `*` matches objects but not prefixes
            let objects = s3bucket
                .list_matching_objects(create::ObjectSelector::Glob {
                    pattern: "prefix3/*".to_string(),
                })
                .await?;

            assert_eq!(
                1,
                objects.len(),
                "unexpected matching keys: {}",
                objects
                    .iter()
                    .map(|io| io.key.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            assert!(objects
                .iter()
                .any(|input_object| &input_object.key == "prefix3/test"));

            Ok(())
        })
    }
}
