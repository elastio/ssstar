use super::{Bucket, ObjectStorage};
use crate::{Config, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_smithy_http::endpoint::Endpoint;
use snafu::prelude::*;
use tracing::debug;
use url::Url;

/// Implementation of [`ObjectStorage`] for S3 and S3-compatible APIs
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
}

#[async_trait::async_trait]
impl ObjectStorage for S3 {
    async fn extract_bucket_from_url(&self, url: &Url) -> Result<Bucket> {
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

        Ok(Bucket {
            name: bucket.to_string(),
        })
    }
}
