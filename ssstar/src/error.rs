use snafu::prelude::*;
use url::Url;

pub type Result<T, E = S3TarError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum S3TarError {
    #[snafu(display("The URL '{url}' doesn't correspond to any supported object storage technology.  Supported URL schemes are: s3"))]
    UnsupportedObjectStorage { url: Url },

    #[snafu(display("The S3 URL '{url}' is missing the bucket name"))]
    MissingBucket { url: Url },

    #[snafu(display(
        "The S3 bucket '{bucket}' either doesn't exist, or your IAM identity is not granted access"
    ))]
    BucketInvalidOrNotAccessible {
        bucket: String,
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::HeadBucketError>,
    },

    #[snafu(display("Error checking if versioning is enabled on S3 bucket '{bucket}'"))]
    GetBucketVersioning {
        bucket: String,
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::GetBucketVersioningError>,
    },

    #[snafu(display("Error getting metadata about object '{key}' on S3 bucket '{bucket}"))]
    HeadObject {
        bucket: String,
        key: String,
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::HeadObjectError>,
    },

    #[snafu(display("Error listing objects in S3 bucket '{bucket}' with prefix '{prefix}"))]
    ListObjectsInPrefix {
        bucket: String,
        prefix: String,
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::ListObjectsV2Error>,
    },

    #[snafu(display("Error listing objects in S3 bucket '{bucket}'"))]
    ListObjectsInBucket {
        bucket: String,
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::ListObjectsV2Error>,
    },

    #[snafu(display("The glob pattern '{pattern}' is invalid"))]
    InvalidGlobPattern {
        pattern: String,
        source: glob::PatternError,
    },
}
