use snafu::prelude::*;
use std::path::PathBuf;
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

    #[snafu(display("Error getting object '{key}'{} in S3 bucket '{bucket}'",
            version_id.as_ref().map(|id| format!(" version '{id}'")).unwrap_or_else(|| "".to_string())))]
    GetObject {
        bucket: String,
        key: String,
        version_id: Option<String>,
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::GetObjectError>,
    },

    #[snafu(display("Error reading byte stream for object '{key}' in S3 bucket '{bucket}'"))]
    ReadByteStream {
        bucket: String,
        key: String,
        source: aws_smithy_http::byte_stream::Error,
    },

    #[snafu(display("The glob pattern '{pattern}' is invalid"))]
    InvalidGlobPattern {
        pattern: String,
        source: glob::PatternError,
    },

    #[snafu(display("Error writing target archive file '{}", path.display()))]
    WritingArchiveFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error appending entry to tar archive"))]
    TarAppendData { source: std::io::Error },

    #[snafu(display("A blocking task panicked or was canceled"))]
    SpawnBlocking { source: tokio::task::JoinError },

    #[snafu(display("Flush of writer failed"))]
    Flush { source: std::io::Error },
}
