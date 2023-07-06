use snafu::prelude::*;
use std::{path::PathBuf, sync::Arc};
use url::Url;

pub type Result<T, E = S3TarError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum S3TarError {
    #[snafu(display("The URL '{url}' doesn't correspond to any supported object storage technology.  Supported URL schemes are: s3"))]
    UnsupportedObjectStorage { url: Url },

    #[snafu(display("The S3 URL '{url}' is missing the bucket name"))]
    MissingBucket { url: Url },

    #[snafu(display("No matching S3 objects were found"))]
    NoInputs,

    #[snafu(display("The input [{input}] did not match any objects; double-check the bucket name and the path expression"))]
    SelectorMatchesNoObjects { input: String },

    #[snafu(display(
        "The S3 bucket '{bucket}' either doesn't exist, or your IAM identity is not granted access"
    ))]
    BucketInvalidOrNotAccessible {
        bucket: String,
        source: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::head_bucket::HeadBucketError>,
    },

    #[snafu(display("Error checking if versioning is enabled on S3 bucket '{bucket}'"))]
    GetBucketVersioning {
        bucket: String,
        source: aws_sdk_s3::error::SdkError<
            aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningError,
        >,
    },

    #[snafu(display("Error getting metadata about object '{key}' on S3 bucket '{bucket}'"))]
    HeadObject {
        bucket: String,
        key: String,
        source: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::head_object::HeadObjectError>,
    },

    #[snafu(display("Object '{key}' in S3 bucket '{bucket}' doesn't exist"))]
    ObjectNotFound { bucket: String, key: String },

    #[snafu(display("No objects in the prefix '{prefix}' in S3 bucket '{bucket}' were found.  If you meant to specify a object and not a prefix, remove the `/` character from the end of the URL.  If you want to match all objects in this prefix recursively, use a glob expression like '{prefix}/**.*'"))]
    PrefixNotFoundOrEmpty { bucket: String, prefix: String },

    #[snafu(display("Error listing objects in S3 bucket '{bucket}' with prefix '{prefix}"))]
    ListObjectsInPrefix {
        bucket: String,
        prefix: String,
        source:
            aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>,
    },

    #[snafu(display("Error listing objects in S3 bucket '{bucket}'"))]
    ListObjectsInBucket {
        bucket: String,
        source:
            aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>,
    },

    #[snafu(display("Error getting object '{key}'{} in S3 bucket '{bucket}'",
            version_id.as_ref().map(|id| format!(" version '{id}'")).unwrap_or_else(|| "".to_string())))]
    GetObject {
        bucket: String,
        key: String,
        version_id: Option<String>,
        source: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    },

    #[snafu(display(
        "Error starting multi-part upload of object '{key}' in S3 bucket '{bucket}'"
    ))]
    CreateMultipartUpload {
        bucket: String,
        key: String,
        source: aws_sdk_s3::error::SdkError<
            aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError,
        >,
    },

    #[snafu(display(
        "Error uploading part number {part_number} of object '{key}' in S3 bucket '{bucket}'"
    ))]
    UploadPart {
        bucket: String,
        key: String,
        part_number: usize,
        source: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::upload_part::UploadPartError>,
    },

    #[snafu(display(
        "Error completing multi-part upload of object '{key}' in S3 bucket '{bucket}'"
    ))]
    CompleteMultipartUpload {
        bucket: String,
        key: String,
        source: aws_sdk_s3::error::SdkError<
            aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError,
        >,
    },

    #[snafu(display("Error uploading object '{key}' in S3 bucket '{bucket}'"))]
    PutObject {
        bucket: String,
        key: String,
        source: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
    },

    #[snafu(display("Caller abandoned upload of object '{key}' in S3 bucket '{bucket}' before any data was uploaded"))]
    UnipartUploadAbandoned { bucket: String, key: String },

    #[snafu(display("Error reading byte stream for object '{key}' in S3 bucket '{bucket}'"))]
    ReadByteStream {
        bucket: String,
        key: String,
        source: aws_smithy_http::byte_stream::error::Error,
    },

    #[snafu(display("Unable to create new object '{key}' in S3 bucket '{bucket}', because the expected size of {size} bytes is larger than the 5TB maximum object size"))]
    ObjectTooLarge {
        bucket: String,
        key: String,
        size: u64,
    },

    #[snafu(display("The glob pattern '{pattern}' is invalid"))]
    InvalidGlobPattern {
        pattern: String,
        source: glob::PatternError,
    },

    #[snafu(display("The filter '{filter}' is not valid.  Filters cannot be empty strings, and they cannot start with '/'"))]
    InvalidFilter { filter: String },

    #[snafu(display("The archive URL '{url}' is missing the key name"))]
    ArchiveUrlInvalid { url: Url },

    #[snafu(display("Error opening source archive file '{}", path.display()))]
    OpeningArchiveFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error writing target archive file '{}", path.display()))]
    WritingArchiveFile {
        path: PathBuf,
        source: std::io::Error,
    },

    /// Some error while extracting the tar archive
    #[snafu(display("Error reading from tar archive"))]
    TarRead { source: std::io::Error },

    #[snafu(display("BUG: it looks like the background task writing an extracted object to object storage has panicked.  Check log output for more details"))]
    AsyncObjectWriterPanic,

    #[snafu(display("BUG: it looks like the background task writing the tar archive to object storage has panicked.  Check log output for more details"))]
    AsyncTarWriterPanic,

    #[snafu(display("Receiver dropped; assuming tar extract is aborted"))]
    TarExtractAborted,

    #[snafu(display("Error appending entry to tar archive"))]
    TarAppendData { source: std::io::Error },

    #[snafu(display("Error writing data to the object store uploader task"))]
    UploadWriterError { source: std::io::Error },

    #[snafu(display("A spawned async task task panicked or was canceled"))]
    Spawn { source: tokio::task::JoinError },

    #[snafu(display("A blocking task panicked or was canceled"))]
    SpawnBlocking { source: tokio::task::JoinError },

    #[snafu(display("Flush of writer failed"))]
    Flush { source: std::io::Error },

    #[snafu(display("Invalid endpoint error"))]
    InvalidEndpoint {
        source: aws_smithy_http::endpoint::error::InvalidEndpointError,
    },

    #[snafu(display("DateTime convert  error"))]
    DateTimeConvert {
        source: aws_smithy_types_convert::date_time::Error,
    },
    // These are special error cases needed because of a technicality with how futures work.  We
    // need to be able to report an error that is actually represented as an `Arc<S3TarError>`.
    // Fortunately the `Error` trait is implemented for `Arc<T>` when `T` implements `Error`
    #[snafu(display("Error initiating a multi-part upload"))]
    TarFileStartMultipartFile { source: Arc<S3TarError> },

    #[snafu(display(
        "Error uploading byte range of an file from the tar archive to object storage"
    ))]
    TarFilePartUpload { source: Arc<S3TarError> },

    #[snafu(display("Error assume role for role-arn: {}", role_arn))]
    AssumeRole {
        role_arn: String,
        source:
            aws_smithy_http::result::SdkError<aws_sdk_sts::operation::assume_role::AssumeRoleError>,
    },

    #[snafu(display("Get parameter error of the parameter: {}", parameter_name))]
    SsmGetParameter {
        source: aws_smithy_http::result::SdkError<
            aws_sdk_ssm::operation::get_parameter::GetParameterError,
        >,
        parameter_name: String,
    },

    #[snafu(display("SSM parameter '{}' is empty", parameter_name))]
    SsmParameterEmpty { parameter_name: String },

    #[snafu(display("SSM parameter '{}' does not exist", parameter_name))]
    SsmParameterMissing { parameter_name: String },
}
