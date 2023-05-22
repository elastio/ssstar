//! Wrapper around the `minio` server binary to run ephemeral instances of S3-compatible object
//! storage for testing

use crate::Result;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::Credentials;
use color_eyre::eyre::eyre;
use duct::Handle;
use once_cell::sync::Lazy;
use rand::prelude::*;
use regex::Regex;
use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::{Arc, Weak},
    time::Duration,
};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tracing::debug;
use which::which;

pub struct MinioServer {
    #[allow(dead_code)] // Never used, but needs to stay in scope so the temp dir isn't deleted
    temp_dir: TempDir,
    handle: Handle,
    endpoint: SocketAddr,
}

impl MinioServer {
    /// Try to re-use an existing instance that other tests might also be using, but if there isn't
    /// one then start a new one.
    ///
    /// This is preferable to [`Self::start`] because if many tests can re-use the same server it
    /// amortizes the time spent starting up a server over many tests
    pub async fn get() -> Result<Arc<Self>> {
        // Rust Mutex types are const now so once-cell isn't needed, but this code needs a tokio
        // Mutex because the lock is held across await points.  Pity.
        static INSTANCE: once_cell::sync::Lazy<Mutex<Option<Weak<MinioServer>>>> =
            once_cell::sync::Lazy::new(|| Mutex::new(None));

        let mut instance = INSTANCE.lock().await;

        let server = match instance.as_ref() {
            Some(weak) => {
                // A weak ref is already in place.  Is the thing it references still alive?
                match weak.upgrade() {
                    Some(strong) => {
                        // Still alive so we can use this reference
                        strong
                    }
                    None => {
                        // Ref went to zero so server was already dropped.  Start another one
                        let strong = Arc::new(Self::start().await?);
                        *instance = Some(Arc::downgrade(&strong));

                        strong
                    }
                }
            }
            None => {
                // First time this is being called, make a new instance
                let strong = Arc::new(Self::start().await?);
                *instance = Some(Arc::downgrade(&strong));

                strong
            }
        };

        debug!(endpoint = %server.endpoint,
            "get() found minio server");

        // Make sure the server is still working
        server.wait_for_service_start().await?;

        Ok(server)
    }

    /// Start a new minio server on a random high port.
    ///
    /// This assumes that Minio is installed in your system somewhere.  First the env var
    /// `MINIO_PATH` is checked, and if that's not set then it's assumed that `minio` is in your
    /// path.  If that doesn't work then this will fail.
    pub async fn start() -> Result<Self> {
        let path = Self::find_minio()?;

        let endpoint = Self::random_endpoint()?;

        let temp_dir = Self::temp_data_dir()?;

        let server = duct::cmd!(
            path,
            "server",
            temp_dir.path(),
            "--address",
            endpoint.to_string(),
            "--quiet"
        );
        let handle = server
            //.stderr_to_stdout()
            //.stdout_capture()
            .start()?;

        let minio_server = Self {
            temp_dir,
            handle,
            endpoint,
        };

        debug!(endpoint = %minio_server.endpoint, "Waiting for minio service to start");

        minio_server.wait_for_service_start().await?;

        debug!(endpoint = %minio_server.endpoint, "Minio started");

        Ok(minio_server)
    }

    /// The S3 API endpoint URL where the server is listening
    pub fn endpoint_uri(&self) -> http::Uri {
        format!("http://{}/", self.endpoint).parse().unwrap()
    }

    /// The S3 API endpoint URL where the server is listening
    pub fn endpoint_url(&self) -> url::Url {
        self.endpoint_uri().to_string().parse().unwrap()
    }

    /// Get [`Client`] instance that is configured to use this Minio server instance
    pub async fn aws_client(&self) -> Result<aws_sdk_s3::Client> {
        let region_provider = RegionProviderChain::first_try("us-east-1");
        let aws_config = aws_config::from_env()
            .region(region_provider)
            .credentials_provider(Credentials::from_keys("minioadmin", "minioadmin", None))
            .load()
            .await;

        let s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config)
            .endpoint_url(self.endpoint_uri().to_string());

        Ok(aws_sdk_s3::Client::from_conf(s3_config_builder.build()))
    }

    /// Make a new bucket on this Minio instance for testing purposes.
    ///
    /// The actual bucket name will have a random suffix appended to it, because we run multiple
    /// tests against the same minio service and we don't want them to conflict with one another.
    pub async fn create_bucket(
        &self,
        bucket: impl AsRef<str>,
        enable_versioning: bool,
    ) -> Result<String> {
        // Bucket names can be a maximum of 63 characters, can consist of letters and numbers and .
        // and - characters, with two `.` characters in a row forbidden.
        //
        // So need to make this bucket name comply with the rules
        static REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r##"[^0-9a-zA-Z\.\-]+"##).unwrap());

        debug!(bucket = bucket.as_ref(), "Creating bucket");

        let bucket = REGEX.replace_all(bucket.as_ref(), "-");

        // Shorten the bucket name so we can append the unique ID and it will still be under 63
        // chars
        let bucket = &bucket[..bucket.len().min(63 - 9)];

        // Prepend a random number to ensure the bucket name is unique across multiple tests
        let bucket = format!("{:08x}-{bucket}", rand::thread_rng().next_u32());

        debug!(%bucket, "Transformed bucket name into valid and unique bucket ID");

        let client = self.aws_client().await?;

        client.create_bucket().bucket(bucket.clone()).send().await?;

        // The creation of a bucket seems like it's sometimes asynchronous, because even after
        // `create_bucket` returns operations on it can fail with errors like:
        //
        // The S3 bucket '7ae02532-filter-by-prefix-src' either doesn't exist, or your IAM identity is not granted access
        //
        // So make sure the bucket actually was created before proceeding
        let policy = again::RetryPolicy::exponential(Duration::from_millis(100))
            .with_max_retries(10)
            .with_max_delay(Duration::from_secs(1));

        let client = self.aws_client().await?;

        if let Err(e) = policy
            .retry(|| client.head_bucket().bucket(&bucket).send())
            .await
        {
            return Err(
                eyre!("The bucket {bucket} is not accessible even after it was explicitly created.  Last error was: \n{e}")
            );
        };

        if enable_versioning {
            client
                .put_bucket_versioning()
                .bucket(bucket.clone())
                .versioning_configuration(
                    aws_sdk_s3::types::VersioningConfiguration::builder()
                        .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                        .build(),
                )
                .send()
                .await?;
        }

        debug!(%bucket, "Bucket created");

        Ok(bucket)
    }

    /// Block until able to successfully connect to the minio server or a timeout ocurrs
    async fn wait_for_service_start(&self) -> Result<()> {
        // The server doesn't start immediately upon the process starting; obviously there's a
        // startup period.  On slow CI boxes this can be agonizingly long.
        let policy = again::RetryPolicy::exponential(Duration::from_millis(100))
            .with_max_retries(10)
            .with_max_delay(Duration::from_secs(1));

        let client = self.aws_client().await?;

        if let Err(e) = policy.retry(|| client.list_buckets().send()).await {
            Err(
                eyre!("The minio server didn't come online in the allowed time.  The last error reported by ListBuckets against the server was:\n{}",
                    e)
            )
        } else {
            Ok(())
        }
    }

    fn find_minio() -> Result<PathBuf> {
        std::env::var_os("MINIO_PATH").map(PathBuf::from)
            .or_else(|| which("minio").ok())
            .ok_or_else(|| eyre!("Unable to find `minio`, either set the MINIO_PATH env var or put place the Minio executable in your PATH"))
    }

    /// Find a socket address on localhost that is free for minio to listen on
    fn random_endpoint() -> Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        drop(listener);

        Ok(addr)
    }

    /// Get a temporary directory for Minio data.
    ///
    /// Minio sucks so much that they've recently made a change whereby it's completely impossible
    /// to use a data directory in a `tmpfs` filesystem, like for example `/tmp` on most Linux
    /// distros, because it doesn't support `O_DIRECT` and their bullshit erasure coding storage
    /// technology requires that.
    ///
    /// The only rock solid use case for a tool like minio is as a test S3 endpoint to avoid
    /// talking to real object storage, and they messed that up!
    ///
    /// Instead, we'll make a `ssstar-minio-temp` directory in your home directory.  Apologies in
    /// advance for the clutter!
    fn temp_data_dir() -> Result<TempDir> {
        let home = dirs::home_dir().ok_or_else(|| eyre!("Unable to determine home directory"))?;

        Ok(tempfile::tempdir_in(home)?)
    }
}

impl Drop for MinioServer {
    fn drop(&mut self) {
        debug!(pids = ?self.handle.pids(), "Killing minio process(es)");

        if let Err(e) = self.handle.kill() {
            eprintln!("Error killing minio process: {}", e);
        }
    }
}
