//! This module provides credentials for role-based authorization
//!
//! The credentials are automatically refreshed after the specified duration that allows to use
//! the this credential provider for long running jobs

use crate::Result;
use aws_credential_types::{
    provider::{error::CredentialsError, ProvideCredentials},
    Credentials,
};
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};
use tokio::sync::Mutex;

/// The configuration which is used to re-fresh credentials for the specified role-arn
struct RefreshConfig {
    /// Region which is used on `AssumeRole` operation
    region: Option<String>,
    /// Role-arn of the role which we expect to use to get temporary credentials
    role_arn: String,
    /// Session name of the role's credentials
    role_session_name: String,
    /// Duration of validity of temporary credentials which will be refreshed after the expire
    role_session_duration: Option<i32>,
}

struct AutoRefreshingProviderInner {
    /// Current valid credentials
    credentials: Option<Credentials>,
    /// Configuration for `AssumeRole` and generating temporary credentials
    refresh_config: RefreshConfig,
}

impl AutoRefreshingProviderInner {
    async fn refresh_credentials(&mut self) -> Result<()> {
        let region_provider = util::load_region_provider(self.refresh_config.region.as_ref());
        let aws_cfg = aws_config::from_env().region(region_provider).load().await;
        let sts_client = aws_sdk_sts::Client::new(&aws_cfg);
        let assume_role_output = sts_client
            .assume_role()
            .role_arn(&self.refresh_config.role_arn)
            .role_session_name(&self.refresh_config.role_session_name)
            .set_duration_seconds(self.refresh_config.role_session_duration)
            .send()
            .await
            .map_err(|source| crate::error::S3TarError::AssumeRole {
                role_arn: self.refresh_config.role_arn.clone(),
                source,
            })?;
        let new_credentials = assume_role_output
            .credentials()
            .expect("BUG: No credentials after assume role");
        let (access, secret, session) = (
            new_credentials
                .access_key_id()
                .map(String::from)
                .expect("BUG: no access key id"),
            new_credentials
                .secret_access_key()
                .map(String::from)
                .expect("BUG: no secret access key"),
            new_credentials
                .session_token()
                .map(String::from)
                .expect("BUG: no session token"),
        );
        let new_creds = Credentials::from_keys(access, secret, Some(session));
        self.credentials = Some(new_creds);
        Ok(())
    }
}

/// Implements `ProvideCredentials` trait for a role.
///
/// This is needed to jobs that uses non-static credentials that may expire earlier than required,
/// this automatically refreshes credentials before the credentials' expiration.
///
/// To assume the role the default env credentials are used, make sure they have this permission,
/// otherwise this will fail
#[derive(Clone)]
pub(crate) struct RoleCredentialsProvider(Arc<Mutex<AutoRefreshingProviderInner>>);

impl RoleCredentialsProvider {
    /// `region` - the region which will be used for `SdkConfig`, if no provided the default region
    /// from env configuration is be used, if no env configuration then the `us-east-1` is used.
    ///
    /// `role_arn` - the role arn like `arn:aws:iam::012345678901:role/my_role` which will be assumed
    /// to get credentials
    ///
    /// `role_session_name` - the name of session which will be associated with the credentials
    ///
    /// `aws_role_session_duration_seconds` - validity duration of the credentials after that the new credentials will be generated
    pub async fn new(
        region: Option<String>,
        role_arn: impl Into<String>,
        role_session_name: impl Into<String>,
        aws_role_session_duration_seconds: Option<i32>,
    ) -> RoleCredentialsProvider {
        let refresh_config = RefreshConfig {
            region,
            role_arn: role_arn.into(),
            role_session_name: role_session_name.into(),
            role_session_duration: aws_role_session_duration_seconds,
        };

        RoleCredentialsProvider(Arc::new(Mutex::new(AutoRefreshingProviderInner {
            credentials: None,
            refresh_config,
        })))
    }
}

impl Debug for RoleCredentialsProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // No any meaningful info in refresh-provider
        f.debug_struct(stringify!(AutoRefreshingProvider)).finish()
    }
}

impl ProvideCredentials for RoleCredentialsProvider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        // Clones the inner context and returns the future that returns the valid credentials
        //
        // This is called only when the current credentials are expired
        let inner = self.0.clone();
        aws_credential_types::provider::future::ProvideCredentials::new(async move {
            let mut guard = inner.lock().await;
            if let Err(source) = guard.refresh_credentials().await {
                return Err(CredentialsError::provider_error(source));
            }

            Ok(guard
                .credentials
                .clone()
                .expect("BUG: no credentials after refresh"))
        })
    }
}

pub(crate) mod util {
    use aws_config::meta::region::RegionProviderChain;
    use aws_types::region::Region;

    /// creates the `RegionProviderChain`, at first try using passed `region` but if this is `None`
    /// then it looks for the region configuration from environment, if no environment configuration
    /// then use `us-east-1` region (which is default region on AWS)
    pub fn load_region_provider(region: Option<impl AsRef<str>>) -> RegionProviderChain {
        if let Some(region) = region {
            RegionProviderChain::first_try(Region::new(region.as_ref().to_string()))
        } else {
            // No explicit region; use the environment
            RegionProviderChain::default_provider().or_else("us-east-1")
        }
    }
}
