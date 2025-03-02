//! This module provides functionality of custom credentials provider
//!
//! The credentials are automatically refreshed if the credentials are expired that allows to use
//! the this credential provider for long running jobs

use async_trait::async_trait;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::{Credentials, provider::ProvideCredentials};
use std::{fmt::Debug, sync::Arc};

/// This represents the credentials to access to AWS API.
#[derive(Clone, Debug)]
pub struct CredentialsUpdateOutput {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

/// This is a trait that represents a callback to update credentials when they become expired
#[async_trait]
pub trait CredentialsUpdateCallback: Debug + Send + Sync {
    /// This is not called for each AWS API call, this is called only when the
    /// current credentials are expired
    async fn update_credentials(
        &self,
    ) -> Result<CredentialsUpdateOutput, Box<dyn std::error::Error + Send + Sync + 'static>>;
}

/// Custom credentials provider that actually implements `ProvideCredentials` and implements the needed
/// traits to make it possible to put it into config as an option
#[derive(Debug, Clone)]
pub struct CredentialsProvider(Arc<dyn CredentialsUpdateCallback>);

/// This is needed to make it possible put it in clap configuration, the implementation doesn't matter
/// because this must be never called
impl PartialEq for CredentialsProvider {
    fn eq(&self, _other: &Self) -> bool {
        unreachable!("BUG: You should never compare `CustomCredentialsProvider`");
    }
}

impl CredentialsProvider {
    pub fn new(inner: impl CredentialsUpdateCallback + 'static) -> Self {
        Self(Arc::new(inner))
    }
}

impl ProvideCredentials for CredentialsProvider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async {
            self.0
                .update_credentials()
                .await
                .map(|output| {
                    Credentials::from_keys(
                        output.access_key,
                        output.secret_key,
                        output.session_token,
                    )
                })
                .map_err(CredentialsError::provider_error)
        })
    }
}
