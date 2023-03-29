use crate::client::Config;
use async_trait::async_trait;
use base64::Engine;

use crate::{BatchResult, Statement, Transaction};

/// Database client. This is the main structure used to
/// communicate with the database.
#[derive(Clone, Debug)]
pub struct Client {
    base_url: String,
    url_for_queries: String,
    auth: String,
}

impl Client {
    /// Creates a database client with JWT authentication.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `token` - auth token
    pub fn new(url: impl Into<String>, token: impl Into<String>) -> Self {
        let token = token.into();
        let url = url.into();
        // Auto-update the URL to start with https:// if no protocol was specified
        let base_url = if !url.contains("://") {
            "https://".to_owned() + &url
        } else {
            url
        };
        let url_for_queries = if cfg!(feature = "separate_url_for_queries") {
            format!("{base_url}/queries")
        } else {
            base_url.clone()
        };
        Self {
            base_url,
            url_for_queries,
            auth: format!("Bearer {token}"),
        }
    }

    /// Creates a database client with Basic HTTP authentication.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `username` - database username
    /// * `pass` - user's password
    pub fn from_credentials(
        url: impl Into<String>,
        username: impl Into<String>,
        pass: impl Into<String>,
    ) -> Self {
        let username = username.into();
        let pass = pass.into();
        let url = url.into();
        // Auto-update the URL to start with https:// if no protocol was specified
        let base_url = if !url.contains("://") {
            "https://".to_owned() + &url
        } else {
            url
        };
        let url_for_queries = if cfg!(feature = "separate_url_for_queries") {
            format!("{base_url}/queries")
        } else {
            base_url.clone()
        };
        Self {
            base_url,
            url_for_queries,
            auth: format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{pass}"))
            ),
        }
    }

    /// Establishes  a database client from a `Config` object
    pub fn from_config(config: Config) -> anyhow::Result<Self> {
        Ok(Self::new(config.url, config.auth_token.unwrap_or_default()))
    }

    /// Establishes a database client, given a `Url`
    ///
    /// # Arguments
    /// * `url` - `Url` object of the database endpoint. This cannot be a relative URL;
    ///
    /// # Examples
    ///
    /// ```
    /// # use libsql_client::reqwest::Client;
    /// use url::Url;
    ///
    /// let db = Client::from_url("https://foo:bar@localhost:8000").unwrap();
    /// ```
    pub fn from_url<T: TryInto<url::Url>>(url: T) -> anyhow::Result<Client>
    where
        <T as TryInto<url::Url>>::Error: std::fmt::Display,
    {
        let url = url
            .try_into()
            .map_err(|e| anyhow::anyhow!(format!("{e}")))?;
        let mut params = url.query_pairs();
        // Try a token=XXX parameter first, continue if not found
        if let Some((_, token)) = params.find(|(param_key, _)| param_key == "token") {
            return Ok(Client::new(url.as_str(), token.into_owned()));
        }

        let username = url.username();
        let password = url.password().unwrap_or_default();
        let mut url = url.clone();
        url.set_username("")
            .map_err(|_| anyhow::anyhow!("Could not extract username from URL. Invalid URL?"))?;
        url.set_password(None)
            .map_err(|_| anyhow::anyhow!("Could not extract password from URL. Invalid URL?"))?;
        Ok(Client::from_credentials(url.as_str(), username, password))
    }

    pub fn from_env() -> anyhow::Result<Client> {
        let url = std::env::var("LIBSQL_CLIENT_URL").map_err(|_| {
            anyhow::anyhow!("LIBSQL_CLIENT_URL variable should point to your sqld database")
        })?;

        if let Ok(token) = std::env::var("LIBSQL_CLIENT_TOKEN") {
            return Ok(Client::new(url, token));
        }

        let user = match std::env::var("LIBSQL_CLIENT_USER") {
            Ok(user) => user,
            Err(_) => {
                return Client::from_url(url.as_str());
            }
        };
        let pass = std::env::var("LIBSQL_CLIENT_PASS").map_err(|_| {
            anyhow::anyhow!("LIBSQL_CLIENT_PASS variable should be set to your sqld password")
        })?;
        Ok(Client::from_credentials(url, user, pass))
    }
}

#[async_trait(?Send)]
impl crate::DatabaseClient for Client {
    async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<BatchResult> {
        let (body, stmts_count) = crate::client::statements_to_string(stmts);
        let client = reqwest::Client::new();
        let response = match client
            .post(&self.url_for_queries)
            .body(body.clone())
            .header("Authorization", &self.auth)
            .send()
            .await
        {
            Ok(resp) if resp.status() == reqwest::StatusCode::OK => resp,
            // Retry with the legacy route: "/"
            resp => {
                if cfg!(feature = "separate_url_for_queries") {
                    client
                        .post(&self.base_url)
                        .body(body)
                        .header("Authorization", &self.auth)
                        .send()
                        .await?
                } else {
                    anyhow::bail!("{}", resp?.status());
                }
            }
        };
        if response.status() != reqwest::StatusCode::OK {
            anyhow::bail!("{}", response.status());
        }
        let resp: String = response.text().await?;
        let response_json: serde_json::Value = serde_json::from_str(&resp)?;
        crate::client::http_json_to_batch_result(response_json, stmts_count)
    }

    async fn transaction<'a>(&'a self) -> anyhow::Result<Transaction<'a, Self>> {
        anyhow::bail!("Interactive transactions are only supported by WebSocket (hrana) and local backends. Use batch() instead")
    }
}
