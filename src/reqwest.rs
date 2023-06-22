use crate::client::Config;
use anyhow::{Context, Result};
use base64::Engine;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{proto::pipeline, BatchResult, ResultSet, Statement};

/// Database client. This is the main structure used to
/// communicate with the database.
#[derive(Clone, Debug)]
pub struct Client {
    inner: reqwest::Client,
    batons: Arc<RwLock<HashMap<u64, String>>>,
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
            format!("https://{}", &url)
        } else {
            url
        };
        let url_for_queries = format!("{base_url}v2/pipeline");
        Self {
            inner: reqwest::Client::new(),
            batons: Arc::new(RwLock::new(HashMap::new())),
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
            format!("https://{}", &url)
        } else {
            url
        };
        let url_for_queries = format!("{base_url}v2/pipeline");
        Self {
            inner: reqwest::Client::new(),
            batons: Arc::new(RwLock::new(HashMap::new())),
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

impl Client {
    fn into_hrana(stmt: Statement) -> crate::proto::Stmt {
        let mut hrana_stmt = crate::proto::Stmt::new(stmt.sql, true);
        for param in stmt.args {
            hrana_stmt.bind(param);
        }
        hrana_stmt
    }

    pub async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<BatchResult> {
        let mut batch = crate::proto::Batch::new();
        for stmt in stmts.into_iter() {
            batch.step(None, Self::into_hrana(stmt.into()));
        }

        let msg = pipeline::ClientMsg {
            baton: None,
            requests: vec![
                pipeline::StreamRequest::Batch(pipeline::StreamBatchReq { batch }),
                pipeline::StreamRequest::Close,
            ],
        };
        let body = serde_json::to_string(&msg)?;

        let response = self
            .inner
            .post(&self.url_for_queries)
            .body(body.clone())
            .header("Authorization", &self.auth)
            .send()
            .await?;
        if response.status() != reqwest::StatusCode::OK {
            anyhow::bail!("{}", response.status());
        }
        let resp: String = response.text().await?;
        let mut response: pipeline::ServerMsg = serde_json::from_str(&resp)?;

        if response.results.is_empty() {
            anyhow::bail!(
                "Unexpected empty response from server: {:?}",
                response.results
            );
        }
        if response.results.len() > 2 {
            // One with actual results, one closing the stream
            anyhow::bail!(
                "Unexpected multiple responses from server: {:?}",
                response.results
            );
        }
        match response.results.swap_remove(0) {
            pipeline::Response::Ok(pipeline::StreamResponseOk {
                response: pipeline::StreamResponse::Batch(batch_result),
            }) => Ok(batch_result.result),
            pipeline::Response::Ok(_) => {
                anyhow::bail!("Unexpected response from server: {:?}", response.results)
            }
            pipeline::Response::Error(e) => {
                anyhow::bail!("Error from server: {:?}", e)
            }
        }
    }

    async fn execute_inner(
        &self,
        stmt: impl Into<Statement> + Send,
        tx_id: u64,
    ) -> Result<ResultSet> {
        let stmt = Self::into_hrana(stmt.into());

        let baton = if tx_id > 0 {
            self.batons.read().unwrap().get(&tx_id).cloned()
        } else {
            None
        };
        let msg = pipeline::ClientMsg {
            baton,
            requests: vec![pipeline::StreamRequest::Execute(
                pipeline::StreamExecuteReq { stmt },
            )],
        };
        let body = serde_json::to_string(&msg)?;

        let response = self
            .inner
            .post(&self.url_for_queries)
            .body(body)
            .header("Authorization", &self.auth)
            .send()
            .await?;
        if response.status() != reqwest::StatusCode::OK {
            anyhow::bail!("{}", response.status());
        }
        let resp: String = response.text().await?;
        let mut response: pipeline::ServerMsg = serde_json::from_str(&resp)?;

        if tx_id > 0 {
            match response.baton {
                Some(baton) => {
                    self.batons.write().unwrap().insert(tx_id, baton);
                }
                None => anyhow::bail!("Stream closed: server returned empty baton"),
            }
        }

        if response.results.is_empty() {
            anyhow::bail!(
                "Unexpected empty response from server: {:?}",
                response.results
            );
        }
        if response.results.len() > 1 {
            anyhow::bail!(
                "Unexpected multiple responses from server: {:?}",
                response.results
            );
        }
        match response.results.swap_remove(0) {
            pipeline::Response::Ok(pipeline::StreamResponseOk {
                response: pipeline::StreamResponse::Execute(execute_result),
            }) => Ok(ResultSet::from(execute_result.result)),
            pipeline::Response::Ok(_) => {
                anyhow::bail!("Unexpected response from server: {:?}", response.results)
            }
            pipeline::Response::Error(e) => {
                anyhow::bail!("Error from server: {:?}", e)
            }
        }
    }

    async fn close_stream_for(&self, tx_id: u64) -> Result<()> {
        let msg = pipeline::ClientMsg {
            baton: self.batons.read().unwrap().get(&tx_id).cloned(),
            requests: vec![pipeline::StreamRequest::Close],
        };
        self.inner
            .post(&self.url_for_queries)
            .body(serde_json::to_string(&msg)?)
            .header("Authorization", &self.auth)
            .send()
            .await
            .context("Failed to close stream")?;
        self.batons.write().unwrap().remove(&tx_id);
        Ok(())
    }

    /// # Arguments
    /// * `stmt` - the SQL statement
    pub async fn execute(&self, stmt: impl Into<Statement> + Send) -> Result<ResultSet> {
        self.execute_inner(stmt, 0).await
    }

    pub async fn execute_in_transaction(&self, tx_id: u64, stmt: Statement) -> Result<ResultSet> {
        self.execute_inner(stmt, tx_id).await
    }

    pub async fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        self.execute_inner("COMMIT", tx_id).await.map(|_| ())?;
        self.close_stream_for(tx_id).await.ok();
        Ok(())
    }

    pub async fn rollback_transaction(&self, tx_id: u64) -> Result<()> {
        self.execute_inner("ROLLBACK", tx_id).await.map(|_| ())?;
        self.close_stream_for(tx_id).await.ok();
        Ok(())
    }
}
