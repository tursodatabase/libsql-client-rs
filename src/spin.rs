use crate::client::Config;
use crate::proto;
// use async_trait::async_trait;
use anyhow::{anyhow, Result};
use base64::Engine;

use crate::{BatchResult, ResultSet, Statement};

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

    /// Creates a database client from a `Config` object.
    pub fn from_config(config: Config) -> Self {
        Self::new(config.url, config.auth_token.unwrap_or_default())
    }

    /// Creates a database client, given a `Url`
    ///
    /// # Arguments
    /// * `url` - `Url` object of the database endpoint. This cannot be a relative URL;
    ///
    /// # Examples
    ///
    /// ```
    /// # use libsql_client::spin::Client;
    /// use url::Url;
    ///
    /// let url  = Url::parse("https://foo:bar@localhost:8080").unwrap();
    /// let db = Client::from_url(url).unwrap();
    /// ```
    pub fn from_url<T: TryInto<url::Url>>(url: T) -> Result<Client>
    where
        <T as TryInto<url::Url>>::Error: std::fmt::Display,
    {
        let url = url.try_into().map_err(|e| anyhow!(format!("{e}")))?;
        let mut params = url.query_pairs();
        // Try a token=XXX parameter first, continue if not found
        if let Some((_, token)) = params.find(|(param_key, _)| param_key == "token") {
            return Ok(Client::new(url.as_str(), token.into_owned()));
        }

        let username = url.username();
        let password = url.password().unwrap_or_default();
        let mut url = url.clone();
        url.set_username("")
            .map_err(|_| anyhow!("Could not extract username from URL. Invalid URL?"))?;
        url.set_password(None)
            .map_err(|_| anyhow!("Could not extract password from URL. Invalid URL?"))?;
        Ok(Client::from_credentials(url.as_str(), username, password))
    }

    pub fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<BatchResult> {
        // FIXME: serialize and deserialize with existing routines from sqld
        let mut body = "{\"statements\": [".to_string();
        let mut stmts_count = 0;
        for stmt in stmts {
            body += &format!("{},", stmt.into());
            stmts_count += 1;
        }
        if stmts_count > 0 {
            body.pop();
        }
        body += "]}";

        let req = http::Request::builder()
            .uri(&self.url_for_queries)
            .header("Authorization", &self.auth)
            .method("POST")
            .body(Some(bytes::Bytes::copy_from_slice(body.as_bytes())))?;

        // NOTICE: legacy base_url parameter is not used in Spin backend
        let _ = &self.base_url;

        let response = spin_sdk::outbound_http::send_request(req);
        let resp: String =
            std::str::from_utf8(&response?.into_body().unwrap_or_default())?.to_string();
        let response_json: serde_json::Value = serde_json::from_str(&resp)?;
        crate::client::http_json_to_batch_result(response_json, stmts_count)
    }
}

/* FIXME: the code below provides async API. Spin does not support async Rust yet,
**        so let's cook a sync API instead
#[async_trait(?Send)]
impl crate::DatabaseClient for Client {
    async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<BatchResult> {
        self.raw_batch(stmts).map_err(|e| anyhow!("{e}"))
    }

    async fn transaction<'a>(&'a self) -> Result<Transaction<'a, Self>> {
        anyhow::bail!("Interactive transactions are only supported by WebSocket (hrana) and local backends. Use batch() instead")
    }
}
*/

impl Client {
    pub fn execute(&self, stmt: impl Into<Statement>) -> Result<ResultSet> {
        let results = self.raw_batch(std::iter::once(stmt))?;
        match (results.step_results.first(), results.step_errors.first()) {
            (Some(Some(result)), Some(None)) => Ok(ResultSet::from(result.clone())),
            (Some(None), Some(Some(err))) => Err(anyhow!(err.message.clone())),
            _ => unreachable!(),
        }
    }

    /// Executes a batch of SQL statements, wrapped in "BEGIN", "END", transaction-style.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    pub fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<Vec<ResultSet>> {
        let batch_results = self.raw_batch(
            std::iter::once(Statement::new("BEGIN"))
                .chain(stmts.into_iter().map(|s| s.into()))
                .chain(std::iter::once(Statement::new("END"))),
        )?;
        let step_error: Option<proto::Error> = batch_results
            .step_errors
            .into_iter()
            .skip(1)
            .find(|e| e.is_some())
            .flatten();
        if let Some(error) = step_error {
            return Err(anyhow!(error.message));
        }
        let mut step_results: Vec<Result<ResultSet>> = batch_results
            .step_results
            .into_iter()
            .skip(1) // BEGIN is not counted in the result, it's implicitly ignored
            .map(|maybe_rs| {
                maybe_rs
                    .map(ResultSet::from)
                    .ok_or_else(|| anyhow!("Unexpected missing result set"))
            })
            .collect();
        step_results.pop(); // END is not counted in the result, it's implicitly ignored
                            // Collect all the results into a single Result
        step_results.into_iter().collect::<Result<Vec<ResultSet>>>()
    }
}
