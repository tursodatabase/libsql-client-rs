use super::client::Config;
use async_trait::async_trait;
use base64::Engine;
use worker::*;

use super::{QueryResult, Statement};

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
        Self::new(config.url, config.token.unwrap_or_default())
    }

    /// Creates a database client, given a `Url`
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
    /// let url  = Url::parse("https://foo:bar@localhost:8080").unwrap();
    /// let db = Client::from_url(&url).unwrap();
    /// ```
    pub fn from_url(url: &url::Url) -> anyhow::Result<Client> {
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

    /// Establishes a database client from Cloudflare Workers context.
    /// Expects the context to contain the following secrets defined:
    /// * `LIBSQL_CLIENT_URL`
    /// * `LIBSQL_CLIENT_USER`
    /// * `LIBSQL_CLIENT_PASS`
    /// # Arguments
    /// * `ctx` - Cloudflare Workers route context
    pub fn from_ctx<D>(ctx: &worker::RouteContext<D>) -> anyhow::Result<Self> {
        if let Ok(token) = ctx.secret("LIBSQL_CLIENT_TOKEN") {
            return Ok(Self::new(
                ctx.secret("LIBSQL_CLIENT_URL")
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .to_string(),
                token.to_string(),
            ));
        }

        Ok(Self::from_credentials(
            ctx.secret("LIBSQL_CLIENT_URL")
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .to_string(),
            ctx.secret("LIBSQL_CLIENT_USER")
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .to_string(),
            ctx.secret("LIBSQL_CLIENT_PASS")
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .to_string(),
        ))
    }

    /// Executes a batch of SQL statements.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn f() {
    /// let db = libsql_client::Client::from_credentials("https://example.com", "admin", "s3cr3tp4ss");
    /// let result = db
    ///     .batch(["CREATE TABLE t(id)", "INSERT INTO t VALUES (42)"])
    ///     .await;
    /// # }
    /// ```
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<Vec<QueryResult>> {
        let mut headers = Headers::new();
        headers.append("Authorization", &self.auth).ok();
        let (body, stmts_count) = crate::client::statements_to_string(stmts);
        let request_init = RequestInit {
            body: Some(wasm_bindgen::JsValue::from_str(&body)),
            headers,
            cf: CfProperties::new(),
            method: Method::Post,
            redirect: RequestRedirect::Follow,
        };
        let req = Request::new_with_init(&self.url_for_queries, &request_init)?;
        let response = Fetch::Request(req).send().await;
        let mut response = match response {
            Ok(r) if r.status_code() == 200 => r,
            // Retry with the legacy route: "/"
            resp => {
                if cfg!(feature = "separate_url_for_queries") {
                    Fetch::Request(Request::new_with_init(&self.base_url, &request_init)?)
                        .send()
                        .await?
                } else {
                    return Err(worker::Error::from(format!("{}", resp?.status_code())));
                }
            }
        };
        if response.status_code() != 200 {
            return Err(worker::Error::from(format!("{}", response.status_code())));
        }
        let resp: String = response.text().await?;
        let response_json: serde_json::Value = serde_json::from_str(&resp)?;
        super::client::json_to_query_result(response_json, stmts_count)
            .map_err(|e| worker::Error::from(format!("Error: {} ({:?})", e, request_init.body)))
    }
}

#[async_trait(?Send)]
impl super::DatabaseClient for Client {
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        self.batch(stmts).await.map_err(|e| anyhow::anyhow!("{e}"))
    }
}
