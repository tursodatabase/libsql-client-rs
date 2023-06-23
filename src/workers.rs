use crate::client::Config;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use worker::*;

use crate::{proto::pipeline, BatchResult, ResultSet, Statement};

/// Database client. This is the main structure used to
/// communicate with the database.
#[derive(Clone, Debug)]
pub struct Client {
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
        // FIXME: build with Url builder to avoid typos, like double /
        let url_for_queries = format!("{base_url}/v2/pipeline");
        Self {
            batons: Arc::new(RwLock::new(HashMap::new())),
            url_for_queries,
            auth: format!("Bearer {token}"),
        }
    }

    /// Creates a database client from a `Config` object.
    pub async fn from_config(config: Config) -> Result<Self> {
        Ok(Self::new(config.url, config.auth_token.unwrap_or_default()))
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
    /// let db = Client::from_url(url).unwrap();
    /// ```
    pub async fn from_url<T: TryInto<url::Url>>(url: T) -> anyhow::Result<Client>
    where
        <T as TryInto<url::Url>>::Error: std::fmt::Display,
    {
        let url = url
            .try_into()
            .map_err(|e| anyhow::anyhow!(format!("{e}")))?;
        let mut params = url.query_pairs();
        // Try if a auth_token=XXX parameter exists
        Ok(
            if let Some((_, token)) = params.find(|(param_key, _)| param_key == "auth_token") {
                Client::new(url.as_str(), token)
            } else {
                Client::new(url, "")
            },
        )
    }

    /// Establishes a database client from Cloudflare Workers context.
    /// Expects the context to contain the following secrets defined:
    /// * `LIBSQL_CLIENT_URL`
    /// * `LIBSQL_CLIENT_USER`
    /// * `LIBSQL_CLIENT_PASS`
    /// # Arguments
    /// * `ctx` - Cloudflare Workers route context
    pub async fn from_ctx<D>(ctx: &worker::RouteContext<D>) -> Result<Self> {
        let token = ctx.secret("LIBSQL_CLIENT_TOKEN")?;
        Ok(Self::new(
            ctx.secret("LIBSQL_CLIENT_URL")?.to_string(),
            token.to_string(),
        ))
    }

    fn into_hrana(stmt: Statement) -> crate::proto::Stmt {
        let mut hrana_stmt = crate::proto::Stmt::new(stmt.sql, true);
        for param in stmt.args {
            hrana_stmt.bind(param);
        }
        hrana_stmt
    }

    async fn raw_batch_internal(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<BatchResult> {
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

        let mut headers = Headers::new();
        headers.append("Authorization", &self.auth).ok();

        let request_init = RequestInit {
            body: Some(wasm_bindgen::JsValue::from_str(&body)),
            headers,
            cf: CfProperties::new(),
            method: Method::Post,
            redirect: RequestRedirect::Follow,
        };
        let req = Request::new_with_init(&self.url_for_queries, &request_init)?;
        let mut response = Fetch::Request(req).send().await?;
        if response.status_code() != 200 {
            return Err(worker::Error::from(format!("{}", response.status_code())));
        }

        let resp: String = response.text().await?;
        let mut response: pipeline::ServerMsg = serde_json::from_str(&resp)?;

        if response.results.is_empty() {
            return Err(worker::Error::from("Empty response from server"));
        }
        if response.results.len() > 2 {
            // One with actual results, one closing the stream
            return Err(worker::Error::from(format!(
                "Unexpected response from server: {:?}",
                response.results
            )));
        }
        match response.results.swap_remove(0) {
            pipeline::Response::Ok(pipeline::StreamResponseOk {
                response: pipeline::StreamResponse::Batch(batch_result),
            }) => Ok(batch_result.result),
            pipeline::Response::Ok(_) => {
                Err(worker::Error::from("Unexpected response from server"))
            }
            pipeline::Response::Error(e) => {
                Err(worker::Error::from(format!("Error from server: {:?}", e)))
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

        let mut headers = Headers::new();
        headers.append("Authorization", &self.auth).ok();

        let request_init = RequestInit {
            body: Some(wasm_bindgen::JsValue::from_str(&body)),
            headers,
            cf: CfProperties::new(),
            method: Method::Post,
            redirect: RequestRedirect::Follow,
        };
        let req = Request::new_with_init(&self.url_for_queries, &request_init)?;
        let mut response = Fetch::Request(req).send().await?;
        if response.status_code() != 200 {
            return Err(worker::Error::from(format!("{}", response.status_code())));
        }

        let resp: String = response.text().await?;

        let mut response: pipeline::ServerMsg = serde_json::from_str(&resp)?;

        if tx_id > 0 {
            match response.baton {
                Some(baton) => {
                    self.batons.write().unwrap().insert(tx_id, baton);
                }
                None => return Err(worker::Error::from("No baton in response")),
            }
        }

        if response.results.is_empty() {
            return Err(worker::Error::from("Empty response from server"));
        }
        if response.results.len() > 1 {
            return Err(worker::Error::from(format!(
                "Unexpected response from server: {:?}",
                response.results
            )));
        }
        match response.results.swap_remove(0) {
            pipeline::Response::Ok(pipeline::StreamResponseOk {
                response: pipeline::StreamResponse::Execute(execute_result),
            }) => Ok(ResultSet::from(execute_result.result)),
            pipeline::Response::Ok(_) => {
                Err(worker::Error::from("Unexpected response from server"))
            }
            pipeline::Response::Error(e) => {
                Err(worker::Error::from(format!("Error from server: {:?}", e)))
            }
        }
    }

    async fn close_stream_for(&self, tx_id: u64) -> anyhow::Result<()> {
        let msg = pipeline::ClientMsg {
            baton: self.batons.read().unwrap().get(&tx_id).cloned(),
            requests: vec![pipeline::StreamRequest::Close],
        };

        let body = serde_json::to_string(&msg)?;

        let mut headers = Headers::new();
        headers.append("Authorization", &self.auth).ok();
        let request_init = RequestInit {
            body: Some(wasm_bindgen::JsValue::from_str(&body)),
            headers,
            cf: CfProperties::new(),
            method: Method::Post,
            redirect: RequestRedirect::Follow,
        };
        let req = Request::new_with_init(&self.url_for_queries, &request_init)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Fetch::Request(req).send().await.ok();

        self.batons.write().unwrap().remove(&tx_id);
        Ok(())
    }
}

impl Client {
    pub async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<BatchResult> {
        self.raw_batch_internal(stmts)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub async fn execute(&self, stmt: impl Into<Statement>) -> anyhow::Result<ResultSet> {
        self.execute_inner(stmt.into(), 0)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub async fn execute_in_transaction(
        &self,
        tx_id: u64,
        stmt: Statement,
    ) -> anyhow::Result<ResultSet> {
        self.execute_inner(stmt, tx_id)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub async fn commit_transaction(&self, tx_id: u64) -> anyhow::Result<()> {
        self.execute_inner("COMMIT", tx_id)
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        self.close_stream_for(tx_id).await.ok();
        Ok(())
    }

    pub async fn rollback_transaction(&self, tx_id: u64) -> anyhow::Result<()> {
        self.execute_inner("ROLLBACK", tx_id)
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        self.close_stream_for(tx_id).await.ok();
        Ok(())
    }
}
