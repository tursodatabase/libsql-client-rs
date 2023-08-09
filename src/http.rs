use crate::client::Config;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{proto::pipeline, BatchResult, ResultSet, Statement};

/// Information about the current session: the server-generated cookie
/// and the URL that should be used for further communication.
#[derive(Clone, Debug, Default)]
struct Cookie {
    baton: Option<String>,
    base_url: Option<String>,
}

/// Generic HTTP client. Needs a helper function that actually sends
/// the request.
#[derive(Clone, Debug)]
pub struct Client {
    inner: InnerClient,
    cookies: Arc<RwLock<HashMap<u64, Cookie>>>,
    url_for_queries: String,
    auth: String,
}

#[derive(Clone, Debug)]
pub enum InnerClient {
    #[cfg(feature = "reqwest_backend")]
    Reqwest(crate::reqwest::HttpClient),
    #[cfg(feature = "workers_backend")]
    Workers(crate::workers::HttpClient),
    #[cfg(feature = "spin_backend")]
    Spin(crate::spin::HttpClient),
    Default,
}

impl InnerClient {
    pub async fn send(
        &self,
        url: String,
        auth: String,
        body: String,
    ) -> Result<pipeline::ServerMsg> {
        match self {
            #[cfg(feature = "reqwest_backend")]
            InnerClient::Reqwest(client) => client.send(url, auth, body).await,
            #[cfg(feature = "workers_backend")]
            InnerClient::Workers(client) => client.send(url, auth, body).await,
            #[cfg(feature = "spin_backend")]
            InnerClient::Spin(client) => client.send(url, auth, body).await,
            _ => panic!("Must enable atleast one feature"),
        }
    }
}

impl Client {
    /// Creates a database client with JWT authentication.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `token` - auth token
    pub fn new(inner: InnerClient, url: impl Into<String>, token: impl Into<String>) -> Self {
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
            inner,
            cookies: Arc::new(RwLock::new(HashMap::new())),
            url_for_queries,
            auth: format!("Bearer {token}"),
        }
    }

    /// Establishes  a database client from a `Config` object
    pub fn from_config(inner: InnerClient, config: Config) -> anyhow::Result<Self> {
        Ok(Self::new(
            inner,
            config.url,
            config.auth_token.unwrap_or_default(),
        ))
    }

    pub fn from_env(inner: InnerClient) -> anyhow::Result<Client> {
        let url = std::env::var("LIBSQL_CLIENT_URL").map_err(|_| {
            anyhow::anyhow!("LIBSQL_CLIENT_URL variable should point to your sqld database")
        })?;

        let token = std::env::var("LIBSQL_CLIENT_TOKEN").unwrap_or_default();
        Ok(Client::new(inner, url, token))
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
        let mut response: pipeline::ServerMsg = self
            .inner
            .send(self.url_for_queries.clone(), self.auth.clone(), body)
            .await?;

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

        let cookie = if tx_id > 0 {
            self.cookies
                .read()
                .unwrap()
                .get(&tx_id)
                .cloned()
                .unwrap_or_default()
        } else {
            Cookie::default()
        };
        let msg = pipeline::ClientMsg {
            baton: cookie.baton,
            requests: vec![pipeline::StreamRequest::Execute(
                pipeline::StreamExecuteReq { stmt },
            )],
        };
        let body = serde_json::to_string(&msg)?;
        let url = cookie
            .base_url
            .unwrap_or_else(|| self.url_for_queries.clone());
        let mut response: pipeline::ServerMsg =
            self.inner.send(url, self.auth.clone(), body).await?;

        if tx_id > 0 {
            let base_url = response.base_url;
            match response.baton {
                Some(baton) => {
                    self.cookies.write().unwrap().insert(
                        tx_id,
                        Cookie {
                            baton: Some(baton),
                            base_url,
                        },
                    );
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
        let cookie = self
            .cookies
            .read()
            .unwrap()
            .get(&tx_id)
            .cloned()
            .unwrap_or_default();
        let msg = pipeline::ClientMsg {
            baton: cookie.baton,
            requests: vec![pipeline::StreamRequest::Close],
        };
        let url = cookie
            .base_url
            .unwrap_or_else(|| self.url_for_queries.clone());
        let body = serde_json::to_string(&msg)?;
        self.inner.send(url, self.auth.clone(), body).await.ok();
        self.cookies.write().unwrap().remove(&tx_id);
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
