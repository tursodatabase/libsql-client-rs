use crate::client::Config;
use crate::proto;
use anyhow::Context;
use async_trait::async_trait;
use worker::*;

use crate::{BatchResult, ResultSet, Statement};

/// Database client. This is the main structure used to
/// communicate with the database.
#[derive(Debug)]
pub struct Client {
    socket: WebSocket,
    next_reqid: std::sync::atomic::AtomicI32,
}

impl Client {
    /// Creates a database client with JWT authentication.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `token` - auth token
    pub async fn new(url: impl Into<String>, token: impl Into<String>) -> anyhow::Result<Self> {
        let token = token.into();
        let url = url.into();
        // Auto-update the URL to start with https://.
        // It will get updated to wss via Workers API automatically
        let url = if !url.contains("://") {
            "https://".to_owned() + &url
        } else if let Some(url) = url.strip_prefix("libsql://") {
            "https://".to_owned() + &url
        } else if let Some(url) = url.strip_prefix("wss://") {
            "https://".to_owned() + &url
        } else {
            url
        };
        let url = url::Url::parse(&url).context("Failed to parse URL")?;

        let mut req =
            Request::new(url.as_str(), Method::Get).map_err(|e| anyhow::anyhow!("{}", e))?;
        let headers = req.headers_mut().map_err(|e| anyhow::anyhow!("{}", e))?;
        headers
            .set("upgrade", "websocket")
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        headers
            .set("Authentication", &format!("Bearer {token}"))
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let res = Fetch::Request(req)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let socket = match res.websocket() {
            Some(ws) => ws,
            None => anyhow::bail!("server did not accept"),
        };

        let mut event_stream = socket.events().map_err(|e| anyhow::anyhow!("{}", e))?;

        socket.accept().map_err(|e| anyhow::anyhow!("{}", e))?;

        let jwt = if token.is_empty() { None } else { Some(token) };
        socket
            .send(&proto::ClientMsg::Hello { jwt })
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        Self::recv_response(&mut event_stream)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // NOTICE: only a single stream id is used for now
        socket
            .send(&proto::ClientMsg::Request {
                request_id: 0,
                request: proto::Request::OpenStream(proto::OpenStreamReq { stream_id: 0 }),
            })
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        Self::recv_response(&mut event_stream)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        console_log!("Response received");
        drop(event_stream);
        Ok(Self {
            socket,
            next_reqid: std::sync::atomic::AtomicI32::new(1),
        })
    }

    /// Creates a database client from a `Config` object.
    pub async fn from_config(config: Config) -> anyhow::Result<Self> {
        Self::new(config.url, config.auth_token.unwrap_or_default()).await
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
        // Try if a jwt=XXX parameter exists
        if let Some((_, token)) = params.find(|(param_key, _)| param_key == "jwt") {
            Client::new(url.as_str(), token).await
        } else {
            Client::new(url, "").await
        }
    }

    /// Establishes a database client from Cloudflare Workers context.
    /// Expects the context to contain the following secrets defined:
    /// * `LIBSQL_CLIENT_URL`
    /// * `LIBSQL_CLIENT_USER`
    /// * `LIBSQL_CLIENT_PASS`
    /// # Arguments
    /// * `ctx` - Cloudflare Workers route context
    pub async fn from_ctx<D>(ctx: &worker::RouteContext<D>) -> anyhow::Result<Self> {
        let token = ctx
            .secret("LIBSQL_CLIENT_TOKEN")
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Self::new(
            ctx.secret("LIBSQL_CLIENT_URL")
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .to_string(),
            token.to_string(),
        )
        .await
    }

    fn send_request(&self, request: proto::Request) -> Result<()> {
        // NOTICE: we effective allow concurrency of 1 here, until we implement
        // id allocation andfMe request tracking
        let request_id = self
            .next_reqid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let msg = proto::ClientMsg::Request {
            request_id,
            request,
        };

        self.socket.send(&msg)
    }

    async fn recv_response(event_stream: &mut EventStream<'_>) -> Result<proto::ServerMsg> {
        use futures_util::StreamExt;

        // NOTICE: we're effectively synchronously waiting for the response here
        if let Some(event) = event_stream.next().await {
            match event? {
                WebsocketEvent::Message(msg) => {
                    let stmt_result: proto::ServerMsg = msg.json::<proto::ServerMsg>()?;
                    Ok(stmt_result)
                }
                WebsocketEvent::Close(msg) => {
                    Err(Error::RustError(format!("connection closed: {msg:?}")))
                }
            }
        } else {
            Err(Error::RustError("no response".to_string()))
        }
    }

    async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<BatchResult> {
        let mut batch = proto::Batch::new();

        for stmt in stmts.into_iter() {
            let stmt: Statement = stmt.into();
            let mut hrana_stmt = proto::Stmt::new(stmt.q, true);
            for param in stmt.params {
                hrana_stmt.bind(param);
            }
            batch.step(None, hrana_stmt);
        }

        let mut event_stream = self.socket.events()?;

        // NOTICE: if we want to support concurrent requests, we need to
        // actually start managing stream ids
        self.send_request(proto::Request::Batch(proto::BatchReq {
            stream_id: 0,
            batch,
        }))?;

        match Self::recv_response(&mut event_stream).await? {
            proto::ServerMsg::ResponseOk {
                request_id: _,
                response: proto::Response::Batch(proto::BatchResp { result }),
            } => Ok(result),
            proto::ServerMsg::ResponseError {
                request_id: _,
                error,
            } => Err(Error::RustError(format!("{error}"))),
            _ => Err(Error::RustError("unexpected response".to_string())),
        }
    }

    async fn execute(&self, stmt: impl Into<Statement>) -> Result<ResultSet> {
        let stmt: Statement = stmt.into();
        let mut hrana_stmt = proto::Stmt::new(stmt.q, true);
        for param in stmt.params {
            hrana_stmt.bind(param);
        }

        let mut event_stream = self.socket.events()?;

        self.send_request(proto::Request::Execute(proto::ExecuteReq {
            stream_id: 0,
            stmt: hrana_stmt,
        }))?;
        match Self::recv_response(&mut event_stream).await? {
            proto::ServerMsg::ResponseOk {
                request_id: _,
                response: proto::Response::Execute(proto::ExecuteResp { result }),
            } => Ok(result),
            proto::ServerMsg::ResponseError {
                request_id: _,
                error,
            } => Err(Error::RustError(format!("{error}"))),
            _ => Err(Error::RustError("unexpected response".to_string())),
        }
    }
}

#[async_trait(?Send)]
impl crate::DatabaseClient for Client {
    async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<BatchResult> {
        self.raw_batch(stmts)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn execute(&self, stmt: impl Into<Statement>) -> anyhow::Result<ResultSet> {
        self.execute(stmt).await.map_err(|e| anyhow::anyhow!("{e}"))
    }
}
