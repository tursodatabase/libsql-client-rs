use anyhow::Result;
use hyper::{Request, Body, StatusCode, Uri, };
use hyper::body::{to_bytes, HttpBody};
use hyper::client::{HttpConnector, connect::Connection};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Service;

use crate::proto::pipeline;

#[derive(Clone, Debug)]
pub struct HttpClient<C = HttpConnector> {
    inner: hyper::client::Client<HttpsConnector<C>>,
}

pub async fn to_text<T>(body: T) -> anyhow::Result<String>
where
    T: HttpBody,
    T::Error: std::error::Error + Sync + Send + 'static,
{
    let bytes = to_bytes(body).await?;
    Ok(String::from_utf8(bytes.to_vec())?)
}

impl HttpClient {
    pub fn new() -> Self {
        let connector = HttpConnector::new();
        Self::with_connector(connector)
    }
}

impl<C> HttpClient<C>
where
    C: Service<Uri> + Send + Clone + Sync + 'static,
    C::Response: Connection + AsyncRead + AsyncWrite + Send + Unpin + 'static,
    C::Future: Send + 'static,
    C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    /// Creates an HttpClient using the provided connector.
    pub fn with_connector(connector: C) -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http2()
            .wrap_connector(connector);

        let builder = hyper::client::Client::builder();
        let inner = builder.build(connector);

        Self { inner }
    }

    pub async fn send(
        &self,
        url: String,
        auth: String,
        body: String,
    ) -> Result<pipeline::ServerMsg> {
        let request = Request::post(url)
            .header("Authorization", auth)
            .body(Body::from(body))?;

        let response = self.inner.request(request).await?;
        if response.status() != StatusCode::OK {
            let status = response.status();
            let txt = to_text(response.into_body()).await?;
            anyhow::bail!("{status}: {txt}");
        }
        let resp = to_text(response.into_body()).await?;
        let response: pipeline::ServerMsg = serde_json::from_str(&resp)?;
        Ok(response)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}
