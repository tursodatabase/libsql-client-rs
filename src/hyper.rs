use anyhow::Result;
use hyper::{client::HttpConnector, Request, Body, StatusCode, body::{to_bytes, HttpBody}};

use crate::proto::pipeline;

pub(crate) type HttpsConnector<H = HttpConnector> = hyper_rustls::HttpsConnector<H>;

#[derive(Clone, Debug)]
pub struct HttpClient<C = HttpsConnector> {
    inner: hyper::client::Client<C>,
}

impl HttpClient {
    pub fn new() -> Self {
        let http_connector = HttpConnector::new();
        let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http2()
            .wrap_connector(http_connector);
        Self::with_connector(https_connector)
    }
}

pub async fn to_text<T>(body: T) -> anyhow::Result<String>
where
    T: HttpBody,
    T::Error: std::error::Error + Sync + Send + 'static,
{
    let bytes = to_bytes(body).await?;
    Ok(String::from_utf8(bytes.to_vec())?)
}

impl<C> HttpClient<C>
where
    C: hyper::client::connect::Connect + Clone + Send + Sync + 'static,
{
    pub fn with_connector(connector: C) -> Self {
        let inner = hyper::client::Client::builder().build(connector);

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
