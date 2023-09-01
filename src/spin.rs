use crate::{Error, Result};

use crate::proto::pipeline;

#[derive(Clone, Debug)]
pub struct HttpClient;

impl HttpClient {
    pub fn new() -> Self {
        Self
    }

    pub async fn send(
        &self,
        url: String,
        auth: String,
        body: String,
    ) -> Result<pipeline::ServerMsg> {
        let req = http::Request::builder()
            .uri(&url)
            .header("Authorization", &auth)
            .method("POST")
            .body(Some(bytes::Bytes::copy_from_slice(body.as_bytes())))
            .map_err(|e| Error::ConnectionFailed(e.to_string()))?;

        let response = spin_sdk::outbound_http::send_request(req)
            .map_err(|e| Error::ConnectionFailed(e.to_string()));
        let resp: String = std::str::from_utf8(&response?.into_body().unwrap_or_default())
            .map_err(|e| Error::ConnectionFailed(e.to_string()))?
            .to_string();
        let response: pipeline::ServerMsg =
            serde_json::from_str(&resp).map_err(|e| Error::ConnectionFailed(e.to_string()))?;
        Ok(response)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}
