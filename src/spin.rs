use anyhow::Result;

use crate::proto::pipeline;

#[derive(Clone, Debug)]
pub struct HttpClient;

impl HttpClient {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) async fn send(
        &self,
        url: String,
        auth: String,
        body: String,
    ) -> Result<pipeline::ServerMsg> {
        let req = http::Request::builder()
            .uri(&url)
            .header("Authorization", &auth)
            .method("POST")
            .body(Some(bytes::Bytes::copy_from_slice(body.as_bytes())))?;

        let response = spin_sdk::outbound_http::send_request(req);
        let resp: String =
            std::str::from_utf8(&response?.into_body().unwrap_or_default())?.to_string();
        let response: pipeline::ServerMsg = serde_json::from_str(&resp)?;
        Ok(response)
    }
}
