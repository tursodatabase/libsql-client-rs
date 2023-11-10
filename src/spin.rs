use anyhow::Result;

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
            .body(Some(bytes::Bytes::copy_from_slice(body.as_bytes())))?;

        let response: http::Response<String> = spin_sdk::http::send(req).await?;
        let response: pipeline::ServerMsg = serde_json::from_str(&response.into_body())?;
        Ok(response)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}
