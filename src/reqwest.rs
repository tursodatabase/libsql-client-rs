use anyhow::Result;

use crate::proto::pipeline;

#[derive(Clone, Debug)]
pub struct HttpClient {
    inner: reqwest::Client,
}

impl HttpClient {
    pub fn new() -> Self {
        Self {
            inner: reqwest::Client::new(),
        }
    }

    pub async fn send(
        &self,
        url: String,
        auth: String,
        body: String,
    ) -> Result<pipeline::ServerMsg> {
        let response = self
            .inner
            .post(url)
            .body(body)
            .header("Authorization", auth)
            .send()
            .await?;
        if response.status() != reqwest::StatusCode::OK {
            let status = response.status();
            let txt = response.text().await.unwrap_or_default();
            anyhow::bail!("{status}: {txt}");
        }
        let resp: String = response.text().await?;
        let response: pipeline::ServerMsg = serde_json::from_str(&resp)?;
        Ok(response)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}
