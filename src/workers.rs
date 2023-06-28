use anyhow::Result;
use worker::*;

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
        let mut headers = Headers::new();
        headers.append("Authorization", &auth).ok();

        let request_init = RequestInit {
            body: Some(wasm_bindgen::JsValue::from_str(&body)),
            headers,
            cf: CfProperties::new(),
            method: Method::Post,
            redirect: RequestRedirect::Follow,
        };
        let req =
            Request::new_with_init(&url, &request_init).map_err(|e| anyhow::anyhow!("{e}"))?;
        let mut response = Fetch::Request(req)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if response.status_code() != 200 {
            anyhow::bail!("Status {}", response.status_code());
        }

        let resp: String = response.text().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        let response: pipeline::ServerMsg = serde_json::from_str(&resp)?;
        Ok(response)
    }
}
