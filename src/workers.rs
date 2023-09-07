use crate::{Error, Result};
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
        let req = Request::new_with_init(&url, &request_init)
            .map_err(|e| Error::ConnectionFailed(e.to_string()))?;
        let mut response = Fetch::Request(req)
            .send()
            .await
            .map_err(|e| Error::ConnectionFailed(e.to_string()))?;
        if response.status_code() != 200 {
            return Err(Error::ConnectionFailed(format!(
                "Status {}",
                response.status_code()
            )));
        }

        let resp: String = response
            .text()
            .await
            .map_err(|e| Error::ConnectionFailed(e.to_string()))?;
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
