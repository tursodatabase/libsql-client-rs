use async_trait::async_trait;
use base64::Engine;

use super::{parse_query_result, QueryResult, Statement};

/// Database connection. This is the main structure used to
/// communicate with the database.
#[derive(Clone, Debug)]
pub struct Connection {
    url: String,
    auth: String,
}

impl Connection {
    /// Establishes a database connection.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `username` - database username
    /// * `pass` - user's password
    pub fn connect(
        url: impl Into<String>,
        username: impl Into<String>,
        pass: impl Into<String>,
    ) -> Self {
        let username = username.into();
        let pass = pass.into();
        let url = url.into();
        // Auto-update the URL to start with https:// if no protocol was specified
        let url = if !url.contains("://") {
            "https://".to_owned() + &url
        } else {
            url
        };
        Self {
            url,
            auth: format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{pass}"))
            ),
        }
    }

    /// Establishes a database connection, given a [`Url`]
    ///
    /// # Arguments
    /// * `url` - [`Url`] object of the database endpoint. This cannot be a relative URL;
    pub fn connect_from_url(url: &url::Url) -> anyhow::Result<Connection> {
        let username = url.username();
        let password = url.password().unwrap_or_default();
        let mut url = url.clone();
        url.set_username("")
            .map_err(|_| anyhow::anyhow!("Could not extract username from URL. Invalid URL?"))?;
        url.set_password(None)
            .map_err(|_| anyhow::anyhow!("Could not extract password from URL. Invalid URL?"))?;
        Ok(Connection::connect(url.as_str(), username, password))
    }

    fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        // FIXME: serialize and deserialize with existing routines from sqld
        let mut body = "{\"statements\": [".to_string();
        let mut stmts_count = 0;
        for stmt in stmts {
            body += &format!("{},", stmt.into());
            stmts_count += 1;
        }
        if stmts_count > 0 {
            body.pop();
        }
        body += "]}";

        let req = http::Request::builder()
            .uri(&self.url)
            .header("Authorization", &self.auth)
            .method("POST")
            .body(Some(bytes::Bytes::copy_from_slice(body.as_bytes())))?;

        let response = spin_sdk::outbound_http::send_request(req);
        let resp: String =
            std::str::from_utf8(&response?.into_body().unwrap_or_default())?.to_string();
        let response_json: serde_json::Value = serde_json::from_str(&resp)?;
        match response_json {
            serde_json::Value::Array(results) => {
                if results.len() != stmts_count {
                    Err(anyhow::anyhow!(
                        "Response array did not contain expected {stmts_count} results"
                    ))
                } else {
                    let mut query_results: Vec<QueryResult> = Vec::with_capacity(stmts_count);
                    for (idx, result) in results.into_iter().enumerate() {
                        query_results.push(parse_query_result(result, idx)?);
                    }

                    Ok(query_results)
                }
            }
            e => Err(anyhow::anyhow!("Error: {} ({:?})", e, body)),
        }
    }
}

#[async_trait(?Send)]
impl super::Connection for Connection {
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        self.batch(stmts).map_err(|e| anyhow::anyhow!("{e}"))
    }
}
