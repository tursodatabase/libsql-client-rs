use super::client::Config;
use anyhow::Result;
use async_trait::async_trait;

use super::{QueryResult, Statement};

/// Database client. This is the main structure used to
/// communicate with the database.
pub struct Client {
    client: hrana_client::Client,
    client_future: hrana_client::ConnFut,
    stream: hrana_client::Stream,
}

impl Client {
    /// Creates a database client with JWT authentication.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `token` - auth token
    pub async fn new(url: impl Into<String>, token: impl Into<String>) -> Result<Self> {
        let token = token.into();
        let url = url.into();
        let (client, client_future) =
            hrana_client::Client::connect(&url, if token.is_empty() { None } else { Some(token) })
                .await?;
        let stream = client.open_stream().await?;
        Ok(Self {
            client,
            client_future,
            stream,
        })
    }

    /// Creates a database client from a `Config` object.
    pub async fn from_config(config: Config) -> Result<Self> {
        Self::new(config.url, config.auth_token.unwrap_or_default()).await
    }

    pub async fn shutdown(self) -> Result<()> {
        self.client.shutdown().await?;
        self.client_future.await?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl super::DatabaseClient for Client {
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        use std::collections::HashMap;

        let mut batch = hrana_client::proto::Batch::new();

        for stmt in stmts.into_iter() {
            let stmt: Statement = stmt.into();
            let mut hrana_stmt = hrana_client::proto::Stmt::new(stmt.q, true);
            for param in stmt.params {
                hrana_stmt.bind(param);
            }
            batch.step(None, hrana_stmt);
        }
        let results = self.stream.execute_batch(batch).await?;

        Ok(results
            .step_results
            .iter()
            .map(|result| match result {
                Some(result) => {
                    let rows = result
                        .rows
                        .iter()
                        .map(|vec_of_values| {
                            let mut cells: HashMap<String, crate::Value> = HashMap::new();
                            for (i, value) in vec_of_values.iter().enumerate() {
                                cells.insert(result.cols[i].name.clone().unwrap(), value.clone());
                            }
                            crate::Row { cells }
                        })
                        .collect();
                    QueryResult::Success((
                        crate::ResultSet {
                            columns: result
                                .cols
                                .iter()
                                .map(|c| c.name.clone().unwrap())
                                .collect(),
                            rows,
                        },
                        crate::Meta::default(),
                    ))
                }
                None => QueryResult::Error(("No result".to_string(), crate::Meta::default())),
            })
            .collect())
    }
}
