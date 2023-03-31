//! `Client` is the main structure to interact with the database.

use async_trait::async_trait;

use anyhow::{anyhow, Result};

use crate::{proto, BatchResult, Col, Statement, ResultSet, Transaction, Value};

/// Trait describing capabilities of a database client:
/// - executing statements, batches, transactions
#[async_trait(?Send)]
pub trait DatabaseClient {
    /// Executes a single SQL statement
    ///
    /// # Arguments
    /// * `stmt` - the SQL statement
    async fn execute(&self, stmt: impl Into<Statement>) -> Result<ResultSet> {
        let results = self.raw_batch(std::iter::once(stmt)).await?;
        match (results.step_results.first(), results.step_errors.first()) {
            (Some(Some(result)), Some(None)) => Ok(result.clone()),
            (Some(None), Some(Some(err))) => Err(anyhow::anyhow!(err.message.clone())),
            _ => unreachable!(),
        }
    }

    /// Executes a batch of SQL statements.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<BatchResult>;

    /// Executes a batch of SQL statements, wrapped in "BEGIN", "END", transaction-style.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<BatchResult> {
        let batch_results = self
            .raw_batch(
                std::iter::once(Statement::new("BEGIN"))
                    .chain(stmts.into_iter().map(|s| s.into()))
                    .chain(std::iter::once(Statement::new("END"))),
            )
            .await?;
        let mut step_results: Vec<Option<ResultSet>> =
            batch_results.step_results.into_iter().skip(1).collect();
        step_results.pop();
        let mut step_errors: Vec<Option<proto::Error>> =
            batch_results.step_errors.into_iter().skip(1).collect();
        step_errors.pop();
        Ok(BatchResult {
            step_results,
            step_errors,
        })
    }

    /// Starts an interactive transaction and returns a `Transaction` object.
    /// The object can be later used to `execute()`, `commit()` or `rollback()`
    /// the interactive transaction.
    async fn transaction<'a>(&'a self) -> Result<Transaction<'a, Self>> {
        Transaction::new(self).await
    }
}

/// A generic client struct, wrapping possible backends.
/// It's a convenience struct which allows implementing connect()
/// with backends being passed as env parameters.
pub enum GenericClient {
    #[cfg(feature = "local_backend")]
    Local(crate::local::Client),
    #[cfg(feature = "reqwest_backend")]
    Reqwest(crate::reqwest::Client),
    #[cfg(feature = "hrana_backend")]
    Hrana(crate::hrana::Client),
    #[cfg(feature = "workers_backend")]
    Workers(crate::workers::Client),
    #[cfg(feature = "spin_backend")]
    Spin(crate::spin::Client),
}

#[async_trait(?Send)]
impl DatabaseClient for GenericClient {
    async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<BatchResult> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.raw_batch(stmts).await,
            #[cfg(feature = "reqwest_backend")]
            Self::Reqwest(r) => r.raw_batch(stmts).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.raw_batch(stmts).await,
            #[cfg(feature = "workers_backend")]
            Self::Workers(w) => w.raw_batch(stmts).await,
            #[cfg(feature = "spin_backend")]
            Self::Spin(s) => s.raw_batch(stmts).await,
        }
    }

    async fn transaction<'a>(&'a self) -> Result<Transaction<'a, Self>> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(_) => Transaction::new(self).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(_) => Transaction::new(self).await,
            #[cfg(feature = "reqwest_backend")]
            Self::Reqwest(_) => {
                anyhow::bail!("Interactive transactions are not supported with the reqwest backend. Use batch() instead.")
            }
            #[cfg(feature = "workers_backend")]
            Self::Workers(_) => {
                anyhow::bail!("Interactive ransactions are not supported with the workers backend. Use batch() instead.")
            }
            #[cfg(feature = "spin_backend")]
            Self::Spin(_) => {
                anyhow::bail!("Interactive ransactions are not supported with the spin backend. Use batch() instead.")
            }
        }
    }
}

/// Configuration for the database client
pub struct Config {
    pub url: url::Url,
    pub auth_token: Option<String>,
}

/// Establishes a database client based on `Config` struct
///
/// # Examples
///
/// ```
/// # async fn f() {
/// # use libsql_client::{DatabaseClient, Config};
/// let config = Config { url: url::Url::parse("file:////tmp/example.db").unwrap(), auth_token: None };
/// let db = libsql_client::new_client_from_config(config).await.unwrap();
/// # }
/// ```
pub async fn new_client_from_config(config: Config) -> anyhow::Result<GenericClient> {
    let scheme = config.url.scheme();
    Ok(match scheme {
        #[cfg(feature = "local_backend")]
        "file" => {
            GenericClient::Local(crate::local::Client::new(config.url.to_string())?)
        },
        #[cfg(feature = "hrana_backend")]
        "ws" | "wss" => {
            GenericClient::Hrana(crate::hrana::Client::from_config(config).await?)
        },
        #[cfg(feature = "hrana_backend")]
        "libsql" => {
            let mut config = config;
            config.url = if config.url.scheme() == "libsql" {
                // We cannot use url::Url::set_scheme() because it prevents changing the scheme to http...
                // Safe to unwrap, because we know that the scheme is libsql
                url::Url::parse(&config.url.as_str().replace("libsql://", "wss://")).unwrap()
            } else {
                config.url
            };
            GenericClient::Hrana(crate::hrana::Client::from_config(config).await?)
        }
        #[cfg(feature = "reqwest_backend")]
        "http" | "https" => {
            GenericClient::Reqwest(crate::reqwest::Client::from_config(config)?)
        },
        #[cfg(feature = "workers_backend")]
        "workers" => {
            GenericClient::Workers(crate::workers::Client::from_config(config))
        },
        #[cfg(feature = "spin_backend")]
        "spin" => {
            GenericClient::Spin(crate::spin::Client::from_config(config))
        },
        _ => anyhow::bail!("Unknown scheme: {scheme}. Make sure your backend exists and is enabled with its feature flag"),
    })
}

/// Establishes a database client based on environment variables
///
/// # Env
/// * `LIBSQL_CLIENT_URL` - URL of the database endpoint - e.g. a https:// endpoint for remote connections
///   (with specified credentials) or local file:/// path for a local database
/// * (optional) `LIBSQL_CLIENT_BACKEND` - one of the available backends,
///   e.g. `reqwest`, `local`, `workers`. The library will try to deduce the backend
///   from the URL if not set explicitly. For instance, it will assume that https:// is not a local file.
/// *
/// # Examples
///
/// ```
/// # async fn run() {
/// # use libsql_client::{DatabaseClient, Config};
/// # std::env::set_var("LIBSQL_CLIENT_URL", "file:////tmp/example.db");
/// let db = libsql_client::new_client().await.unwrap();
/// # }
/// ```
pub async fn new_client() -> anyhow::Result<GenericClient> {
    let url = std::env::var("LIBSQL_CLIENT_URL").map_err(|_| {
        anyhow::anyhow!("LIBSQL_CLIENT_URL variable should point to your libSQL/sqld database")
    })?;
    let url = match url::Url::parse(&url) {
        Ok(url) => url,
        #[cfg(feature = "local_backend")]
        Err(_) if cfg!(feature = "local") => {
            return Ok(GenericClient::Local(crate::local::Client::new(url)?))
        }
        Err(e) => return Err(e.into()),
    };
    let scheme = url.scheme();
    let backend = std::env::var("LIBSQL_CLIENT_BACKEND").unwrap_or_else(|_| {
        match scheme {
            "ws" | "wss" | "libsql" if cfg!(feature = "hrana_backend") => "hrana",
            "http" | "https" => {
                if cfg!(feature = "reqwest_backend") {
                    "reqwest"
                } else if cfg!(feature = "workers_backend") {
                    "workers"
                } else if cfg!(feature = "spin_backend") {
                    "spin"
                } else {
                    "local"
                }
            }
            _ => "local",
        }
        .to_string()
    });
    Ok(match backend.as_str() {
        #[cfg(feature = "local_backend")]
        "local" => {
            GenericClient::Local(crate::local::Client::new(url.as_str())?)
        },
        #[cfg(feature = "reqwest_backend")]
        "reqwest" => {
            GenericClient::Reqwest(crate::reqwest::Client::from_url(url.as_str())?)
        },
        #[cfg(feature = "hrana_backend")]
        "hrana" => {
            let url = if url.scheme() == "libsql" {
                // We cannot use url::Url::set_scheme() because it prevents changing the scheme to http...
                // Safe to unwrap, because we know that the scheme is libsql
                url::Url::parse(&url.as_str().replace("libsql://", "wss://")).unwrap()
            } else {
                url
            };
            GenericClient::Hrana(crate::hrana::Client::new(url.as_str(), "").await?)
        },
        #[cfg(feature = "workers_backend")]
        "workers" => {
            anyhow::bail!("Connecting from workers API may need access to worker::RouteContext. Please call libsql_client::workers::Client::connect_from_ctx() directly")
        },
        #[cfg(feature = "spin_backend")]
        "spin" => {
            anyhow::bail!("Connecting from spin API may need access to specific Spin SDK secrets. Please call libsql_client::spin::Client::connect_from_url() directly")
        },
        _ => anyhow::bail!("Unknown backend: {backend}. Make sure your backend exists and is enabled with its feature flag"),
    })
}

// FIXME: serialize and deserialize with existing routines from sqld
pub(crate) fn statements_to_string(
    stmts: impl IntoIterator<Item = impl Into<Statement>>,
) -> (String, usize) {
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
    (body, stmts_count)
}

pub(crate) fn parse_columns(
    columns: Vec<serde_json::Value>,
    result_idx: usize,
) -> Result<Vec<Col>> {
    let mut result = Vec::with_capacity(columns.len());
    for (idx, column) in columns.into_iter().enumerate() {
        match column {
            serde_json::Value::String(column) => result.push(Col { name: Some(column) }),
            _ => {
                return Err(anyhow!(format!(
                    "Result {result_idx} column name {idx} not a string",
                )))
            }
        }
    }
    Ok(result)
}

pub(crate) fn parse_value(
    cell: serde_json::Value,
    result_idx: usize,
    row_idx: usize,
    cell_idx: usize,
) -> Result<Value> {
    match cell {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Number(v) => match v.as_i64() {
            Some(v) => Ok(Value::Integer{value: v} ),
            None => match v.as_f64() {
                Some(v) => Ok(Value::Float{value: v}),
                None => Err(anyhow!(
                    "Result {result_idx} row {row_idx} cell {cell_idx} had unknown number value: {v}",
                )),
            },
        },
        serde_json::Value::String(v) => Ok(Value::Text{value: v}),
        _ => Err(anyhow!(
            "Result {result_idx} row {row_idx} cell {cell_idx} had unknown type",
        )),
    }
}

pub(crate) fn parse_rows(
    rows: Vec<serde_json::Value>,
    cols_len: usize,
    result_idx: usize,
) -> Result<Vec<Vec<Value>>> {
    let mut result = Vec::with_capacity(rows.len());
    for (idx, row) in rows.into_iter().enumerate() {
        match row {
            serde_json::Value::Array(row) => {
                if row.len() != cols_len {
                    return Err(anyhow!(
                        "Result {result_idx} row {idx} had wrong number of cells",
                    ));
                }
                let mut cells: Vec<Value> = Vec::with_capacity(cols_len);
                for (cell_idx, value) in row.into_iter().enumerate() {
                    cells.push(parse_value(value, result_idx, idx, cell_idx)?);
                }
                result.push(cells)
            }
            _ => return Err(anyhow!("Result {result_idx} row {idx} was not an array",)),
        }
    }
    Ok(result)
}

pub(crate) fn parse_query_result(
    result: serde_json::Value,
    idx: usize,
) -> Result<(Option<ResultSet>, Option<proto::Error>)> {
    match result {
        serde_json::Value::Object(obj) => {
            if let Some(err) = obj.get("error") {
                return match err {
                    serde_json::Value::Object(obj) => match obj.get("message") {
                        Some(serde_json::Value::String(msg)) => Ok((
                            None,
                            Some(proto::Error {
                                message: msg.clone(),
                            }),
                        )),
                        _ => Err(anyhow!("Result {idx} error message was not a string",)),
                    },
                    _ => Err(anyhow!("Result {idx} results was not an object",)),
                };
            }

            let results = obj.get("results");
            match results {
                Some(serde_json::Value::Object(obj)) => {
                    let columns = obj
                        .get("columns")
                        .ok_or_else(|| anyhow!(format!("Result {idx} had no columns")))?;
                    let rows = obj
                        .get("rows")
                        .ok_or_else(|| anyhow!(format!("Result {idx} had no rows")))?;
                    match (rows, columns) {
                        (serde_json::Value::Array(rows), serde_json::Value::Array(columns)) => {
                            let cols = parse_columns(columns.to_vec(), idx)?;
                            let rows = parse_rows(rows.to_vec(), columns.len(), idx)?;
                            // FIXME: affected_row_count and last_insert_rowid are not implemented yet
                            let stmt_result = ResultSet {
                                cols,
                                rows,
                                affected_row_count: 0,
                                last_insert_rowid: None,
                            };
                            Ok((Some(stmt_result), None))
                        }
                        _ => Err(anyhow!(
                            "Result {idx} had rows or columns that were not an array",
                        )),
                    }
                }
                Some(_) => Err(anyhow!("Result {idx} was not an object",)),
                None => Err(anyhow!("Result {idx} did not contain results or error",)),
            }
        }
        _ => Err(anyhow!("Result {idx} was not an object",)),
    }
}

pub(crate) fn http_json_to_batch_result(
    response_json: serde_json::Value,
    stmts_count: usize,
) -> anyhow::Result<BatchResult> {
    match response_json {
        serde_json::Value::Array(results) => {
            if results.len() != stmts_count {
                return Err(anyhow::anyhow!(
                    "Response array did not contain expected {stmts_count} results"
                ));
            }

            let mut step_results: Vec<Option<ResultSet>> = Vec::with_capacity(stmts_count);
            let mut step_errors: Vec<Option<proto::Error>> = Vec::with_capacity(stmts_count);
            for (idx, result) in results.into_iter().enumerate() {
                let (step_result, step_error) =
                    parse_query_result(result, idx).map_err(|e| anyhow::anyhow!("{e}"))?;
                step_results.push(step_result);
                step_errors.push(step_error);
            }

            Ok(BatchResult {
                step_results,
                step_errors,
            })
        }
        e => Err(anyhow::anyhow!("Error: {}", e)),
    }
}
