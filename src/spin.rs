use crate::client::Config;
use crate::proto;
// use async_trait::async_trait;
use anyhow::{anyhow, Context, Result};
use base64::Engine;

use crate::{BatchResult, Col, ResultSet, Statement, Value};

use base64::prelude::BASE64_STANDARD_NO_PAD;

/// Database client. This is the main structure used to
/// communicate with the database.
#[derive(Clone, Debug)]
pub struct Client {
    base_url: String,
    url_for_queries: String,
    auth: String,
}

impl Client {
    /// Creates a database client with JWT authentication.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `token` - auth token
    pub fn new(url: impl Into<String>, token: impl Into<String>) -> Self {
        let token = token.into();
        let url = url.into();
        // Auto-update the URL to start with https:// if no protocol was specified
        let base_url = if !url.contains("://") {
            format!("https://{}", &url)
        } else {
            url
        };
        let url_for_queries = if cfg!(feature = "separate_url_for_queries") {
            format!("{base_url}/queries")
        } else {
            base_url.clone()
        };
        Self {
            base_url,
            url_for_queries,
            auth: format!("Bearer {token}"),
        }
    }

    /// Creates a database client with Basic HTTP authentication.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `username` - database username
    /// * `pass` - user's password
    pub fn from_credentials(
        url: impl Into<String>,
        username: impl Into<String>,
        pass: impl Into<String>,
    ) -> Self {
        let username = username.into();
        let pass = pass.into();
        let url = url.into();
        // Auto-update the URL to start with https:// if no protocol was specified
        let base_url = if !url.contains("://") {
            format!("https://{}", &url)
        } else {
            url
        };
        let url_for_queries = if cfg!(feature = "separate_url_for_queries") {
            format!("{base_url}/queries")
        } else {
            base_url.clone()
        };
        Self {
            base_url,
            url_for_queries,
            auth: format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{pass}"))
            ),
        }
    }

    /// Creates a database client from a `Config` object.
    pub fn from_config(config: Config) -> Self {
        Self::new(config.url, config.auth_token.unwrap_or_default())
    }

    /// Creates a database client, given a `Url`
    ///
    /// # Arguments
    /// * `url` - `Url` object of the database endpoint. This cannot be a relative URL;
    ///
    /// # Examples
    ///
    /// ```
    /// # use libsql_client::spin::Client;
    /// use url::Url;
    ///
    /// let url  = Url::parse("https://foo:bar@localhost:8080").unwrap();
    /// let db = Client::from_url(url).unwrap();
    /// ```
    pub fn from_url<T: TryInto<url::Url>>(url: T) -> Result<Client>
    where
        <T as TryInto<url::Url>>::Error: std::fmt::Display,
    {
        let url = url.try_into().map_err(|e| anyhow!(format!("{e}")))?;
        let mut params = url.query_pairs();
        // Try a token=XXX parameter first, continue if not found
        if let Some((_, token)) = params.find(|(param_key, _)| param_key == "token") {
            return Ok(Client::new(url.as_str(), token.into_owned()));
        }

        let username = url.username();
        let password = url.password().unwrap_or_default();
        let mut url = url.clone();
        url.set_username("")
            .map_err(|_| anyhow!("Could not extract username from URL. Invalid URL?"))?;
        url.set_password(None)
            .map_err(|_| anyhow!("Could not extract password from URL. Invalid URL?"))?;
        Ok(Client::from_credentials(url.as_str(), username, password))
    }

    pub fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<BatchResult> {
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
            .uri(&self.url_for_queries)
            .header("Authorization", &self.auth)
            .method("POST")
            .body(Some(bytes::Bytes::copy_from_slice(body.as_bytes())))?;

        // NOTICE: legacy base_url parameter is not used in Spin backend
        let _ = &self.base_url;

        let response = spin_sdk::outbound_http::send_request(req);
        let resp: String =
            std::str::from_utf8(&response?.into_body().unwrap_or_default())?.to_string();
        let response_json: serde_json::Value = serde_json::from_str(&resp)?;
        http_json_to_batch_result(response_json, stmts_count)
    }
}

impl Client {
    /// Executes a single SQL statement.
    pub fn execute(&self, stmt: impl Into<Statement>) -> Result<ResultSet> {
        let results = self.raw_batch(std::iter::once(stmt))?;
        match (results.step_results.first(), results.step_errors.first()) {
            (Some(Some(result)), Some(None)) => Ok(ResultSet::from(result.clone())),
            (Some(None), Some(Some(err))) => Err(anyhow!(err.message.clone())),
            _ => unreachable!(),
        }
    }

    /// Executes a batch of SQL statements, wrapped in "BEGIN", "END", transaction-style.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    pub fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<Vec<ResultSet>> {
        let batch_results = self.raw_batch(
            std::iter::once(Statement::new("BEGIN"))
                .chain(stmts.into_iter().map(|s| s.into()))
                .chain(std::iter::once(Statement::new("END"))),
        )?;
        let step_error: Option<proto::Error> = batch_results
            .step_errors
            .into_iter()
            .skip(1)
            .find(|e| e.is_some())
            .flatten();
        if let Some(error) = step_error {
            return Err(anyhow!(error.message));
        }
        let mut step_results: Vec<Result<ResultSet>> = batch_results
            .step_results
            .into_iter()
            .skip(1) // BEGIN is not counted in the result, it's implicitly ignored
            .map(|maybe_rs| {
                maybe_rs
                    .map(ResultSet::from)
                    .ok_or_else(|| anyhow!("Unexpected missing result set"))
            })
            .collect();
        step_results.pop(); // END is not counted in the result, it's implicitly ignored
                            // Collect all the results into a single Result
        step_results.into_iter().collect::<Result<Vec<ResultSet>>>()
    }
}

// FIXME: legacy, we should just migrate to HTTP v2 based on Hrana

fn parse_columns(columns: Vec<serde_json::Value>, result_idx: usize) -> Result<Vec<Col>> {
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

fn parse_value(
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
        serde_json::Value::Object(v) => {
            let base64_field = v.get("base64").with_context(|| format!("Result {result_idx} row {row_idx} cell {cell_idx} had unknown object, expected base64 field"))?;
            let base64_string = base64_field.as_str().with_context(|| format!("Result {result_idx} row {row_idx} cell {cell_idx} had empty base64 field: {base64_field}"))?;
            let decoded = BASE64_STANDARD_NO_PAD.decode(base64_string)?;
            Ok(Value::Blob{value: decoded})
        },
        _ => Err(anyhow!(
            "Result {result_idx} row {row_idx} cell {cell_idx} had unknown type",
        )),
    }
}

fn parse_rows(
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

fn parse_query_result(
    result: serde_json::Value,
    idx: usize,
) -> Result<(Option<proto::StmtResult>, Option<proto::Error>)> {
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
                            let result_set = proto::StmtResult {
                                cols,
                                rows,
                                affected_row_count: 0,
                                last_insert_rowid: None,
                            };
                            Ok((Some(result_set), None))
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

fn http_json_to_batch_result(
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

            let mut step_results: Vec<Option<proto::StmtResult>> = Vec::with_capacity(stmts_count);
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
