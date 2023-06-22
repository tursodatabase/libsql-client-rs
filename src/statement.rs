//! `Statement` represents an SQL statement,
//! which can be later sent to a database.

use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;

use crate::Value;

/// SQL statement, possibly with bound parameters
pub struct Statement {
    pub(crate) sql: String,
    pub(crate) args: Vec<Value>,
}

impl Statement {
    /// Creates a new simple statement without bound parameters
    ///
    /// # Examples
    ///
    /// ```
    /// let stmt = libsql_client::Statement::new("SELECT * FROM sqlite_master");
    /// ```
    pub fn new(q: impl Into<String>) -> Statement {
        Self {
            sql: q.into(),
            args: vec![],
        }
    }

    /// Creates a statement with bound parameters
    ///
    /// # Examples
    ///
    /// ```
    /// let stmt = libsql_client::Statement::with_args("UPDATE t SET x = ? WHERE key = ?", &[3, 8]);
    /// ```
    pub fn with_args(q: impl Into<String>, params: &[impl Into<Value> + Clone]) -> Statement {
        Self {
            sql: q.into(),
            args: params.iter().map(|p| p.clone().into()).collect(),
        }
    }
}

impl From<String> for Statement {
    fn from(q: String) -> Statement {
        Statement {
            sql: q,
            args: vec![],
        }
    }
}

impl From<&str> for Statement {
    fn from(val: &str) -> Self {
        val.to_string().into()
    }
}

impl From<&&str> for Statement {
    fn from(val: &&str) -> Self {
        val.to_string().into()
    }
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params: Vec<String> = self
            .args
            .iter()
            .map(|p| match p {
                Value::Blob { value } => serde_json::json!({
                    "base64": BASE64_STANDARD_NO_PAD.encode(value),
                })
                .to_string(),
                _ => serde_json::json!(p)["value"].to_string(),
            })
            .collect();
        write!(
            f,
            "{{\"sql\": {}, \"args\": [{}]}}",
            serde_json::json!(self.sql),
            params.join(",")
        )
    }
}
