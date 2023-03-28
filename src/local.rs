use super::{Meta, QueryResult, ResultSet, Row, Statement, Value};
use async_trait::async_trait;

use rusqlite::types::Value as RusqliteValue;

/// Database client. This is the main structure used to
/// communicate with the database.
#[derive(Debug)]
pub struct Client {
    inner: rusqlite::Connection,
}

struct ValueWrapper(Value);

impl From<ValueWrapper> for RusqliteValue {
    fn from(v: ValueWrapper) -> Self {
        match v.0 {
            Value::Null => RusqliteValue::Null,
            Value::Integer { value: n } => RusqliteValue::Integer(n),
            Value::Text { value: s } => RusqliteValue::Text(s),
            Value::Float { value: d } => RusqliteValue::Real(d),
            Value::Blob { value: b } => RusqliteValue::Blob(b),
        }
    }
}

impl From<RusqliteValue> for ValueWrapper {
    fn from(v: RusqliteValue) -> Self {
        match v {
            RusqliteValue::Null => ValueWrapper(Value::Null),
            RusqliteValue::Integer(n) => ValueWrapper(Value::Integer { value: n }),
            RusqliteValue::Text(s) => ValueWrapper(Value::Text { value: s }),
            RusqliteValue::Real(d) => ValueWrapper(Value::Float { value: d }),
            RusqliteValue::Blob(b) => ValueWrapper(Value::Blob { value: b }),
        }
    }
}

impl Client {
    /// Establishes a database client.
    ///
    /// # Arguments
    /// * `path` - path of the local database
    pub fn new(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: rusqlite::Connection::open(path).map_err(|e| anyhow::anyhow!("{e}"))?,
        })
    }

    /// Establishes a new in-memory database and connects to it.
    pub fn in_memory() -> anyhow::Result<Self> {
        Ok(Self {
            inner: rusqlite::Connection::open(":memory:").map_err(|e| anyhow::anyhow!("{e}"))?,
        })
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let path = std::env::var("LIBSQL_CLIENT_URL").map_err(|_| {
            anyhow::anyhow!("LIBSQL_CLIENT_URL variable should point to your sqld database")
        })?;
        let path = match path.strip_prefix("file:///") {
            Some(path) => path,
            None => anyhow::bail!("Local URL needs to start with file:///"),
        };
        Self::new(path)
    }

    /// Executes a batch of SQL statements.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn f() {
    /// let db = libsql_client::local::Client::new("/tmp/example321.db").unwrap();
    /// let result = db
    ///     .batch(["CREATE TABLE t(id)", "INSERT INTO t VALUES (42)"])
    ///     .await;
    /// # }
    /// ```
    pub async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        let mut result = vec![];
        for stmt in stmts {
            let stmt = stmt.into();
            let sql_string = &stmt.q;
            let params = rusqlite::params_from_iter(
                stmt.params
                    .into_iter()
                    .map(ValueWrapper)
                    .map(RusqliteValue::from),
            );
            let mut stmt = self.inner.prepare(sql_string)?;
            let columns: Vec<String> = stmt
                .columns()
                .into_iter()
                .map(|c| c.name().to_string())
                .collect();
            let mut rows = Vec::new();
            let mut input_rows = match stmt.query(params) {
                Ok(rows) => rows,
                Err(e) => {
                    result.push(QueryResult::Error((format!("{e}"), Meta { duration: 0 })));
                    break;
                }
            };
            while let Some(row) = input_rows.next()? {
                let cells = columns
                    .iter()
                    .map(|col| {
                        (
                            col.clone(),
                            ValueWrapper::from(
                                row.get::<&str, RusqliteValue>(col.as_str()).unwrap(),
                            )
                            .0,
                        )
                    })
                    .collect();
                rows.push(Row { cells })
            }
            let meta = Meta { duration: 0 };
            let result_set = ResultSet { columns, rows };
            result.push(QueryResult::Success((result_set, meta)))
        }
        Ok(result)
    }
}

#[async_trait(?Send)]
impl super::DatabaseClient for Client {
    async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        self.raw_batch(stmts).await
    }
}
