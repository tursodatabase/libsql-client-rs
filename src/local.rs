use crate::{proto, proto::StmtResult, BatchResult, Col, ResultSet, Statement, Value};
use anyhow::Result;
use sqlite3_parser::ast::{Cmd, Stmt};
use sqlite3_parser::lexer::sql::Parser;
use std::sync::{Arc, Mutex};

use fallible_iterator::FallibleIterator;
use rusqlite::types::Value as RusqliteValue;

/// Database client. This is the main structure used to
/// communicate with the database.
#[derive(Debug)]
pub struct Client {
    inner: Arc<Mutex<rusqlite::Connection>>,
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
            inner: Arc::new(Mutex::new(
                rusqlite::Connection::open(path).map_err(|e| anyhow::anyhow!("{e}"))?,
            )),
        })
    }

    /// Establishes a new in-memory database and connects to it.
    pub fn in_memory() -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                rusqlite::Connection::open(":memory:").map_err(|e| anyhow::anyhow!("{e}"))?,
            )),
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
    /// # fn f() {
    /// let db = libsql_client::local::Client::new("/tmp/example321.db").unwrap();
    /// let result = db
    ///     .raw_batch(["CREATE TABLE t(id)", "INSERT INTO t VALUES (42)"]);
    /// # }
    /// ```
    pub fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<BatchResult> {
        let mut step_results = vec![];
        let mut step_errors = vec![];
        for stmt in stmts {
            let stmt = stmt.into();
            let sql_string = &stmt.sql;
            let params = rusqlite::params_from_iter(
                stmt.args
                    .into_iter()
                    .map(ValueWrapper)
                    .map(RusqliteValue::from),
            );
            let inner = self.inner.lock().unwrap();
            let mut stmt = inner.prepare(sql_string)?;
            let cols: Vec<Col> = stmt
                .columns()
                .into_iter()
                .map(|c| Col {
                    name: Some(c.name().to_string()),
                })
                .collect();
            let mut rows = Vec::new();
            let mut input_rows = match stmt.query(params) {
                Ok(rows) => rows,
                Err(e) => {
                    step_results.push(None);
                    step_errors.push(Some(proto::Error {
                        message: e.to_string(),
                    }));
                    break;
                }
            };
            while let Some(row) = input_rows.next()? {
                let cells = (0..cols.len())
                    .map(|i| ValueWrapper::from(row.get::<usize, RusqliteValue>(i).unwrap()).0)
                    .collect();
                rows.push(cells)
            }
            let parser = Parser::new(sql_string.as_bytes());
            let cmd = parser.last();

            let last_insert_rowid = match cmd {
                Ok(Some(Cmd::Stmt(Stmt::Insert { .. }))) => Some(inner.last_insert_rowid()),
                _ => None,
            };

            let affected_row_count = match cmd {
                Ok(Some(
                    Cmd::Stmt(Stmt::Insert { .. })
                    | Cmd::Stmt(Stmt::Update { .. })
                    | Cmd::Stmt(Stmt::Delete { .. }),
                )) => inner.changes(),
                _ => 0,
            };

            let stmt_result = StmtResult {
                cols,
                rows,
                affected_row_count,
                last_insert_rowid,
            };
            step_results.push(Some(stmt_result));
            step_errors.push(None);
        }
        Ok(BatchResult {
            step_results,
            step_errors,
        })
    }

    /// Executes a batch of SQL statements, wrapped in "BEGIN", "END", transaction-style.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    pub fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement> + Send> + Send,
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
            return Err(anyhow::anyhow!(error.message));
        }
        let mut step_results: Vec<Result<ResultSet>> = batch_results
            .step_results
            .into_iter()
            .skip(1) // BEGIN is not counted in the result, it's implicitly ignored
            .map(|maybe_rs| {
                maybe_rs
                    .map(ResultSet::from)
                    .ok_or_else(|| anyhow::anyhow!("Unexpected missing result set"))
            })
            .collect();
        step_results.pop(); // END is not counted in the result, it's implicitly ignored
                            // Collect all the results into a single Result
        step_results.into_iter().collect::<Result<Vec<ResultSet>>>()
    }

    /// # Arguments
    /// * `stmt` - the SQL statement
    pub fn execute(&self, stmt: impl Into<Statement> + Send) -> Result<ResultSet> {
        let results = self.raw_batch(std::iter::once(stmt))?;
        match (results.step_results.first(), results.step_errors.first()) {
            (Some(Some(result)), Some(None)) => Ok(ResultSet::from(result.clone())),
            (Some(None), Some(Some(err))) => Err(anyhow::anyhow!(err.message.clone())),
            _ => unreachable!(),
        }
    }

    pub fn execute_in_transaction(&self, _tx_id: u64, stmt: Statement) -> Result<ResultSet> {
        self.execute(stmt)
    }

    pub fn commit_transaction(&self, _tx_id: u64) -> Result<()> {
        self.execute("COMMIT").map(|_| ())
    }

    pub fn rollback_transaction(&self, _tx_id: u64) -> Result<()> {
        self.execute("ROLLBACK").map(|_| ())
    }
}
