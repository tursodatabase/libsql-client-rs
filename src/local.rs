use crate::{proto, proto::StmtResult, BatchResult, Col, ResultSet, Statement, Value};
use crate::{Error, Result};
use sqlite3_parser::ast::{Cmd, Stmt};
use sqlite3_parser::lexer::sql::Parser;

use fallible_iterator::FallibleIterator;

/// Database client. This is the main structure used to
/// communicate with the database.
pub struct Client {
    db: libsql::Database,
    conn: libsql::Connection,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("local::Client").finish()
    }
}

struct ValueWrapper(Value);

impl From<ValueWrapper> for libsql::Value {
    fn from(v: ValueWrapper) -> Self {
        match v.0 {
            Value::Null => libsql::Value::Null,
            Value::Integer { value: n } => libsql::Value::Integer(n),
            Value::Text { value: s } => libsql::Value::Text(s),
            Value::Float { value: d } => libsql::Value::Real(d),
            Value::Blob { value: b } => libsql::Value::Blob(b),
        }
    }
}

impl From<libsql::Value> for ValueWrapper {
    fn from(v: libsql::Value) -> Self {
        match v {
            libsql::Value::Null => ValueWrapper(Value::Null),
            libsql::Value::Integer(n) => ValueWrapper(Value::Integer { value: n }),
            libsql::Value::Text(s) => ValueWrapper(Value::Text { value: s }),
            libsql::Value::Real(d) => ValueWrapper(Value::Float { value: d }),
            libsql::Value::Blob(b) => ValueWrapper(Value::Blob { value: b }),
        }
    }
}

impl Client {
    /// Establishes a database client.
    ///
    /// # Arguments
    /// * `path` - path of the local database
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let db = libsql::Database::open(path.into())?;
        let conn = db.connect()?;
        Ok(Self { db, conn })
    }

    /// Establishes a new in-memory database and connects to it.
    pub fn in_memory() -> Result<Self> {
        let db = libsql::Database::open(":memory:")?;
        let conn = db.connect()?;
        Ok(Self { db, conn })
    }

    pub fn from_env() -> Result<Self> {
        let path = std::env::var("LIBSQL_CLIENT_URL").map_err(|_| {
            Error::Misuse("LIBSQL_CLIENT_URL variable should point to your sqld database".into())
        })?;
        let path = match path.strip_prefix("file:///") {
            Some(path) => path,
            None => {
                return Err(Error::Misuse(
                    "Local URL needs to start with file:///".into(),
                ))
            }
        };
        Self::new(path)
    }

    pub async fn sync(&self) -> Result<usize> {
        self.db
            .sync()
            .await
            .map_err(|e| Error::Misuse(e.to_string()))
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
    ) -> Result<BatchResult> {
        let mut step_results = vec![];
        let mut step_errors = vec![];
        for stmt in stmts {
            let stmt = stmt.into();
            let sql_string = &stmt.sql;
            let params: libsql::Params = stmt
                .args
                .into_iter()
                .map(ValueWrapper)
                .map(libsql::Value::from)
                .collect::<Vec<_>>()
                .into();
            let stmt = self.conn.prepare(sql_string)?;
            let cols: Vec<Col> = stmt
                .columns()
                .into_iter()
                .map(|c| Col {
                    name: Some(c.name().to_string()),
                })
                .collect();
            let mut rows = Vec::new();
            let input_rows = match stmt.query(&params) {
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
                    .map(|i| ValueWrapper::from(row.get_value(i as i32).unwrap()).0)
                    .collect();
                rows.push(cells)
            }
            let parser = Parser::new(sql_string.as_bytes());
            let cmd = parser.last();

            let last_insert_rowid = match cmd {
                Ok(Some(Cmd::Stmt(Stmt::Insert { .. }))) => Some(self.conn.last_insert_rowid()),
                _ => None,
            };

            let affected_row_count = match cmd {
                Ok(Some(
                    Cmd::Stmt(Stmt::Insert { .. })
                    | Cmd::Stmt(Stmt::Update { .. })
                    | Cmd::Stmt(Stmt::Delete { .. }),
                )) => self.conn.changes(),
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
            return Err(Error::Misuse(error.message));
        }
        let mut step_results: Vec<Result<ResultSet>> = batch_results
            .step_results
            .into_iter()
            .skip(1) // BEGIN is not counted in the result, it's implicitly ignored
            .map(|maybe_rs| {
                maybe_rs
                    .map(ResultSet::from)
                    .ok_or_else(|| Error::Misuse("Unexpected missing result set".into()))
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
            (Some(None), Some(Some(err))) => Err(Error::Misuse(err.message.clone())),
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
