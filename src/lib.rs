//! A library for communicating with a libSQL database over HTTP.
//!
//! libsql-client is a lightweight HTTP-based driver for sqld,
//! which is a server mode for libSQL, which is an open-contribution fork of SQLite.
//!
//! libsql-client compiles to wasm32-unknown-unknown target, which makes it a great
//! driver for environments that run on WebAssembly.
//!
pub mod statement;
pub use statement::Statement;

pub mod proto;
pub use libsql::{Error, Result};
pub use proto::{BatchResult, Col, Value};

#[cfg(feature = "mapping_names_to_values_in_rows")]
pub mod de;

#[cfg(feature = "workers_backend")]
pub use worker;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Represents a row returned from the database.
pub struct Row {
    pub values: Vec<Value>,
    #[cfg(feature = "mapping_names_to_values_in_rows")]
    pub value_map: std::collections::HashMap<String, Value>,
}

impl<'a> Row {
    /// Try to get a value by index from this row and convert it to the desired type
    ///
    /// Will return an error if the index is invalid or if the value cannot be converted to the
    /// desired type
    ///
    /// # Examples
    /// ```
    /// # async fn f() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::SyncClient::in_memory().unwrap();
    /// db.execute("create table example(num integer, str text)").unwrap();
    /// db.execute("insert into example (num, str) values (0, 'zero')").unwrap();
    /// let rs = db.execute("select * from example").unwrap();
    /// let row = &rs.rows[0]; // ResultSet returns array of Rows
    /// let num : usize = row.try_get(0).unwrap();
    /// let text : &str = row.try_get(1).unwrap();
    /// # }
    /// ```
    pub fn try_get<V: TryFrom<&'a Value, Error = String>>(&'a self, index: usize) -> Result<V> {
        let val = self
            .values
            .get(index)
            .ok_or_else(|| Error::Misuse(format!("out of bound index {}", index)))?;
        val.try_into().map_err(|x: String| Error::Misuse(x))
    }

    /// Try to get a value given a column name from this row and convert it to the desired type
    ///
    /// Will return an error if the column name is invalid or if the value cannot be converted to the
    /// desired type
    ///
    /// # Examples
    /// ```
    /// # async fn f() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::SyncClient::in_memory().unwrap();
    /// db.execute("create table example(num integer, str text)").unwrap();
    /// db.execute("insert into example (num, str) values (0, 'zero')").unwrap();
    /// let rs = db.execute("select * from example").unwrap();
    /// let row = &rs.rows[0]; // ResultSet returns array of Rows
    /// let num : usize = row.try_column("num").unwrap();
    /// let text : &str = row.try_column("str").unwrap();
    /// # }
    /// ```
    #[cfg(feature = "mapping_names_to_values_in_rows")]
    pub fn try_column<V: TryFrom<&'a Value, Error = String>>(&'a self, col: &str) -> Result<V> {
        let val = self
            .value_map
            .get(col)
            .ok_or_else(|| Error::Misuse(format!("column `{}` not present", col)))?;
        val.try_into().map_err(|x: String| Error::Misuse(x))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Represents the result of a database query
///
/// # Examples
/// ```
/// # async fn f() {
/// # use libsql_client::Config;
/// let db = libsql_client::SyncClient::in_memory().unwrap();
/// let rs = db.execute("create table example(num integer, str text)").unwrap();
/// assert_eq!(rs.columns.is_empty(), true);
/// assert_eq!(rs.rows.is_empty(), true);
/// assert_eq!(rs.rows_affected, 0);
/// assert_eq!(rs.last_insert_rowid, None);
/// db.execute("insert into example (num, str) values (0, 'zero')").unwrap();
/// let rs = db.execute("select * from example").unwrap();
/// assert_eq!(rs.columns, ["num", "str"]);
/// assert_eq!(rs.rows.len(), 1)
/// # }
/// ```
pub struct ResultSet {
    /// name of the columns present in this `ResultSet`.
    pub columns: Vec<String>,
    /// One entry per row returned from the database. See [Row] for details.
    pub rows: Vec<Row>,
    /// How many rows were changed by this statement
    pub rows_affected: u64,
    /// the rowid for last insertion. See <https://www.sqlite.org/c3ref/last_insert_rowid.html> for
    /// details
    pub last_insert_rowid: Option<i64>,
}

impl std::convert::From<proto::StmtResult> for ResultSet {
    fn from(value: proto::StmtResult) -> Self {
        let columns: Vec<String> = value
            .cols
            .into_iter()
            .map(|c| c.name.unwrap_or_default())
            .collect();
        let rows = value
            .rows
            .into_iter()
            .map(|values| {
                #[cfg(feature = "mapping_names_to_values_in_rows")]
                let value_map = columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (c.to_string(), values[i].clone()))
                    .collect();
                Row {
                    values,
                    #[cfg(feature = "mapping_names_to_values_in_rows")]
                    value_map,
                }
            })
            .collect();
        ResultSet {
            columns,
            rows,
            rows_affected: value.affected_row_count,
            last_insert_rowid: value.last_insert_rowid,
        }
    }
}

pub mod client;
pub use client::{Client, Config, SyncClient};

#[cfg(any(
    feature = "reqwest_backend",
    feature = "workers_backend",
    feature = "spin_backend",
))]
pub mod http;
pub mod transaction;
pub use transaction::{SyncTransaction, Transaction};

#[cfg(feature = "workers_backend")]
pub mod workers;

#[cfg(feature = "reqwest_backend")]
pub mod reqwest;

#[cfg(feature = "local_backend")]
pub mod local;

#[cfg(feature = "spin_backend")]
pub mod spin;

#[cfg(feature = "hrana_backend")]
pub mod hrana;
mod utils;

/// A macro for passing parameters to statements without having to manually
/// define their types.
///
/// # Example
///
/// ```rust,no_run
///   # async fn f() -> anyhow::Result<()> {
///   # use crate::libsql_client::{Statement, args};
///   let db = libsql_client::Client::from_env().await?;
///   db.execute(
///       Statement::with_args("INSERT INTO cart(product_id, product_name, quantity, price) VALUES (?, ?, ?, ?)",
///       args!(64, "socks", 2, 4.5)),
///   ).await?;
///   # Ok(())
///   # }
/// ```
#[macro_export]
macro_rules! args {
    () => { &[] };
    ($($param:expr),+ $(,)?) => {
        &[$($param.into()),+] as &[libsql_client::Value]
    };
}
