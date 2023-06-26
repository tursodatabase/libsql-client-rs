//! A library for communicating with a libSQL database over HTTP.
//!
//! libsql-client is a lightweight HTTP-based driver for sqld,
//! which is a server mode for libSQL, which is an open-contribution fork of SQLite.
//!
//! libsql-client compiles to wasm32-unknown-unknown target, which makes it a great
//! driver for environments that run on WebAssembly.
//!
//! It is expected to become a general-purpose driver for communicating with sqld/libSQL,
//! but the only backend implemented at the moment is for Cloudflare Workers environment.

pub mod statement;
pub use statement::Statement;

pub mod proto;
pub use proto::{BatchResult, Col, Value};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
    #[cfg(feature = "mapping_names_to_values_in_rows")]
    pub value_map: std::collections::HashMap<String, Value>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub rows_affected: u64,
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
