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

pub use proto::StmtResult as ResultSet;

pub mod client;
pub use client::{new_client, new_client_from_config, Config, DatabaseClient};

pub mod transaction;
pub use transaction::Transaction;

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
///   # use crate::libsql_client::{DatabaseClient, Statement, args};
///   let db = libsql_client::new_client().await?;
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
