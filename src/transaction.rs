//! `Transaction` is a structure representing an interactive transaction.

use crate::{DatabaseClient, QueryResult, Statement};
use anyhow::{Context, Result};

pub struct Transaction<'a, Client: DatabaseClient + ?Sized> {
    client: &'a mut Client,
}

impl<'a, Client: DatabaseClient + ?Sized> Transaction<'a, Client> {
    /// Creates a new transaction.
    pub async fn new(client: &'a mut Client) -> Result<Transaction<'a, Client>> {
        client.raw_batch(vec![Statement::new("BEGIN")]).await?;
        Ok(Self { client })
    }

    /// Executes a statement within the current transaction.
    /// # Example
    ///
    /// ```rust,no_run
    ///   # async fn f() -> anyhow::Result<()> {
    ///   # use crate::libsql_client::{DatabaseClient, Statement, params};
    ///   let mut db = libsql_client::new_client().await?;
    ///   let mut tx = db.transaction().await?;
    ///   tx.execute(Statement::with_params("INSERT INTO users (name) VALUES (?)", params!["John"])).await?;
    ///   let res = tx.execute(Statement::with_params("INSERT INTO users (name) VALUES (?)", params!["Jane"])).await;
    ///   if res.is_err() {
    ///     tx.rollback().await?;
    ///   } else {
    ///     tx.commit().await?;
    ///   }
    ///   # Ok(())
    ///   # }
    /// ```
    pub async fn execute(&mut self, stmt: Statement) -> Result<QueryResult> {
        self.client
            .raw_batch(vec![stmt])
            .await?
            .pop()
            .context("Exactly one QueryResult was expected")
    }

    /// Commits the transaction to the database.
    pub async fn commit(self) -> Result<()> {
        self.client
            .raw_batch(vec![Statement::new("COMMIT")])
            .await?;
        Ok(())
    }

    /// Rolls back the transaction, cancelling any of its side-effects.
    pub async fn rollback(self) -> Result<()> {
        self.client
            .raw_batch(vec![Statement::new("ROLLBACK")])
            .await?;
        Ok(())
    }
}
