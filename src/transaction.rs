//! `Transaction` is a structure representing an interactive transaction.

use crate::{DatabaseClient, Statement, StmtResult};
use anyhow::Result;

pub struct Transaction<'a, Client: DatabaseClient + ?Sized> {
    client: &'a Client,
}

impl<'a, Client: DatabaseClient + ?Sized> Transaction<'a, Client> {
    /// Creates a new transaction.
    pub async fn new(client: &'a Client) -> Result<Transaction<'a, Client>> {
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
    pub async fn execute(&mut self, stmt: Statement) -> Result<StmtResult> {
        self.client.execute(stmt).await
    }

    /// Commits the transaction to the database.
    pub async fn commit(self) -> Result<()> {
        self.client.execute("COMMIT").await?;
        Ok(())
    }

    /// Rolls back the transaction, cancelling any of its side-effects.
    pub async fn rollback(self) -> Result<()> {
        self.client.execute("ROLLBACK").await?;
        Ok(())
    }
}
