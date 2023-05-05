//! `Transaction` is a structure representing an interactive transaction.

use crate::{DatabaseClient, ResultSet, Statement};
use anyhow::Result;

pub struct Transaction<'a, Client: DatabaseClient + ?Sized> {
    pub(crate) id: u64,
    pub(crate) client: &'a Client,
}

impl<'a, Client: DatabaseClient + ?Sized> Transaction<'a, Client> {
    pub async fn new(client: &'a Client, id: u64) -> Result<Transaction<'a, Client>> {
        client.execute_in_transaction(id, Statement::from("BEGIN")).await?;
        Ok(Self { id, client })
    }
        
    /// Executes a statement within the current transaction.
    /// # Example
    ///
    /// ```rust,no_run
    ///   # async fn f() -> anyhow::Result<()> {
    ///   # use crate::libsql_client::{DatabaseClient, Statement, args};
    ///   let mut db = libsql_client::new_client().await?;
    ///   let tx = db.transaction().await?;
    ///   tx.execute(Statement::with_args("INSERT INTO users (name) VALUES (?)", args!["John"])).await?;
    ///   let res = tx.execute(Statement::with_args("INSERT INTO users (name) VALUES (?)", args!["Jane"])).await;
    ///   if res.is_err() {
    ///     tx.rollback().await?;
    ///   } else {
    ///     tx.commit().await?;
    ///   }
    ///   # Ok(())
    ///   # }
    /// ```
    pub async fn execute(&self, stmt: impl Into<Statement>) -> Result<ResultSet> {
        self.client.execute_in_transaction(self.id, stmt.into()).await
    }

    /// Commits the transaction to the database.
    pub async fn commit(self) -> Result<()> {
        self.client.commit_transaction(self.id).await
    }

    /// Rolls back the transaction, cancelling any of its side-effects.
    pub async fn rollback(self) -> Result<()> {
        self.client.rollback_transaction(self.id).await
    }
}
