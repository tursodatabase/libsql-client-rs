//! `Transaction` is a structure representing an interactive transaction.

use crate::Result;
use crate::{Client, ResultSet, Statement, SyncClient};

pub struct Transaction<'a> {
    pub(crate) id: u64,
    pub(crate) client: &'a Client,
}

impl<'a> Transaction<'a> {
    pub async fn new(client: &'a Client, id: u64) -> Result<Transaction<'a>> {
        client
            .execute_in_transaction(id, Statement::from("BEGIN"))
            .await?;
        Ok(Self { id, client })
    }

    /// Executes a statement within the current transaction.
    /// # Example
    ///
    /// ```rust,no_run
    ///   # async fn f() -> libsql_client::Result<()> {
    ///   # use crate::libsql_client::{Statement, args};
    ///   let mut db = libsql_client::Client::from_env().await?;
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
        self.client
            .execute_in_transaction(self.id, stmt.into())
            .await
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

pub struct SyncTransaction<'a> {
    pub(crate) id: u64,
    pub(crate) client: &'a SyncClient,
}

impl<'a> SyncTransaction<'a> {
    pub fn new(client: &'a SyncClient, id: u64) -> Result<SyncTransaction<'a>> {
        client.execute_in_transaction(id, Statement::from("BEGIN"))?;
        Ok(Self { id, client })
    }

    /// Executes a statement within the current transaction.
    /// # Example
    ///
    /// ```rust,no_run
    ///   # fn f() -> libsql_client::Result<()> {
    ///   # use crate::libsql_client::{Statement, args};
    ///   let mut db = libsql_client::SyncClient::from_env()?;
    ///   let tx = db.transaction()?;
    ///   tx.execute(Statement::with_args("INSERT INTO users (name) VALUES (?)", args!["John"]))?;
    ///   let res = tx.execute(Statement::with_args("INSERT INTO users (name) VALUES (?)", args!["Jane"]));
    ///   if res.is_err() {
    ///     tx.rollback()?;
    ///   } else {
    ///     tx.commit()?;
    ///   }
    ///   # Ok(())
    ///   # }
    /// ```
    pub fn execute(&self, stmt: impl Into<Statement>) -> Result<ResultSet> {
        self.client.execute_in_transaction(self.id, stmt.into())
    }

    /// Commits the transaction to the database.
    pub fn commit(self) -> Result<()> {
        self.client.commit_transaction(self.id)
    }

    /// Rolls back the transaction, cancelling any of its side-effects.
    pub fn rollback(self) -> Result<()> {
        self.client.rollback_transaction(self.id)
    }
}
