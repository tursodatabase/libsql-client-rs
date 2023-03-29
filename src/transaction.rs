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
