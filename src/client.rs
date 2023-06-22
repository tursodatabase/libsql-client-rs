//! `Client` is the main structure to interact with the database.
use anyhow::Result;

use crate::{proto, BatchResult, ResultSet, Statement, Transaction};

static TRANSACTION_IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A generic client struct, wrapping possible backends.
/// It's a convenience struct which allows implementing connect()
/// with backends being passed as env parameters.
#[derive(Debug)]
pub enum Client {
    #[cfg(feature = "local_backend")]
    Local(crate::local::Client),
    #[cfg(feature = "reqwest_backend")]
    Reqwest(crate::reqwest::Client),
    #[cfg(feature = "hrana_backend")]
    Hrana(crate::hrana::Client),
    #[cfg(feature = "workers_backend")]
    Workers(crate::workers::Client),
    #[cfg(feature = "spin_backend")]
    Spin(crate::spin::Client),
}

unsafe impl Send for Client {}

impl Client {
    pub async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement> + Send> + Send,
    ) -> Result<BatchResult> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.raw_batch(stmts),
            #[cfg(feature = "reqwest_backend")]
            Self::Reqwest(r) => r.raw_batch(stmts).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.raw_batch(stmts).await,
            #[cfg(feature = "workers_backend")]
            Self::Workers(w) => w.raw_batch(stmts).await,
            #[cfg(feature = "spin_backend")]
            Self::Spin(s) => s.raw_batch(stmts),
        }
    }

    /// Executes a batch of SQL statements, wrapped in "BEGIN", "END", transaction-style.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    pub async fn batch<I: IntoIterator<Item = impl Into<Statement> + Send> + Send>(
        &self,
        stmts: I,
    ) -> Result<Vec<ResultSet>>
    where
        <I as IntoIterator>::IntoIter: Send,
    {
        let batch_results = self
            .raw_batch(
                std::iter::once(Statement::new("BEGIN"))
                    .chain(stmts.into_iter().map(|s| s.into()))
                    .chain(std::iter::once(Statement::new("END"))),
            )
            .await?;
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

    pub async fn execute(&self, stmt: impl Into<Statement> + Send) -> Result<ResultSet> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.execute(stmt),
            #[cfg(feature = "reqwest_backend")]
            Self::Reqwest(r) => r.execute(stmt).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.execute(stmt).await,
            #[cfg(feature = "workers_backend")]
            Self::Workers(w) => w.execute(stmt).await,
            #[cfg(feature = "spin_backend")]
            Self::Spin(s) => s.execute(stmt),
        }
    }

    pub async fn transaction(&self) -> Result<Transaction> {
        let id = TRANSACTION_IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Transaction::new(self, id).await
    }

    pub async fn execute_in_transaction(&self, tx_id: u64, stmt: Statement) -> Result<ResultSet> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.execute_in_transaction(tx_id, stmt),
            #[cfg(feature = "reqwest_backend")]
            Self::Reqwest(r) => r.execute_in_transaction(tx_id, stmt).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.execute_in_transaction(tx_id, stmt).await,
            #[cfg(feature = "workers_backend")]
            Self::Workers(w) => w.execute_in_transaction(tx_id, stmt).await,
            #[cfg(feature = "spin_backend")]
            Self::Spin(_) => anyhow::bail!("Interactive ransactions are not supported yet with the spin backend. Use batch() instead."),
        }
    }

    pub async fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.commit_transaction(tx_id),
            #[cfg(feature = "reqwest_backend")]
            Self::Reqwest(r) => r.commit_transaction(tx_id).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.commit_transaction(tx_id).await,
            #[cfg(feature = "workers_backend")]
            Self::Workers(w) => w.commit_transaction(tx_id).await,
            #[cfg(feature = "spin_backend")]
            Self::Spin(_) => anyhow::bail!("Interactive ransactions are not supported yet with the spin backend. Use batch() instead."),
        }
    }

    pub async fn rollback_transaction(&self, tx_id: u64) -> Result<()> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.rollback_transaction(tx_id),
            #[cfg(feature = "reqwest_backend")]
            Self::Reqwest(r) => r.rollback_transaction(tx_id).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.rollback_transaction(tx_id).await,
            #[cfg(feature = "workers_backend")]
            Self::Workers(w) => w.rollback_transaction(tx_id).await,
            #[cfg(feature = "spin_backend")]
            Self::Spin(_) => anyhow::bail!("Interactive ransactions are not supported yet with the spin backend. Use batch() instead."),
        }
    }
}

/// Configuration for the database client
pub struct Config {
    pub url: url::Url,
    pub auth_token: Option<String>,
}

/// Establishes a database client based on `Config` struct
///
/// # Examples
///
/// ```
/// # async fn f() {
/// # use libsql_client::Config;
/// let config = Config { url: url::Url::parse("file:////tmp/example.db").unwrap(), auth_token: None };
/// let db = libsql_client::new_client_from_config(config).await.unwrap();
/// # }
/// ```
pub async fn new_client_from_config<'a>(config: Config) -> anyhow::Result<Client> {
    let scheme = config.url.scheme();
    Ok(match scheme {
        #[cfg(feature = "local_backend")]
        "file" => {
            Client::Local(crate::local::Client::new(config.url.to_string())?)
        },
        #[cfg(feature = "hrana_backend")]
        "ws" | "wss" => {
            Client::Hrana(crate::hrana::Client::from_config(config).await?)
        },
        #[cfg(feature = "reqwest_backend")]
        "libsql" => {
            let mut config = config;
            config.url = if config.url.scheme() == "libsql" {
                // We cannot use url::Url::set_scheme() because it prevents changing the scheme to http...
                // Safe to unwrap, because we know that the scheme is libsql
                url::Url::parse(&config.url.as_str().replace("libsql://", "https://")).unwrap()
            } else {
                config.url
            };
            Client::Reqwest(crate::reqwest::Client::from_config(config)?)
        }
        #[cfg(feature = "reqwest_backend")]
        "http" | "https" => {
            Client::Reqwest(crate::reqwest::Client::from_config(config)?)
        },
        #[cfg(feature = "workers_backend")]
        "workers" => {
            Client::Workers(crate::workers::Client::from_config(config).await.map_err(|e| anyhow::anyhow!("{}", e))?)
        },
        #[cfg(feature = "spin_backend")]
        "spin" => {
            Client::Spin(crate::spin::Client::from_config(config))
        },
        _ => anyhow::bail!("Unknown scheme: {scheme}. Make sure your backend exists and is enabled with its feature flag"),
    })
}

/// Establishes a database client based on environment variables
///
/// # Env
/// * `LIBSQL_CLIENT_URL` - URL of the database endpoint - e.g. a https:// endpoint for remote connections
///   (with specified credentials) or local file:/// path for a local database
/// * (optional) `LIBSQL_CLIENT_TOKEN` - authentication token for the database. Skip if your database
///   does not require authentication
/// *
/// # Examples
///
/// ```
/// # async fn run() {
/// # use libsql_client::Config;
/// # std::env::set_var("LIBSQL_CLIENT_URL", "file:////tmp/example.db");
/// let db = libsql_client::new_client().await.unwrap();
/// # }
/// ```
pub async fn new_client() -> anyhow::Result<Client> {
    let url = std::env::var("LIBSQL_CLIENT_URL").map_err(|_| {
        anyhow::anyhow!("LIBSQL_CLIENT_URL variable should point to your libSQL/sqld database")
    })?;
    let auth_token = std::env::var("LIBSQL_CLIENT_TOKEN").ok();
    new_client_from_config(Config {
        url: url::Url::parse(&url)?,
        auth_token,
    })
    .await
}
