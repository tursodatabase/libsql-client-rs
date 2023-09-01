//! [Client] is the main structure to interact with the database.
use crate::{
    proto, BatchResult, Error, Result, ResultSet, Statement, SyncTransaction, Transaction,
};

static TRANSACTION_IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A generic client struct, wrapping possible backends.
/// It's a convenience struct which allows implementing connect()
/// with backends being passed as env parameters.
#[derive(Debug)]
pub enum Client {
    #[cfg(feature = "local_backend")]
    Local(crate::local::Client),
    #[cfg(any(
        feature = "reqwest_backend",
        feature = "workers_backend",
        feature = "spin_backend"
    ))]
    Http(crate::http::Client),
    #[cfg(feature = "hrana_backend")]
    Hrana(crate::hrana::Client),
    Default,
}

/// A synchronous flavor of [Client]. All its public methods are synchronous,
/// to make it usable in environments that don't support async/await.
pub struct SyncClient {
    inner: Client,
}

unsafe impl Send for Client {}

impl Client {
    /// Executes a batch of independent SQL statements.
    ///
    /// For a version in which statements execute transactionally, see [`Client::batch()`]
    /// # Arguments
    /// * `stmts` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::Client::in_memory().unwrap();
    /// # db.execute("create table foo(bar text)").await.unwrap();
    /// let res = db.raw_batch([
    ///   "select * from foo",
    ///   "insert into foo(bar) values ('bar')"
    /// ]).await.unwrap();
    /// # }
    /// ```
    pub async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement> + Send> + Send,
    ) -> Result<BatchResult> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.raw_batch(stmts),
            #[cfg(any(
                feature = "reqwest_backend",
                feature = "workers_backend",
                feature = "spin_backend"
            ))]
            Self::Http(r) => r.raw_batch(stmts).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.raw_batch(stmts).await,
            _ => panic!("Must enable at least one feature"),
        }
    }

    /// Transactionally executes a batch of SQL statements.
    ///
    /// For a version in which statements can fail or succeed independently, see [`Client::raw_batch()`]
    /// # Arguments
    /// * `stmts` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::Client::in_memory().unwrap();
    /// # db.execute("create table foo(bar text)").await.unwrap();
    /// let res = db.batch([
    ///   "select * from foo",
    ///   "insert into foo(bar) values ('bar')"
    /// ]).await.unwrap();
    /// assert_eq!(res.len(), 2)
    /// # }
    /// ```
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

    /// Transactionally executes a batch of SQL statements, in synchronous contexts.
    ///
    /// This method calls [block_on](`futures::executor::block_on()`) internally.
    ///
    /// For the async version of this method, see [`Client::batch()`]
    /// # Arguments
    /// * `stmts` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// # use libsql_client::Config;
    /// # std::env::set_var("LIBSQL_CLIENT_URL", "file:////tmp/example.db");
    /// let db = libsql_client::Client::in_memory().unwrap();
    /// # db.execute("create table foo(bar text)").await.unwrap();
    /// let res = db.batch_sync([
    ///   "select * from foo",
    ///   "insert into foo(bar) values ('bar')"
    /// ]).unwrap();
    /// assert_eq!(res.len(), 2)
    /// # }
    /// ```
    pub fn batch_sync<I: IntoIterator<Item = impl Into<Statement> + Send> + Send>(
        &self,
        stmts: I,
    ) -> Result<Vec<ResultSet>>
    where
        <I as std::iter::IntoIterator>::IntoIter: std::marker::Send,
    {
        futures::executor::block_on(self.batch(stmts))
    }

    /// Executes a single SQL statement
    ///
    /// # Arguments
    /// * `stmt` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::Client::in_memory().unwrap();
    /// # db.execute("create table foo(bar text)").await.unwrap();
    /// db.execute("select * from foo").await.unwrap();
    /// # }
    /// ```
    pub async fn execute(&self, stmt: impl Into<Statement> + Send) -> Result<ResultSet> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.execute(stmt),
            #[cfg(any(
                feature = "reqwest_backend",
                feature = "workers_backend",
                feature = "spin_backend"
            ))]
            Self::Http(r) => r.execute(stmt).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.execute(stmt).await,
            _ => panic!("Must enable at least one feature"),
        }
    }

    /// Creates an interactive transaction
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::Client::in_memory().unwrap();
    /// # db.execute("create table foo(bar text)").await.unwrap();
    /// let tx = db.transaction().await.unwrap();
    /// tx.execute("select * from foo").await.unwrap();
    /// tx.commit();
    /// # }
    /// ```
    pub async fn transaction(&self) -> Result<Transaction> {
        let id = TRANSACTION_IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Transaction::new(self, id).await
    }

    pub(crate) async fn execute_in_transaction(
        &self,
        tx_id: u64,
        stmt: Statement,
    ) -> Result<ResultSet> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.execute_in_transaction(tx_id, stmt),
            #[cfg(any(
                feature = "reqwest_backend",
                feature = "workers_backend",
                feature = "spin_backend"
            ))]
            Self::Http(r) => r.execute_in_transaction(tx_id, stmt).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.execute_in_transaction(tx_id, stmt).await,

            _ => panic!("Must enable at least one feature"),
        }
    }

    pub(crate) async fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.commit_transaction(tx_id),
            #[cfg(any(
                feature = "reqwest_backend",
                feature = "workers_backend",
                feature = "spin_backend"
            ))]
            Self::Http(r) => r.commit_transaction(tx_id).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.commit_transaction(tx_id).await,

            _ => panic!("Must enable at least one feature"),
        }
    }

    pub(crate) async fn rollback_transaction(&self, tx_id: u64) -> Result<()> {
        match self {
            #[cfg(feature = "local_backend")]
            Self::Local(l) => l.rollback_transaction(tx_id),
            #[cfg(any(
                feature = "reqwest_backend",
                feature = "workers_backend",
                feature = "spin_backend"
            ))]
            Self::Http(r) => r.rollback_transaction(tx_id).await,
            #[cfg(feature = "hrana_backend")]
            Self::Hrana(h) => h.rollback_transaction(tx_id).await,

            _ => panic!("Must enable at least one feature"),
        }
    }
}

impl Client {
    /// Creates an in-memory database
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn f() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::Client::in_memory().unwrap();
    /// # }
    /// ```
    #[cfg(feature = "local_backend")]
    pub fn in_memory() -> Result<Client> {
        Ok(Client::Local(crate::local::Client::in_memory()?))
    }

    /// Establishes a database client based on [Config] struct
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn f() {
    /// # use libsql_client::Config;
    /// let config = Config {
    ///   url: url::Url::parse("file:////tmp/example.db").unwrap(),
    ///   auth_token: None
    /// };
    /// let db = libsql_client::Client::from_config(config).await.unwrap();
    /// # }
    /// ```
    #[allow(unreachable_patterns)]
    pub async fn from_config<'a>(mut config: Config) -> Result<Client> {
        config.url = if config.url.scheme() == "libsql" {
            // We cannot use url::Url::set_scheme() because it prevents changing the scheme to http...
            // Safe to unwrap, because we know that the scheme is libsql
            url::Url::parse(&config.url.as_str().replace("libsql://", "https://")).unwrap()
        } else {
            config.url
        };
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
            "http" | "https" => {
                let inner = crate::http::InnerClient::Reqwest(crate::reqwest::HttpClient::new());
                Client::Http(crate::http::Client::from_config(inner, config)?)
            },
            #[cfg(feature = "workers_backend")]
            "workers" | "http" | "https" => {
                let inner = crate::http::InnerClient::Workers(crate::workers::HttpClient::new());
                Client::Http(crate::http::Client::from_config(inner, config)?)
            },
            #[cfg(feature = "spin_backend")]
            "spin" | "http" | "https" => {
                let inner = crate::http::InnerClient::Spin(crate::spin::HttpClient::new());
                Client::Http(crate::http::Client::from_config(inner, config)?)
            },
            _ => return Err(Error::Misuse(format!("Unknown scheme: {scheme}. Make sure your backend exists and is enabled with its feature flag"))),
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
    /// let db = libsql_client::Client::from_env().await.unwrap();
    /// # }
    /// ```
    pub async fn from_env() -> Result<Client> {
        let url = std::env::var("LIBSQL_CLIENT_URL").map_err(|_| {
            Error::Misuse(
                "LIBSQL_CLIENT_URL variable should point to your libSQL/sqld database".into(),
            )
        })?;
        let auth_token = std::env::var("LIBSQL_CLIENT_TOKEN").ok();
        Self::from_config(Config {
            url: url::Url::parse(&url).map_err(|e| Error::Misuse(e.to_string()))?,
            auth_token,
        })
        .await
    }

    #[cfg(feature = "workers_backend")]
    pub fn from_workers_env(env: &worker::Env) -> anyhow::Result<Client> {
        let url = env
            .secret("LIBSQL_CLIENT_URL")
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .to_string();
        let token = env
            .secret("LIBSQL_CLIENT_TOKEN")
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .to_string();
        let config = Config {
            url: url::Url::parse(&url)?,
            auth_token: Some(token),
        };
        let inner = crate::http::InnerClient::Workers(crate::workers::HttpClient::new());
        Ok(Client::Http(crate::http::Client::from_config(
            inner, config,
        )?))
    }
}

pub mod sync {}
impl SyncClient {
    /// Creates an in-memory database
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn f() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::SyncClient::in_memory().unwrap();
    /// # }
    /// ```
    #[cfg(feature = "local_backend")]
    pub fn in_memory() -> Result<Self> {
        Ok(Self {
            inner: Client::in_memory()?,
        })
    }

    /// Establishes a database client based on [Config] struct
    ///
    /// # Examples
    ///
    /// ```
    /// # fn f() {
    /// # use libsql_client::Config;
    /// let config = Config { url: url::Url::parse("file:////tmp/example.db").unwrap(), auth_token: None };
    /// let db = libsql_client::SyncClient::from_config(config).unwrap();
    /// # }
    /// ```
    pub fn from_config(config: Config) -> Result<Self> {
        Ok(Self {
            inner: futures::executor::block_on(Client::from_config(config))?,
        })
    }

    /// Establishes a database client based on environment variables
    ///
    /// # Env
    /// * `LIBSQL_CLIENT_URL` - URL of the database endpoint - e.g. a https:// endpoint for remote connections
    ///   (with specified credentials) or local file:/// path for a local database
    /// * (optional) `LIBSQL_CLIENT_TOKEN` - authentication token for the database. Skip if your database
    ///   does not require authentication
    ///
    /// # Examples
    ///
    /// ```
    /// # fn run() {
    /// # use libsql_client::Config;
    /// # std::env::set_var("LIBSQL_CLIENT_URL", "file:////tmp/example.db");
    /// let db = libsql_client::SyncClient::from_env().unwrap();
    /// # }
    /// ```
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            inner: futures::executor::block_on(Client::from_env())?,
        })
    }

    #[cfg(feature = "workers_backend")]
    pub fn from_workers_env(env: &worker::Env) -> Result<Self> {
        Ok(Self {
            inner: Client::from_workers_env(env)?,
        })
    }

    /// Executes a batch of independent SQL statements.
    ///
    /// For a version in which statements execute transactionally, see [`SyncClient::batch()`]
    /// # Arguments
    /// * `stmts` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # fn run() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::SyncClient::in_memory().unwrap();
    /// # db.execute("create table foo(bar text)").unwrap();
    /// let res = db.raw_batch([
    ///   "select * from foo",
    ///   "insert into foo(bar) values ('bar')"
    /// ]).unwrap();
    /// # }
    /// ```
    pub fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement> + Send> + Send,
    ) -> Result<BatchResult> {
        futures::executor::block_on(self.inner.raw_batch(stmts))
    }

    /// Transactionally executes a batch of SQL statements.
    ///
    /// For a version in which statements can fail or succeed independently, see [`SyncClient::raw_batch()`]
    /// # Arguments
    /// * `stmts` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # fn run() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::SyncClient::in_memory().unwrap();
    /// # db.execute("create table foo(bar text)").unwrap();
    /// let res = db.batch([
    ///   "select * from foo",
    ///   "insert into foo(bar) values ('bar')"
    /// ]).unwrap();
    /// assert_eq!(res.len(), 2)
    /// # }
    /// ```
    pub fn batch<I: IntoIterator<Item = impl Into<Statement> + Send> + Send>(
        &self,
        stmts: I,
    ) -> Result<Vec<ResultSet>>
    where
        <I as std::iter::IntoIterator>::IntoIter: std::marker::Send,
    {
        futures::executor::block_on(self.inner.batch(stmts))
    }

    /// Executes a single SQL statement
    ///
    /// # Arguments
    /// * `stmt` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::SyncClient::in_memory().unwrap();
    /// # db.execute("create table foo(bar text)").unwrap();
    /// db.execute("select * from foo").unwrap();
    /// # }
    /// ```
    pub fn execute(&self, stmt: impl Into<Statement> + Send) -> Result<ResultSet> {
        futures::executor::block_on(self.inner.execute(stmt))
    }

    /// Creates an interactive transaction
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// # use libsql_client::Config;
    /// let db = libsql_client::SyncClient::in_memory().unwrap();
    /// # db.execute("create table foo(bar text)").unwrap();
    /// let tx = db.transaction().unwrap();
    /// tx.execute("select * from foo").unwrap();
    /// tx.commit();
    /// # }
    /// ```
    pub fn transaction(&self) -> Result<SyncTransaction> {
        let id = TRANSACTION_IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SyncTransaction::new(self, id)
    }

    pub(crate) fn execute_in_transaction(&self, tx_id: u64, stmt: Statement) -> Result<ResultSet> {
        futures::executor::block_on(self.inner.execute_in_transaction(tx_id, stmt))
    }

    pub(crate) fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        futures::executor::block_on(self.inner.commit_transaction(tx_id))
    }

    pub(crate) fn rollback_transaction(&self, tx_id: u64) -> Result<()> {
        futures::executor::block_on(self.inner.rollback_transaction(tx_id))
    }
}

/// Configuration for the database client
#[derive(Debug)]
pub struct Config {
    pub url: url::Url,
    pub auth_token: Option<String>,
}

impl Config {
    /// Create a new [Config]
    /// # Examples
    ///
    /// ```
    /// # async fn f() -> anyhow::Result<()> {
    /// # use libsql_client::Config;
    /// let config = Config::new("file:////tmp/example.db")?;
    /// let db = libsql_client::Client::from_config(config).await.unwrap();
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<T: TryInto<url::Url>>(url: T) -> Result<Self>
    where
        <T as TryInto<url::Url>>::Error: std::fmt::Display,
    {
        Ok(Self {
            url: url
                .try_into()
                .map_err(|e| Error::Misuse(format!("Failed to parse url: {e}")))?,
            auth_token: None,
        })
    }

    /// Adds an authentication token to config
    /// # Examples
    ///
    /// ```
    /// # async fn f() -> anyhow::Result<()> {
    /// # use libsql_client::Config;
    /// let config = Config::new("https://example.com/db")?.with_auth_token("secret");
    /// let db = libsql_client::Client::from_config(config).await.unwrap();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }
}
