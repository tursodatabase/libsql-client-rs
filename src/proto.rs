//! `proto` contains libSQL/sqld/hrana wire protocol.

#[cfg(feature = "hrana_backend")]
pub use hrana_client::proto::{BatchResult, Col, Error, StmtResult, Value};
#[cfg(not(feature = "hrana_backend"))]
pub use hrana_client_proto::{BatchResult, Col, Error, StmtResult, Value};
