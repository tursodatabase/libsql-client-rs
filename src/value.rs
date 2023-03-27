//! `Value` represents libSQL values and types.
//! Each database row consists of one or more cell values.

#[cfg(feature = "hrana_backend")]
pub use hrana_client::proto::Value;
#[cfg(not(feature = "hrana_backend"))]
pub use hrana_client_proto::Value;
