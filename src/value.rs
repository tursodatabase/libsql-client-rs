//! `Value` represents libSQL values and types.
//! Each database row consists of one or more cell values.

use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;

/// Value of a single database cell
// FIXME: We need to support blobs as well
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

// FIXME: we should *not* rely on Display for serialization purposes
impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Integer(n) => write!(f, "{n}"),
            Value::Real(d) => write!(f, "{d}"),
            Value::Text(s) => write!(f, "{}", serde_json::json!(s)),
            Value::Blob(b) => {
                let b = BASE64_STANDARD_NO_PAD.encode(b);
                write!(f, "{{\"base64\": {b}}}")
            }
        }
    }
}

impl From<()> for Value {
    fn from(_: ()) -> Value {
        Value::Null
    }
}

macro_rules! impl_from_value {
    ($typename: ty, $variant: ident) => {
        impl From<$typename> for Value {
            fn from(t: $typename) -> Value {
                Value::$variant(t.into())
            }
        }
    };
}

impl_from_value!(String, Text);
impl_from_value!(&String, Text);
impl_from_value!(&str, Text);

impl_from_value!(i8, Integer);
impl_from_value!(i16, Integer);
impl_from_value!(i32, Integer);
impl_from_value!(i64, Integer);

impl_from_value!(u8, Integer);
impl_from_value!(u16, Integer);
impl_from_value!(u32, Integer);

impl_from_value!(f32, Real);
impl_from_value!(f64, Real);

impl_from_value!(Vec<u8>, Blob);

macro_rules! impl_value_try_from {
    ($variant: ident, $typename: ty) => {
        impl TryFrom<Value> for $typename {
            type Error = String;
            fn try_from(v: Value) -> Result<$typename, Self::Error> {
                match v {
                    Value::$variant(v) => v.try_into().map_err(|e| format!("{e}")),
                    _ => Err(format!(
                        "cannot transform {} to {}",
                        stringify!($ty),
                        stringify!($variant)
                    )),
                }
            }
        }
    };
}

impl_value_try_from!(Text, String);

impl_value_try_from!(Integer, i8);
impl_value_try_from!(Integer, i16);
impl_value_try_from!(Integer, i32);
impl_value_try_from!(Integer, i64);
impl_value_try_from!(Integer, u8);
impl_value_try_from!(Integer, u16);
impl_value_try_from!(Integer, u32);
impl_value_try_from!(Integer, u64);

impl_value_try_from!(Real, f64);

impl_value_try_from!(Blob, Vec<u8>);
