//! libsql deserialization utilities.

use serde::de::{value::Error as DeError, Error};
use std::collections::hash_map::Iter;

use hrana_client_proto::Value;
use serde::{
    de::{value::SeqDeserializer, IntoDeserializer, MapAccess, Visitor},
    Deserialize, Deserializer,
};

use crate::Row;

/// Deserialize from a [`Row`] into any type `T` that implements [`serde::Deserialize`].
///
/// # Types
///
/// Structs must match their field name to the column name but the order does not matter.
/// There is a limited set of Rust types which are supported and those are:
///
/// - String
/// - Vec<u8>
/// - i64
/// - f64
/// - bool
/// - Option<T> (where T is any of the above)
/// - ()
///
/// # Example
///
/// ```no_run
/// # async fn run(db: libsql_client::Client) -> anyhow::Result<()> {
/// use libsql_client::de;
///
/// #[derive(Debug, serde::Deserialize)]
/// struct User {
///     name: String,
///     email: String,
///     age: i64,
/// }
///
/// let users = db
///     .execute("SELECT * FROM users")
///     .await?
///     .rows
///     .iter()
///     .map(de::from_row)
///     .collect::<Result<Vec<User>, _>>()?;
///
/// println!("Users: {:?}", users);
/// # Ok(())
/// # }
/// ```
pub fn from_row<'de, T: Deserialize<'de>>(row: &'de Row) -> anyhow::Result<T> {
    let de = De { row };
    T::deserialize(de).map_err(Into::into)
}

struct De<'de> {
    row: &'de Row,
}

impl<'de> Deserializer<'de> for De<'de> {
    type Error = serde::de::value::Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(DeError::custom("Expects a struct"))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        struct RowMapAccess<'a> {
            iter: Iter<'a, String, Value>,
            value: Option<&'a Value>,
        }

        impl<'de> MapAccess<'de> for RowMapAccess<'de> {
            type Error = serde::de::value::Error;

            fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
            where
                K: serde::de::DeserializeSeed<'de>,
            {
                if let Some((k, v)) = self.iter.next() {
                    self.value = Some(v);
                    seed.deserialize(k.to_string().into_deserializer())
                        .map(Some)
                } else {
                    Ok(None)
                }
            }

            fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
            where
                V: serde::de::DeserializeSeed<'de>,
            {
                let value = self
                    .value
                    .take()
                    .expect("next_value called before next_key");

                seed.deserialize(V(value))
            }
        }

        visitor.visit_map(RowMapAccess {
            iter: self.row.value_map.iter(),
            value: None,
        })
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map enum identifier ignored_any
    }
}

struct V<'a>(&'a Value);

impl<'de> Deserializer<'de> for V<'de> {
    type Error = serde::de::value::Error;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Value::Text { value } => visitor.visit_string(value.to_string()),
            Value::Null => visitor.visit_unit(),
            Value::Integer { value } => visitor.visit_i64(*value),
            Value::Float { value } => visitor.visit_f64(*value),
            Value::Blob { value } => {
                let seq = SeqDeserializer::new(value.iter().cloned());
                visitor.visit_seq(seq)
            }
        }
    }

    #[inline]
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Value::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    #[inline]
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Value::Integer { value: 1 } => visitor.visit_bool(true),
            Value::Integer { value: 0 } => visitor.visit_bool(false),
            _ => Err(DeError::custom("Expects a bool")),
        }
    }

    serde::forward_to_deserialize_any! {
        i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct seq tuple
        tuple_struct map enum struct identifier ignored_any
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[derive(serde::Deserialize)]
    #[allow(unused)]
    struct Foo {
        bar: String,
        baf: f64,
        baf2: f64,
        baz: i64,
        bab: Vec<u8>,
        ban: (),
        bad: Option<i64>,
        bac: Option<f64>,
        bag: Option<Vec<u8>>,
        bah: bool,
        bax: Option<bool>,
    }

    #[test]
    fn struct_from_row() {
        let mut row = Row {
            values: Vec::new(),
            value_map: HashMap::new(),
        };
        row.value_map.insert(
            "bar".to_string(),
            Value::Text {
                value: "foo".into(),
            },
        );
        row.value_map
            .insert("baz".to_string(), Value::Integer { value: 42 });
        row.value_map
            .insert("baf".to_string(), Value::Float { value: 42.0 });
        row.value_map
            .insert("baf2".to_string(), Value::Float { value: 43.0 });

        row.value_map.insert(
            "bab".to_string(),
            Value::Blob {
                value: vec![6u8; 128],
            },
        );
        row.value_map.insert("ban".to_string(), Value::Null);
        row.value_map
            .insert("bad".to_string(), Value::Integer { value: 42 });
        row.value_map.insert("bac".to_string(), Value::Null);
        row.value_map.insert(
            "bag".to_string(),
            Value::Blob {
                value: vec![6u8; 128],
            },
        );
        row.value_map
            .insert("bah".to_string(), Value::Integer { value: 1 });
        row.value_map
            .insert("bax".to_string(), Value::Integer { value: 0 });

        let foo = from_row::<Foo>(&row).unwrap();

        assert_eq!(&foo.bar, &"foo");
        assert_eq!(foo.baz, 42);
        assert!(foo.baf > 41.0);
        assert!(foo.baf2 > 42.0);
        assert_eq!(foo.bab, vec![6u8; 128]);
        assert_eq!(foo.bad, Some(42));
        assert_eq!(foo.bac, None);
        assert_eq!(foo.bag, Some(vec![6u8; 128]));
        assert!(foo.bah);
        assert_eq!(foo.bax, Some(false));
    }
}
