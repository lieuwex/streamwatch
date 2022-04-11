pub mod duration_seconds {
    use chrono::Duration;
    use serde::{de, Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(duration.num_seconds())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_i64(DurationVisitor)
    }

    struct DurationVisitor;

    impl<'de> de::Visitor<'de> for DurationVisitor {
        type Value = Duration;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(
                formatter,
                "an integer containing the number of seconds of the duration"
            )
        }

        fn visit_i64<E>(self, value: i64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            Ok(Duration::seconds(value))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            i64::try_from(value)
                .map(|s| Duration::seconds(s))
                .map_err(|_| E::custom(format!("value out of range for i64: {}", value)))
        }

        fn visit_f64<E>(self, value: f64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            if value.floor() == value {
                Ok(Duration::seconds(value as i64))
            } else {
                Err(E::custom(format!("value out of range for i64: {}", value)))
            }
        }
    }
}

pub mod duration_seconds_float {
    use chrono::Duration;
    use serde::{de, Deserializer, Serializer};
    use std::fmt;

    use crate::functions::{duration_to_seconds_float, seconds_float_to_duration};

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let seconds = duration_to_seconds_float(duration);
        serializer.serialize_f64(seconds)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_f64(DurationVisitor)
    }

    struct DurationVisitor;

    impl<'de> de::Visitor<'de> for DurationVisitor {
        type Value = Duration;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(
                formatter,
                "a floating point number containing the number of seconds of the duration"
            )
        }

        fn visit_i64<E>(self, value: i64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            Ok(Duration::seconds(value))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            i64::try_from(value)
                .map(|s| Duration::seconds(s))
                .map_err(|_| E::custom(format!("value out of range for i64: {}", value)))
        }

        fn visit_f64<E>(self, value: f64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            Ok(seconds_float_to_duration(value))
        }
    }
}
