pub mod duration_seconds {
    use serde::{de, Deserializer, Serializer};
    use std::fmt;
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
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

        fn visit_u64<E>(self, value: u64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            Ok(Duration::from_secs(value))
        }

        fn visit_i64<E>(self, value: i64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            u64::try_from(value)
                .map(|s| Duration::from_secs(s))
                .map_err(|_| E::custom(format!("value out of range for u64: {}", value)))
        }

        fn visit_f64<E>(self, value: f64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            Ok(Duration::from_secs_f64(value))
        }
    }
}

pub mod duration_seconds_float {
    use serde::{de, Deserializer, Serializer};
    use std::fmt;
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(duration.as_secs_f64())
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
                "an integer containing the number of seconds of the duration"
            )
        }

        fn visit_u64<E>(self, value: u64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            Ok(Duration::from_secs(value))
        }

        fn visit_i64<E>(self, value: i64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            u64::try_from(value)
                .map(|s| Duration::from_secs(s))
                .map_err(|_| E::custom(format!("value out of range for u64: {}", value)))
        }

        fn visit_f64<E>(self, value: f64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            Ok(Duration::from_secs_f64(value))
        }
    }
}

pub mod duration_milliseconds {
    use serde::{de, Deserializer, Serializer};
    use std::fmt;
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u128(duration.as_millis())
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
                "an integer containing the number of milliseconds of the duration"
            )
        }

        fn visit_u64<E>(self, value: u64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            Ok(Duration::from_millis(value))
        }

        fn visit_i64<E>(self, value: i64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            u64::try_from(value)
                .map(|s| Duration::from_millis(s))
                .map_err(|_| E::custom(format!("value out of range for u64: {}", value)))
        }

        fn visit_f64<E>(self, value: f64) -> Result<Duration, E>
        where
            E: de::Error,
        {
            Ok(Duration::from_secs_f64(value / 1e3))
        }
    }
}
