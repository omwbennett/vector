use std::borrow::Cow;

use fluss::{
    metadata::{DataType, RowType},
    row::{Date, Datum, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz},
};
use vector_lib::{
    event::{Event, Value},
    lookup::{OwnedTargetPath, owned_value_path},
};

use super::config::{FlussColumnType, FlussSinkConfig, SchemaMode};

#[derive(Debug)]
pub(super) struct TableSchema {
    columns: Vec<SchemaColumn>,
}

#[derive(Debug)]
struct SchemaColumn {
    name: String,
    path: Option<OwnedTargetPath>,
    data_type: DataType,
}

impl TableSchema {
    pub(super) fn new(row_type: &RowType, config: &FlussSinkConfig) -> Result<Self, String> {
        if row_type.fields().is_empty() {
            return Err("table has no columns".into());
        }
        for mapping in &config.columns {
            if !row_type
                .fields()
                .iter()
                .any(|field| field.name() == mapping.to_column)
            {
                return Err(format!("unknown destination column {}", mapping.to_column));
            }
        }
        let columns = row_type
            .fields()
            .iter()
            .map(|field| {
                let data_type = field.data_type().clone();
                // shortcut: nested types are explicitly rejected until native nested encoding is tested.
                if matches!(
                    data_type,
                    DataType::Array(_) | DataType::Map(_) | DataType::Row(_)
                ) {
                    return Err(format!(
                        "column {} has unsupported type {data_type}",
                        field.name()
                    ));
                }
                let path = match config.schema_mode {
                    SchemaMode::Auto => {
                        Some(OwnedTargetPath::event(owned_value_path!(field.name())))
                    }
                    SchemaMode::Manual => {
                        match config
                            .columns
                            .iter()
                            .find(|mapping| mapping.to_column == field.name())
                        {
                            Some(mapping) => {
                                if mapping.data_type != column_type(&data_type) {
                                    return Err(format!(
                                        "configured type for column {} does not match {data_type}",
                                        field.name()
                                    ));
                                }
                                Some(mapping.from_field.0.clone())
                            }
                            None if data_type.is_nullable() => None,
                            None => {
                                return Err(format!(
                                    "missing mapping for required column {}",
                                    field.name()
                                ));
                            }
                        }
                    }
                };
                Ok(SchemaColumn {
                    name: field.name().to_owned(),
                    path,
                    data_type,
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(Self { columns })
    }

    pub(super) fn encode(&self, event: &Event) -> Result<GenericRow<'static>, String> {
        let mut row = GenericRow::new(self.columns.len());
        for (position, column) in self.columns.iter().enumerate() {
            let value = column.path.as_ref().and_then(|path| match event {
                Event::Log(log) => log.get(path),
                Event::Trace(trace) => trace.get(path),
                Event::Metric(_) => None,
            });
            row.set_field(
                position,
                convert(value.unwrap_or(&Value::Null), &column.data_type)
                    .map_err(|error| format!("column {}: {error}", column.name))?,
            );
        }
        Ok(row)
    }
}

fn column_type(data_type: &DataType) -> FlussColumnType {
    match data_type {
        DataType::Boolean(_) => FlussColumnType::Boolean,
        DataType::TinyInt(_) => FlussColumnType::Tinyint,
        DataType::SmallInt(_) => FlussColumnType::Smallint,
        DataType::Int(_) => FlussColumnType::Int,
        DataType::BigInt(_) => FlussColumnType::Bigint,
        DataType::Float(_) => FlussColumnType::Float,
        DataType::Double(_) => FlussColumnType::Double,
        DataType::Decimal(_) => FlussColumnType::Decimal,
        DataType::String(_) => FlussColumnType::String,
        DataType::Char(_) => FlussColumnType::Char,
        DataType::Bytes(_) => FlussColumnType::Bytes,
        DataType::Binary(_) => FlussColumnType::Binary,
        DataType::Date(_) => FlussColumnType::Date,
        DataType::Time(_) => FlussColumnType::Time,
        DataType::Timestamp(_) => FlussColumnType::Timestamp,
        DataType::TimestampLTz(_) => FlussColumnType::TimestampLtz,
        DataType::Array(_) => FlussColumnType::Array,
        DataType::Map(_) => FlussColumnType::Map,
        DataType::Row(_) => unreachable!("nested columns rejected before mapping"),
    }
}

fn convert(value: &Value, data_type: &DataType) -> Result<Datum<'static>, String> {
    if matches!(value, Value::Null) {
        return if data_type.is_nullable() {
            Ok(Datum::Null)
        } else {
            Err("null or missing required value".into())
        };
    }
    let invalid =
        || format!("expected {data_type}; value has wrong type, range, length or precision");
    match (data_type, value) {
        (DataType::Boolean(_), Value::Boolean(value)) => Ok(Datum::Bool(*value)),
        (DataType::TinyInt(_), Value::Integer(value)) => {
            i8::try_from(*value).map(Datum::Int8).map_err(|_| invalid())
        }
        (DataType::SmallInt(_), Value::Integer(value)) => i16::try_from(*value)
            .map(Datum::Int16)
            .map_err(|_| invalid()),
        (DataType::Int(_), Value::Integer(value)) => i32::try_from(*value)
            .map(Datum::Int32)
            .map_err(|_| invalid()),
        (DataType::BigInt(_), Value::Integer(value)) => Ok(Datum::Int64(*value)),
        (DataType::Float(_) | DataType::Double(_), Value::Float(_) | Value::Integer(_)) => {
            let number = match value {
                Value::Float(number) => number.into_inner(),
                Value::Integer(number) => *number as f64,
                _ => unreachable!(),
            };
            if !number.is_finite() {
                return Err(invalid());
            }
            if matches!(data_type, DataType::Float(_)) {
                let narrowed = number as f32;
                if !narrowed.is_finite() || (number != 0.0 && narrowed == 0.0) {
                    return Err(invalid());
                }
                if let Value::Integer(integer) = value
                    && narrowed as i128 != i128::from(*integer)
                {
                    return Err(invalid());
                }
                Ok(Datum::from(narrowed))
            } else {
                if let Value::Integer(integer) = value
                    && number as i128 != i128::from(*integer)
                {
                    return Err(invalid());
                }
                Ok(Datum::from(number))
            }
        }
        (DataType::Decimal(decimal), value) => {
            let text = match value {
                Value::Integer(number) => Cow::Owned(number.to_string()),
                _ => Cow::Borrowed(value.to_str().ok_or_else(invalid)?),
            };
            let unscaled =
                parse_decimal(&text, decimal.precision(), decimal.scale()).ok_or_else(invalid)?;
            Decimal::from_arrow_decimal128(
                unscaled,
                i64::from(decimal.scale()),
                decimal.precision(),
                decimal.scale(),
            )
            .map(Datum::Decimal)
            .map_err(|_| invalid())
        }
        (DataType::String(_) | DataType::Char(_), value) => {
            let string = value.to_str().ok_or_else(invalid)?;
            if let DataType::Char(char_type) = data_type
                && string.chars().count() > char_type.length() as usize
            {
                return Err(invalid());
            }
            Ok(Datum::String(Cow::Owned(string.to_owned())))
        }
        (DataType::Bytes(_) | DataType::Binary(_), value) => {
            let bytes = value.as_bytes().ok_or_else(invalid)?;
            if let DataType::Binary(binary) = data_type
                && bytes.len() != binary.length()
            {
                return Err(invalid());
            }
            Ok(Datum::Blob(Cow::Owned(bytes.to_vec())))
        }
        (DataType::Date(_), Value::Integer(days)) => i32::try_from(*days)
            .map(|days| Datum::Date(Date::new(days)))
            .map_err(|_| invalid()),
        (DataType::Date(_), Value::Timestamp(timestamp)) => {
            i32::try_from(timestamp.timestamp().div_euclid(86_400))
                .map(|days| Datum::Date(Date::new(days)))
                .map_err(|_| invalid())
        }
        (DataType::Time(time), Value::Integer(millis)) => {
            if !(0..86_400_000).contains(millis)
                || millis % 10_i64.pow(3_u32.saturating_sub(time.precision())) != 0
            {
                return Err(invalid());
            }
            Ok(Datum::Time(Time::new(*millis as i32)))
        }
        (
            DataType::Timestamp(_) | DataType::TimestampLTz(_),
            Value::Timestamp(_) | Value::Integer(_),
        ) => {
            let (millis, nanos) = match value {
                Value::Timestamp(timestamp)
                    if timestamp.timestamp_subsec_nanos() < 1_000_000_000 =>
                {
                    (
                        timestamp.timestamp_millis(),
                        (timestamp.timestamp_subsec_nanos() % 1_000_000) as i32,
                    )
                }
                Value::Integer(millis) => (*millis, 0),
                _ => return Err(invalid()),
            };
            let precision = match data_type {
                DataType::Timestamp(t) => t.precision(),
                DataType::TimestampLTz(t) => t.precision(),
                _ => unreachable!(),
            };
            let total_nanos = i128::from(millis) * 1_000_000 + i128::from(nanos);
            if total_nanos % 10_i128.pow(9 - precision) != 0 {
                return Err(invalid());
            }
            // The native Arrow writer uses i64 seconds/milliseconds/microseconds/nanoseconds.
            let arrow_divisor = match precision {
                0 => 1_000_000_000,
                1..=3 => 1_000_000,
                4..=6 => 1_000,
                _ => 1,
            };
            i64::try_from(total_nanos / arrow_divisor).map_err(|_| invalid())?;
            match data_type {
                DataType::Timestamp(_) => TimestampNtz::from_millis_nanos(millis, nanos)
                    .map(Datum::TimestampNtz)
                    .map_err(|_| invalid()),
                _ => TimestampLtz::from_millis_nanos(millis, nanos)
                    .map(Datum::TimestampLtz)
                    .map_err(|_| invalid()),
            }
        }
        _ => Err(invalid()),
    }
}

// Decimal inputs are plain base-10 strings or integers, never binary floating point.
// Reject rounding and overflow before constructing the native decimal.
fn parse_decimal(text: &str, precision: u32, scale: u32) -> Option<i128> {
    let negative = text.starts_with('-');
    let digits = text
        .strip_prefix('-')
        .or_else(|| text.strip_prefix('+'))
        .unwrap_or(text);
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let significant_fraction = fraction.trim_end_matches('0');
    if significant_fraction.len() > scale as usize {
        return None;
    }
    let mut value = 0_i128;
    for digit in whole.bytes().chain(significant_fraction.bytes()) {
        value = value
            .checked_mul(10)?
            .checked_add(i128::from(digit - b'0'))?;
    }
    value = value.checked_mul(10_i128.checked_pow(scale - significant_fraction.len() as u32)?)?;
    if value >= 10_i128.checked_pow(precision)? {
        return None;
    }
    Some(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::super::config::ColumnMapping;
    use super::*;
    use crate::{config::GenerateConfig, event::LogEvent};
    use fluss::metadata::{
        BinaryType, CharType, DataField, DataTypes, DecimalType, TimeType, TimestampType,
    };

    fn config() -> FlussSinkConfig {
        serde_json::from_value(FlussSinkConfig::generate_config()).unwrap()
    }

    #[test]
    fn checked_numbers_and_nullability() {
        assert_eq!(
            convert(&Value::Integer(127), &DataTypes::tinyint()).unwrap(),
            Datum::Int8(127)
        );
        assert!(convert(&Value::Integer(128), &DataTypes::tinyint()).is_err());
        assert!(convert(&Value::Integer(i64::MAX), &DataTypes::int()).is_err());
        assert!(convert(&Value::from(1.5), &DataTypes::bigint()).is_err());
        assert!(convert(&Value::from("true"), &DataTypes::boolean()).is_err());
        assert!(convert(&Value::Null, &DataTypes::int().as_non_nullable()).is_err());
        assert_eq!(
            convert(&Value::Null, &DataTypes::int()).unwrap(),
            Datum::Null
        );
        assert!(convert(&Value::Integer(i64::MAX), &DataTypes::double()).is_err());
        assert!(convert(&Value::from(f64::MAX), &DataTypes::float()).is_err());
    }

    #[test]
    fn decimals_preserve_scale_without_rounding_or_overflow() {
        let decimal = DataType::Decimal(DecimalType::new(6, 2).unwrap());
        let value = convert(&Value::from("-1234.50"), &decimal).unwrap();
        assert_eq!(value.as_decimal().to_unscaled_long().unwrap(), -123450);
        assert_eq!(value.as_decimal().scale(), 2);
        assert!(convert(&Value::from("12345.67"), &decimal).is_err());
        assert!(convert(&Value::from("0.001"), &decimal).is_err());
        assert!(convert(&Value::from(0.1), &decimal).is_err());
        assert_eq!(parse_decimal("1.2300", 4, 2), Some(123));
        assert_eq!(parse_decimal("1e2", 4, 2), None);
        assert_eq!(parse_decimal("", 4, 2), None);
        let large = "99999999999999999999999999999999999999";
        let decimal = DataType::Decimal(DecimalType::new(38, 0).unwrap());
        assert_eq!(
            convert(&Value::from(large), &decimal)
                .unwrap()
                .as_decimal()
                .to_string(),
            large
        );
        assert!(convert(&Value::from(format!("1{large}")), &decimal).is_err());
    }

    #[test]
    fn lengths_and_utf8_are_validated() {
        assert!(convert(&Value::Bytes(vec![0xff].into()), &DataTypes::string()).is_err());
        let char_type = DataType::Char(CharType::new(2));
        assert!(convert(&Value::from("éé"), &char_type).is_ok());
        assert!(convert(&Value::from("abc"), &char_type).is_err());
        let binary = DataType::Binary(BinaryType::new(2));
        assert!(convert(&Value::from("ab"), &binary).is_ok());
        assert!(convert(&Value::from("a"), &binary).is_err());
        assert!(convert(&Value::Integer(42), &DataTypes::string()).is_err());
    }

    #[test]
    fn string_and_bytes_values_encode_identically() {
        for (text, data_type) in [
            ("hello", DataTypes::string()),
            ("éé", DataType::Char(CharType::new(2))),
            ("ab", DataTypes::bytes()),
            ("ab", DataType::Binary(BinaryType::new(2))),
            (
                "-1234.50",
                DataType::Decimal(DecimalType::new(6, 2).unwrap()),
            ),
        ] {
            let raw = Value::Bytes(bytes::Bytes::copy_from_slice(text.as_bytes()));
            assert_eq!(
                convert(&Value::from(text), &data_type).unwrap(),
                convert(&raw, &data_type).unwrap()
            );
        }
        let raw = vec![0, 0xff];
        assert_eq!(
            convert(&Value::Bytes(raw.clone().into()), &DataTypes::bytes()).unwrap(),
            Datum::Blob(Cow::Owned(raw))
        );
    }

    #[test]
    fn temporal_precision_and_negative_epoch() {
        let timestamp = chrono::DateTime::parse_from_rfc3339("1969-12-31T23:59:59.999123456Z")
            .unwrap()
            .to_utc();
        let value = Value::Timestamp(timestamp);
        let datum = convert(&value, &DataType::Timestamp(TimestampType::new(9).unwrap())).unwrap();
        assert_eq!(datum.as_timestamp_ntz().get_millisecond(), -1);
        assert_eq!(datum.as_timestamp_ntz().get_nano_of_millisecond(), 123456);
        assert!(convert(&value, &DataType::Timestamp(TimestampType::new(3).unwrap())).is_err());
        assert_eq!(
            convert(&value, &DataTypes::date())
                .unwrap()
                .as_date()
                .get_inner(),
            -1
        );
        let time = DataType::Time(TimeType::new(0).unwrap());
        assert!(convert(&Value::Integer(1000), &time).is_ok());
        for invalid in [-1, 1, 86_400_000] {
            assert!(convert(&Value::Integer(invalid), &time).is_err());
        }
        assert!(
            convert(
                &Value::Integer(i64::MAX),
                &DataType::Timestamp(TimestampType::new(9).unwrap())
            )
            .is_err()
        );
    }

    #[test]
    fn manual_mappings_use_table_positions_and_required_fields() {
        let row_type = RowType::new(vec![
            DataField::new("id", DataTypes::int().as_non_nullable(), None),
            DataField::new("message", DataTypes::string(), None),
            DataField::new("optional", DataTypes::string(), None),
        ]);
        let mut config = config();
        config.schema_mode = SchemaMode::Manual;
        config.columns = vec![
            ColumnMapping {
                from_field: ".body".into(),
                to_column: "message".into(),
                data_type: FlussColumnType::String,
            },
            ColumnMapping {
                from_field: ".number".into(),
                to_column: "id".into(),
                data_type: FlussColumnType::Int,
            },
        ];
        let schema = TableSchema::new(&row_type, &config).unwrap();
        let mut log = LogEvent::from("unused");
        log.insert(vector_lib::lookup::event_path!("body"), "hello");
        log.insert(vector_lib::lookup::event_path!("number"), 42);
        let row = schema.encode(&Event::Log(log)).unwrap();
        assert_eq!(
            row.values,
            vec![Datum::Int32(42), Datum::from("hello"), Datum::Null]
        );
        assert!(
            schema
                .encode(&Event::Log(LogEvent::from("missing id")))
                .is_err()
        );
        config.columns.pop();
        assert!(TableSchema::new(&row_type, &config).is_err());
        config.columns[0].to_column = "nonexistent".into();
        assert!(TableSchema::new(&row_type, &config).is_err());
    }

    #[test]
    fn auto_names_are_literal_and_nested_types_are_rejected() {
        let row_type = RowType::new(vec![DataField::new("a.b", DataTypes::int(), None)]);
        let schema = TableSchema::new(&row_type, &config()).unwrap();
        let mut log = LogEvent::from("test");
        log.insert(vector_lib::lookup::event_path!("a.b"), 7);
        assert_eq!(
            schema.encode(&Event::Log(log)).unwrap().values,
            vec![Datum::Int32(7)]
        );
        let row_type = RowType::new(vec![DataField::new(
            "items",
            DataTypes::array(DataTypes::int()),
            None,
        )]);
        assert!(
            TableSchema::new(&row_type, &config())
                .unwrap_err()
                .contains("unsupported")
        );
    }
}
