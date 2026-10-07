use crate::utils::error::CsvError;
use arrow::array::ArrayBuilder;
use csv::StringRecord;
use datafusion::arrow::{
    array::*,
    datatypes::{DataType, Field},
};
use std::any::type_name;
use std::error::Error as StdError;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

type Result<T, E = CsvError> = std::result::Result<T, E>;

macro_rules! parse_and_append {
    ($csv_row:expr, $cell_index:expr, $cell_type:ty, $builders:expr, $builder_index:expr, $field:expr, $builder_ty:ty, $type_str:expr) => {
        let mut cell_value = $csv_row[$cell_index].to_string();
        if cell_value.is_empty() {
            if $field.is_nullable() {
                $builders[*$builder_index]
                    .as_any_mut()
                    .downcast_mut::<$builder_ty>()
                    .unwrap()
                    .append_null();
            } else {
                return Err(CsvError::CellParse {
                    expected: $type_str.to_string(),
                    found: cell_value.to_string(),
                    source: "Empty cell for non-nullable field".into(),
                    position: $csv_row.position().cloned(),
                });
            }
        } else {
            if $type_str == "bool" {
                cell_value = cell_value.to_lowercase();
            }
            let parsed_value =
                cell_value
                    .parse::<$cell_type>()
                    .map_err(|e| CsvError::CellParse {
                        expected: $type_str.to_string(),
                        found: cell_value.to_string(),
                        source: Box::new(e),
                        position: $csv_row.position().cloned(),
                    })?;
            $builders[*$builder_index]
                .as_any_mut()
                .downcast_mut::<$builder_ty>()
                .unwrap()
                .append_value(parsed_value);
        }
    };
}

pub fn add_primitive_field_to_builders(
    csv_row: &StringRecord,
    builders: &mut Vec<Box<dyn ArrayBuilder>>,
    builder_index: &usize,
    field: &Arc<Field>,
    cell_index: usize,
) -> Result<()> {
    match field.data_type() {
        DataType::Int64 => {
            parse_and_append!(
                csv_row,
                cell_index,
                i64,
                builders,
                builder_index,
                field,
                Int64Builder,
                "i64"
            );
        }
        DataType::Int32 => {
            parse_and_append!(
                csv_row,
                cell_index,
                i32,
                builders,
                builder_index,
                field,
                Int32Builder,
                "i32"
            );
        }
        DataType::Float64 => {
            parse_and_append!(
                csv_row,
                cell_index,
                f64,
                builders,
                builder_index,
                field,
                Float64Builder,
                "f64"
            );
        }
        DataType::Float32 => {
            parse_and_append!(
                csv_row,
                cell_index,
                f32,
                builders,
                builder_index,
                field,
                Float32Builder,
                "f32"
            );
        }
        DataType::Utf8 => {
            let value = &csv_row[cell_index];
            builders[*builder_index]
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(value);
        }
        DataType::Boolean => {
            parse_and_append!(
                csv_row,
                cell_index,
                bool,
                builders,
                builder_index,
                field,
                BooleanBuilder,
                "bool"
            );
        }
        _ => {
            return Err(CsvError::CellParse {
                expected: format!("{:?}", field.data_type()),
                found: csv_row[cell_index].to_string(),
                source: "Unsupported non-primitive data type".into(),
                position: csv_row.position().cloned(),
            });
        }
    };
    Ok(())
}

pub fn is_datatype_primitive(field: &Field) -> bool {
    match field.data_type() {
        DataType::Int64
        | DataType::Float64
        | DataType::Utf8
        | DataType::Boolean
        | DataType::Int32
        | DataType::Float32 => true,
        _ => false,
    }
}

pub fn parse_value<T>(str_value: &str, save_result: &mut T) -> Result<()>
where
    T: FromStr,
    <T as FromStr>::Err: Display + StdError + Send + Sync + 'static,
{
    if str_value.is_empty() {
        return Err(CsvError::CellParse {
            expected: type_name::<T>().to_string(),
            found: "".to_string(),
            source: "Empty string".into(),
            position: None,
        });
    }
    let parsed_value = str_value.parse::<T>().map_err(|e| CsvError::CellParse {
        expected: type_name::<T>().to_string(),
        found: str_value.to_string(),
        source: Box::new(e),
        position: None,
    })?;
    *save_result = parsed_value;
    Ok(())
}

// Special version for Option<T>
pub fn parse_optional<'a, T>(str_value: &'a str, save_result: &mut Option<T>) -> Result<()>
where
    T: FromStr,
    <T as FromStr>::Err: Display + StdError + Send + Sync + 'static,
{
    if str_value.is_empty() {
        *save_result = None;
    } else {
        let parsed_value = str_value.parse::<T>().map_err(|e| CsvError::CellParse {
            expected: type_name::<T>().to_string(),
            found: str_value.to_string(),
            source: Box::new(e),
            position: None,
        })?;
        *save_result = Some(parsed_value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_value() -> Result<()> {
        let mut value: i64 = 0;
        parse_value("42", &mut value)?;
        assert_eq!(value, 42);
        let result = parse_value("5a", &mut value);
        assert!(result.is_err());

        let mut value: f64 = 0.0;
        parse_value("3.14", &mut value)?;
        assert_eq!(value, 3.14);
        let result = parse_value("3.14a", &mut value);
        assert!(result.is_err());

        let mut value: String = String::new();
        parse_value("hello", &mut value)?;
        assert_eq!(value, "hello");

        let mut value: bool = false;
        parse_value("true", &mut value)?;
        assert!(value);
        let result = parse_value("not_a_bool", &mut value);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_parse_optional() -> Result<()> {
        let mut value: Option<i64> = None;
        parse_optional("42", &mut value)?;
        assert_eq!(value, Some(42));
        let result = parse_optional("5a", &mut value);
        assert!(result.is_err());
        parse_optional("", &mut value)?;
        assert_eq!(value, None);

        let mut value: Option<f64> = None;
        parse_optional("3.14", &mut value)?;
        assert_eq!(value, Some(3.14));
        let result = parse_optional("3.14a", &mut value);
        assert!(result.is_err());
        parse_optional("", &mut value)?;
        assert_eq!(value, None);

        let mut value: Option<String> = None;
        parse_optional("hello", &mut value)?;
        assert_eq!(value, Some("hello".to_string()));
        parse_optional("", &mut value)?;
        assert_eq!(value, None);

        let mut value: Option<bool> = None;
        parse_optional("true", &mut value)?;
        assert_eq!(value, Some(true));
        let result = parse_optional("not_a_bool", &mut value);
        assert!(result.is_err());
        parse_optional("", &mut value)?;
        assert_eq!(value, None);

        Ok(())
    }
}
