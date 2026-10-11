// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Property `iceberg.field.id` for `Column`
pub(crate) const ICEBERG_FIELD_ID: &str = "iceberg.field.id";
/// Property `iceberg.field.optional` for `Column`
pub(crate) const ICEBERG_FIELD_OPTIONAL: &str = "iceberg.field.optional";
/// Property `iceberg.field.current` for `Column`
pub(crate) const ICEBERG_FIELD_CURRENT: &str = "iceberg.field.current";

use std::collections::{HashMap, HashSet};

use aws_sdk_glue::types::Column;
use iceberg::spec::{PrimitiveType, SchemaVisitor, TableMetadata, VariantType, visit_schema};
use iceberg::{Error, ErrorKind, Result};

use crate::error::from_aws_build_error;

type GlueSchema = Vec<Column>;

#[derive(Debug, Default)]
pub(crate) struct GlueSchemaBuilder {
    schema: GlueSchema,
    is_current: bool,
    depth: usize,
    /// Top-level names already emitted. See [`Self::field`].
    emitted_column_names: HashSet<String>,
}

impl GlueSchemaBuilder {
    /// Creates a new `GlueSchemaBuilder` from iceberg `Schema`
    pub fn from_iceberg(metadata: &TableMetadata) -> Result<GlueSchemaBuilder> {
        let current_schema = metadata.current_schema();

        let mut builder = Self {
            schema: Vec::new(),
            is_current: true,
            depth: 0,
            emitted_column_names: HashSet::new(),
        };

        visit_schema(current_schema, &mut builder)?;

        builder.is_current = false;

        for schema in metadata.schemas_iter() {
            if schema.schema_id() == current_schema.schema_id() {
                continue;
            }

            visit_schema(schema, &mut builder)?;
        }

        Ok(builder)
    }

    /// Returns the newly converted `GlueSchema`
    pub fn build(self) -> GlueSchema {
        self.schema
    }

    /// Check if is in `StructType` while traversing schema
    fn is_inside_struct(&self) -> bool {
        self.depth > 0
    }
}

impl SchemaVisitor for GlueSchemaBuilder {
    type T = String;

    fn schema(
        &mut self,
        _schema: &iceberg::spec::Schema,
        value: Self::T,
    ) -> iceberg::Result<String> {
        Ok(value)
    }

    fn before_struct_field(&mut self, _field: &iceberg::spec::NestedFieldRef) -> Result<()> {
        self.depth += 1;
        Ok(())
    }

    fn r#struct(
        &mut self,
        r#_struct: &iceberg::spec::StructType,
        results: Vec<String>,
    ) -> iceberg::Result<String> {
        Ok(format!("struct<{}>", results.join(", ")))
    }

    fn after_struct_field(&mut self, _field: &iceberg::spec::NestedFieldRef) -> Result<()> {
        self.depth -= 1;
        Ok(())
    }

    fn field(
        &mut self,
        field: &iceberg::spec::NestedFieldRef,
        value: String,
    ) -> iceberg::Result<String> {
        if self.is_inside_struct() {
            return Ok(format!("{}:{}", field.name, &value));
        }

        // A repeated name makes Athena silently skip query-result reuse for the table, and
        // `from_iceberg` walks every schema version. The current schema is visited first, so
        // keeping the first occurrence keeps a live name's current type. Matches Iceberg
        // Java's `IcebergToGlueConverter.toColumns` and pyiceberg's `_to_columns`.
        if !self.emitted_column_names.insert(field.name.clone()) {
            return Ok(value);
        }

        let parameters = HashMap::from([
            (ICEBERG_FIELD_ID.to_string(), format!("{}", field.id)),
            (
                ICEBERG_FIELD_OPTIONAL.to_string(),
                format!("{}", !field.required).to_lowercase(),
            ),
            (
                ICEBERG_FIELD_CURRENT.to_string(),
                format!("{}", self.is_current).to_lowercase(),
            ),
        ]);

        let mut builder = Column::builder()
            .name(field.name.clone())
            .r#type(&value)
            .set_parameters(Some(parameters));

        if let Some(comment) = field.doc.as_ref() {
            builder = builder.comment(comment);
        }

        let column = builder.build().map_err(from_aws_build_error)?;

        self.schema.push(column);

        Ok(value)
    }

    fn list(&mut self, _list: &iceberg::spec::ListType, value: String) -> iceberg::Result<String> {
        Ok(format!("array<{value}>"))
    }

    fn map(
        &mut self,
        _map: &iceberg::spec::MapType,
        key_value: String,
        value: String,
    ) -> iceberg::Result<String> {
        Ok(format!("map<{key_value},{value}>"))
    }

    fn primitive(&mut self, p: &iceberg::spec::PrimitiveType) -> iceberg::Result<Self::T> {
        let glue_type = match p {
            PrimitiveType::Boolean => "boolean".to_string(),
            PrimitiveType::Int => "int".to_string(),
            PrimitiveType::Long => "bigint".to_string(),
            PrimitiveType::Float => "float".to_string(),
            PrimitiveType::Double => "double".to_string(),
            PrimitiveType::Date => "date".to_string(),
            PrimitiveType::Timestamp => "timestamp".to_string(),
            PrimitiveType::TimestampNs => "timestamp_ns".to_string(),
            // Glue has no timezone-aware timestamp type; map to the equivalent non-tz type,
            // matching the behavior of Spark's Glue catalog integration.
            PrimitiveType::Timestamptz => "timestamp".to_string(),
            PrimitiveType::TimestamptzNs => "timestamp_ns".to_string(),
            PrimitiveType::Time | PrimitiveType::String | PrimitiveType::Uuid => {
                "string".to_string()
            }
            PrimitiveType::Binary | PrimitiveType::Fixed(_) => "binary".to_string(),
            PrimitiveType::Decimal { precision, scale } => {
                format!("decimal({precision},{scale})")
            }
        };

        Ok(glue_type)
    }

    fn variant(&mut self, _v: &VariantType) -> Result<Self::T> {
        Ok("variant".to_string())
    }
}

#[cfg(test)]
mod tests {
    use iceberg::TableCreation;
    use iceberg::spec::{FormatVersion, Schema, TableMetadataBuilder};

    use super::*;

    fn create_metadata(schema: Schema) -> Result<TableMetadata> {
        create_metadata_with_format_version(schema, FormatVersion::V2)
    }

    fn create_metadata_with_format_version(
        schema: Schema,
        format_version: FormatVersion,
    ) -> Result<TableMetadata> {
        let table_creation = TableCreation::builder()
            .name("my_table".to_string())
            .location("my_location".to_string())
            .schema(schema)
            .format_version(format_version)
            .build();
        let metadata = TableMetadataBuilder::from_table_creation(table_creation)?
            .build()?
            .metadata;

        Ok(metadata)
    }

    fn create_column(
        name: impl Into<String>,
        r#type: impl Into<String>,
        id: impl Into<String>,
        optional: bool,
    ) -> Result<Column> {
        create_column_with_current(name, r#type, id, optional, true)
    }

    /// A column carried only by an older schema version.
    fn create_historical_column(
        name: impl Into<String>,
        r#type: impl Into<String>,
        id: impl Into<String>,
        optional: bool,
    ) -> Result<Column> {
        create_column_with_current(name, r#type, id, optional, false)
    }

    fn create_column_with_current(
        name: impl Into<String>,
        r#type: impl Into<String>,
        id: impl Into<String>,
        optional: bool,
        is_current: bool,
    ) -> Result<Column> {
        let parameters = HashMap::from([
            (ICEBERG_FIELD_ID.to_string(), id.into()),
            (ICEBERG_FIELD_OPTIONAL.to_string(), optional.to_string()),
            (ICEBERG_FIELD_CURRENT.to_string(), is_current.to_string()),
        ]);

        Column::builder()
            .name(name)
            .r#type(r#type)
            .set_comment(None)
            .set_parameters(Some(parameters))
            .build()
            .map_err(from_aws_build_error)
    }

    #[test]
    fn test_schema_with_simple_fields() -> Result<()> {
        let record = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                {
                    "id": 1,
                    "name": "c1",
                    "required": true,
                    "type": "boolean"
                },
                {
                    "id": 2,
                    "name": "c2",
                    "required": true,
                    "type": "int"
                },
                {
                    "id": 3,
                    "name": "c3",
                    "required": true,
                    "type": "long"
                },
                {
                    "id": 4,
                    "name": "c4",
                    "required": true,
                    "type": "float"
                },
                {
                    "id": 5,
                    "name": "c5",
                    "required": true,
                    "type": "double"
                },
                {
                    "id": 6,
                    "name": "c6",
                    "required": true,
                    "type": "decimal(2,2)"
                },
                {
                    "id": 7,
                    "name": "c7",
                    "required": true,
                    "type": "date"
                },
                {
                    "id": 8,
                    "name": "c8",
                    "required": true,
                    "type": "time"
                },
                {
                    "id": 9,
                    "name": "c9",
                    "required": true,
                    "type": "timestamp"
                },
                {
                    "id": 10,
                    "name": "c10",
                    "required": true,
                    "type": "string"
                },
                {
                    "id": 11,
                    "name": "c11",
                    "required": true,
                    "type": "uuid"
                },
                {
                    "id": 12,
                    "name": "c12",
                    "required": true,
                    "type": "fixed[4]"
                },
                {
                    "id": 13,
                    "name": "c13",
                    "required": true,
                    "type": "binary"
                }
            ]
        }"#;

        let schema = serde_json::from_str::<Schema>(record)?;
        let metadata = create_metadata(schema)?;

        let result = GlueSchemaBuilder::from_iceberg(&metadata)?.build();

        let expected = vec![
            create_column("c1", "boolean", "1", false)?,
            create_column("c2", "int", "2", false)?,
            create_column("c3", "bigint", "3", false)?,
            create_column("c4", "float", "4", false)?,
            create_column("c5", "double", "5", false)?,
            create_column("c6", "decimal(2,2)", "6", false)?,
            create_column("c7", "date", "7", false)?,
            create_column("c8", "string", "8", false)?,
            create_column("c9", "timestamp", "9", false)?,
            create_column("c10", "string", "10", false)?,
            create_column("c11", "string", "11", false)?,
            create_column("c12", "binary", "12", false)?,
            create_column("c13", "binary", "13", false)?,
        ];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_schema_with_structs() -> Result<()> {
        let record = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                {
                    "id": 1,
                    "name": "person",
                    "required": true,
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "id": 2,
                                "name": "name",
                                "required": true,
                                "type": "string"
                            },
                            {
                                "id": 3,
                                "name": "age",
                                "required": false,
                                "type": "int"
                            }
                        ]
                    }
                }
            ]
        }"#;

        let schema = serde_json::from_str::<Schema>(record)?;
        let metadata = create_metadata(schema)?;

        let result = GlueSchemaBuilder::from_iceberg(&metadata)?.build();

        let expected = vec![create_column(
            "person",
            "struct<name:string, age:int>",
            "1",
            false,
        )?];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_schema_with_struct_inside_list() -> Result<()> {
        let record = r#"
        {
            "schema-id": 1,
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "location",
                    "required": true,
                    "type": {
                        "type": "list",
                        "element-id": 2,
                        "element-required": true,
                        "element": {
                            "type": "struct",
                            "fields": [
                                {
                                    "id": 3,
                                    "name": "latitude",
                                    "required": false,
                                    "type": "float"
                                },
                                {
                                    "id": 4,
                                    "name": "longitude",
                                    "required": false,
                                    "type": "float"
                                }
                            ]
                        }
                    }
                }
            ]
        }
        "#;

        let schema = serde_json::from_str::<Schema>(record)?;
        let metadata = create_metadata(schema)?;

        let result = GlueSchemaBuilder::from_iceberg(&metadata)?.build();

        let expected = vec![create_column(
            "location",
            "array<struct<latitude:float, longitude:float>>",
            "1",
            false,
        )?];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_schema_with_nested_maps() -> Result<()> {
        let record = r#"
            {
                "schema-id": 1,
                "type": "struct",
                "fields": [
                    {
                        "id": 1,
                        "name": "quux",
                        "required": true,
                        "type": {
                            "type": "map",
                            "key-id": 2,
                            "key": "string",
                            "value-id": 3,
                            "value-required": true,
                            "value": {
                                "type": "map",
                                "key-id": 4,
                                "key": "string",
                                "value-id": 5,
                                "value-required": true,
                                "value": "int"
                            }
                        }
                    }
                ]
            }
        "#;

        let schema = serde_json::from_str::<Schema>(record)?;
        let metadata = create_metadata(schema)?;

        let result = GlueSchemaBuilder::from_iceberg(&metadata)?.build();

        let expected = vec![create_column(
            "quux",
            "map<string,map<string,int>>",
            "1",
            false,
        )?];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_schema_with_optional_fields() -> Result<()> {
        let record = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                {
                    "id": 1,
                    "name": "required_field",
                    "required": true,
                    "type": "string"
                },
                {
                    "id": 2,
                    "name": "optional_field",
                    "required": false,
                    "type": "int"
                }
            ]
        }"#;

        let schema = serde_json::from_str::<Schema>(record)?;
        let metadata = create_metadata(schema)?;

        let result = GlueSchemaBuilder::from_iceberg(&metadata)?.build();

        let expected = vec![
            create_column("required_field", "string", "1", false)?,
            create_column("optional_field", "int", "2", true)?,
        ];

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_schema_with_variant() -> Result<()> {
        let record = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                {
                    "id": 1,
                    "name": "v",
                    "required": true,
                    "type": "variant"
                }
            ]
        }"#;

        let schema = serde_json::from_str::<Schema>(record)?;
        // Variant requires format version 3.
        let metadata = create_metadata_with_format_version(schema, FormatVersion::V3)?;

        let result = GlueSchemaBuilder::from_iceberg(&metadata)?.build();

        let expected = vec![create_column("v", "variant", "1", false)?];

        assert_eq!(result, expected);
        Ok(())
    }
    #[test]
    fn test_schema_history_emits_each_column_name_once() -> Result<()> {
        // The three ways a name recurs across schema versions.
        let initial = r#"{
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "string"},
                {"id": 2, "name": "hit_count", "required": false, "type": "int"},
                {"id": 3, "name": "legacy_flag", "required": false, "type": "boolean"}
            ]
        }"#;
        // int -> long promotion, a drop, and a reorder.
        let evolved = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                {"id": 2, "name": "hit_count", "required": false, "type": "long"},
                {"id": 1, "name": "id", "required": true, "type": "string"}
            ]
        }"#;

        let table_creation = TableCreation::builder()
            .name("my_table".to_string())
            .location("my_location".to_string())
            .schema(serde_json::from_str::<Schema>(initial)?)
            .format_version(FormatVersion::V2)
            .build();
        let metadata = TableMetadataBuilder::from_table_creation(table_creation)?
            .add_current_schema(serde_json::from_str::<Schema>(evolved)?)?
            .build()?
            .metadata;

        // Guards the test itself: the builder must really walk two schemas.
        assert_eq!(metadata.schemas_iter().count(), 2);

        let result = GlueSchemaBuilder::from_iceberg(&metadata)?.build();

        let expected = vec![
            // Current schema first, in its own field order, then the dropped column once.
            create_column("hit_count", "bigint", "2", true)?,
            create_column("id", "string", "1", false)?,
            create_historical_column("legacy_flag", "boolean", "3", true)?,
        ];

        assert_eq!(result, expected);

        let names: Vec<&str> = result.iter().map(|c| c.name()).collect();
        let unique: HashSet<&str> = names.iter().copied().collect();
        assert_eq!(
            names.len(),
            unique.len(),
            "Glue column names must be unique"
        );

        Ok(())
    }

    /// A table with a realistic amount of history: twelve live columns across three
    /// schema versions -- an original, a reorder, and an int -> long promotion -- plus a
    /// column dropped on the way.
    fn evolved_table_metadata() -> Result<TableMetadata> {
        const LIVE: [(&str, &str); 12] = [
            ("id", "string"),
            ("owner_id", "string"),
            ("category_id", "string"),
            ("group_id", "string"),
            ("created_at", "timestamptz"),
            ("updated_at", "timestamptz"),
            ("source", "string"),
            ("action", "string"),
            ("resource", "string"),
            ("payload", "string"),
            ("hit_count", "int"),
            ("miss_count", "long"),
        ];

        let field_json = |id: usize, name: &str, ftype: &str| {
            format!(r#"{{"id": {id}, "name": "{name}", "required": false, "type": "{ftype}"}}"#)
        };
        let schema_json = |schema_id: i32, fields: Vec<String>| {
            serde_json::from_str::<Schema>(&format!(
                r#"{{"type": "struct", "schema-id": {schema_id}, "fields": [{}]}}"#,
                fields.join(",")
            ))
        };

        // v0: original order, `hit_count` int, plus `legacy_column` (dropped later).
        let mut v0: Vec<String> = LIVE
            .iter()
            .enumerate()
            .map(|(i, (name, ftype))| field_json(i + 1, name, ftype))
            .collect();
        v0.push(field_json(99, "legacy_column", "string"));

        // v1: the two counters move to the end. `hit_count` still int.
        let reorder = |hit_type: &str| -> Vec<String> {
            let mut out: Vec<String> = LIVE
                .iter()
                .enumerate()
                .filter(|(_, (name, _))| *name != "hit_count" && *name != "miss_count")
                .map(|(i, (name, ftype))| field_json(i + 1, name, ftype))
                .collect();
            out.push(field_json(11, "hit_count", hit_type));
            out.push(field_json(12, "miss_count", "long"));
            out
        };

        Ok(TableMetadataBuilder::from_table_creation(
            TableCreation::builder()
                .name("my_table".to_string())
                .location("my_location".to_string())
                .schema(schema_json(0, v0)?)
                .format_version(FormatVersion::V2)
                .build(),
        )?
        .add_current_schema(schema_json(1, reorder("int"))?)?
        // The type promotion.
        .add_current_schema(schema_json(2, reorder("long"))?)?
        .build()?
        .metadata)
    }

    #[test]
    fn test_evolved_table_yields_one_column_per_name() -> Result<()> {
        let metadata = evolved_table_metadata()?;
        // Guards the fixture: three versions is what triggers the 3x duplication.
        assert_eq!(metadata.schemas_iter().count(), 3);

        let result = GlueSchemaBuilder::from_iceberg(&metadata)?.build();

        // 12 live + the dropped one. Before the fix this was 37 entries for 13 names.
        assert_eq!(result.len(), 13);
        let names: Vec<&str> = result.iter().map(|c| c.name()).collect();
        let unique: HashSet<&str> = names.iter().copied().collect();
        assert_eq!(names.len(), unique.len(), "repeated names: {names:?}");

        Ok(())
    }

    #[test]
    fn test_evolved_table_keeps_current_type_for_promoted_column() -> Result<()> {
        let result = GlueSchemaBuilder::from_iceberg(&evolved_table_metadata()?)?.build();

        // The int -> long promotion: the surviving entry must be the CURRENT type, not the
        // `int` that two older schemas still carry.
        let denied: Vec<&Column> = result.iter().filter(|c| c.name() == "hit_count").collect();
        assert_eq!(denied.len(), 1);
        assert_eq!(denied[0].r#type(), Some("bigint"));
        assert_eq!(
            denied[0].parameters().unwrap().get(ICEBERG_FIELD_CURRENT),
            Some(&"true".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_evolved_table_retains_dropped_column_once() -> Result<()> {
        let result = GlueSchemaBuilder::from_iceberg(&evolved_table_metadata()?)?.build();

        // Dedupe, not current-only: a dropped column stays resolvable for old data, exactly
        // once, flagged historical. This is what pyiceberg and Iceberg Java also emit.
        let legacy: Vec<&Column> = result
            .iter()
            .filter(|c| c.name() == "legacy_column")
            .collect();
        assert_eq!(legacy.len(), 1);
        assert_eq!(
            legacy[0].parameters().unwrap().get(ICEBERG_FIELD_CURRENT),
            Some(&"false".to_string())
        );

        // And it is the ONLY historical entry. Without dedupe the two older schemas
        // contribute a second copy of all 12 live names here, which is the defect.
        let historical_count = result
            .iter()
            .filter(|c| {
                c.parameters().unwrap().get(ICEBERG_FIELD_CURRENT) == Some(&"false".to_string())
            })
            .count();
        assert_eq!(historical_count, 1);

        // Every live column is flagged current.
        let current_count = result
            .iter()
            .filter(|c| {
                c.parameters().unwrap().get(ICEBERG_FIELD_CURRENT) == Some(&"true".to_string())
            })
            .count();
        assert_eq!(current_count, 12);

        Ok(())
    }
}
