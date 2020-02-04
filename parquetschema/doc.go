// Package parquetschema contains functions and data types to manage
// schema definitions for the parquet-go package. Most importantly,
// provides a schema definition parser to turn a textual representation
// of a parquet schema into a SchemaDefinition object.
//
// For the purpose of giving users the ability to define parquet schemas
// in other ways, this package also exposes the data types necessary for it.
// Users have the possibility to manually assemble their own SchemaDefinition
// object manually and programmatically.
package parquetschema
