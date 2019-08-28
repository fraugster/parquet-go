/*

Package floor provides a high-level interface to read from and write to parquet files. It works
in conjunction with the goparquet package.

To start writing to a parquet file, you must first create a Writer object. You can do this
using the NewWriter function and a parquet.Writer object that you've previously created using
the lower-level parquet library. Alternatively, there is also a NewFileWriter function available
that will accept a filename and an optional list of goparquet.FileWriterOption objects, and return
a floor.Writer object or an error. If you use that function, you also need to provide a
parquet schema definition, which you can create using the parquet.CreateSchemaDefinition
function.

	sd, err := goparquet.CreateSchemaDefinition(`
		message your_msg {
			required int64 id;
			optional binary data (JSON);
			optional float64 score;
			optional group attrs (MAP) {
				repeated group key_value (MAP_KEY_VALUE) {
					required binary key (STRING);
					required binary value (STRING);
				}
			}
		}
	`)
	// ...
	w, err := floor.NewFileWriter("your-file.parquet", goparquet.UseSchemaDefinition(sd))
	if err != nil {
		// ...
	}
	defer w.Close()
	// ...
	record := &yourRecord{ID: id, Data: string(data), Score: computeScore(data), Attrs: map[string]string{"foo": "bar"}}
	if err := w.Write(record); err != nil {
		// ...
	}

By default, floor will use reflection to map your data structure to a parquet schema. Alternatively,
you can choose to bypass the use of reflection by implementing the floor.Marshaller interface. This is
especially useful if the structure of your parquet schema doesn't exactly match the structure of your
Go data structure but rather requires some translating or mapping.

To implement the floor.Marshaller interface, a data type needs to implement the method MarshalParquet(MarshalObject) error.
The MarshalObject object provides methods of adding fields, set their value for a particular data type supported by
parquet, as well as structure the object using lists and maps.

	func (r *yourRecord) MarshalParquet(obj MarshalObject) error {
		obj.AddField("id").SetInt64(r.ID)
		obj.AddField("data").SetByteArray([]byte(r.Data))
		obj.AddField("score").SetFloat64(r.Score)
		attrMap := obj.AddField("attrs").Map()
		for attrName, attrValue := range r.Attrs {
			kvPair := attrMap.Add()
			kvPair.Key().SetByteArray([]byte(attrName))
			kvPair.Value().SetByteArray([]byte(attrValue))
		}
		return nil
	}

Reflection does this work automatically for you, but in turn you are hit by a slight performance penalty for using reflection,
and you lose some flexibility in how you define your Go structs in relation to your parquet schema definition. If the object
that you want to write does not implement the floor.Marshaller interface, then (*Writer).Write will inspect it via reflection.
You can only write objects that are either a struct or a *struct. It will then iterate the struct's field, attempting to
decode each field according to its data type.  Struct fields are matched up with parquet columns by converting the Go field name
to lowercase. If the struct field is equal to the parquet column name, it's a positive match. The exact mechanics of this may
change in the future.

Boolean types and numeric types will be mapped to their parquet equivalents.

In particular, Go's int, int8, int16, int32, uint, uint8, and uint16 types will be mapped to parquet's int32 type, while
Go's int64, uint32 and uint64 types will be mapped to parquet's int64 type. Go's bool will be mapped to parquet's boolean.

Go's float32 will be mapped to parquet's float, and Go's float64 will be mapped to parquet's double.

Go strings, byte slices and byte arrays will be mapped to parquet byte arrays. Byte slices and byte arrays, if specified
in the schema, are also mapped to fixed length byte arrays, with additional check to ensure that the length of the slices
resp. arrays matches up with the parquet schema definition.

Go slices of other data types will be mapped to parquet's LIST logical type. A strict adherence to a structure like this
will be enforced:

	<repetition-type> group <group-name> (LIST) {
		repeated group list {
			<repetition-type> <data-type> element;
		}
	}

Go maps will be mapped to parquet's MAP logical type. As with the LIST type, a strict adherence to a particular structure
will be enforced:

	<repetition-type> group <group-name> (MAP) {
		repeated group key_value (MAP_KEY_VALUE) {
			<repetition-type> <data-type> key;
			<repetition-type> <data-type> value;
		}
	}

Nested Go types will be mapped to parquet groups, e.g. if your Go type is a slice of a struct, it will be encoded to match
a schema definition of a LIST logical type in which the element is a group containing the fields of the struct.

Pointers are automagically taken care of when analyzing a data structure via reflection. Types such as interfaces, chans
and functions do not have suitable equivalents in parquet, and are therefore unsupported. Attempting to write data structures
that involve any of these types in any of their fields will fail.

Reading from parquet files works in a similar fashion. Package floor provides the function NewFileReader. The only parameter
it accepts is the filename of your parquet file, and it returns a floor.Reader object or an error if opening the parquet file
failed. There is no need to provide any further parameters, as other necessary information, such as the parquet schema, are
stored in the parquet file itself.

The floor.Reader object is styled after the iterator pattern that can be found in other Go packages:

	r, err := floor.NewFileReader("your-file.parquet")
	// ...
	for r.Next() {
		var record yourRecord
		if err := r.Scan(&record); err != nil {
			// ...
		}
		// ...
	}

The (*floor.Reader).Scan method supports two ways of populating your objects: by default, it uses reflection. If the provided
object implements the floor.Unmarshaller interface, it will call (floor.Unmarshaller).UnmarshalParquet on the object instead. This
approach works without any reflection and gives the implementer the greatest freedom in terms of dealing with differences
between the parquet schema definition and the Go data structures, but also puts all the burden to correctly populate data
onto the implementer.

	fun (r *yourRecord) UnmarshalParquet(record floor.UnmarshalObject) error {
		idField, err := record.GetField("id")
		if err != nil {
			return err
		}
		r.ID, err = idField.Int64()
		if err != nil {
			return err
		}

		dataField, err := record.GetField("data")
		if err != nil {
			return err
		}
		dataValue, err := dataField.ByteArray()
		if err != nil {
			return err
		}
		r.Data = string(dataValue)

		scoreField, err := record.GetField("score")
		if err != nil {
			return err
		}
		r.Score, err = scoreField.Float64()
		if err != nil {
			return err
		}

		attrsField, err := record.GetField("attrs")
		if err != nil {
			return err
		}

		attrsMap, err := attrsField.Map()
		r.Attrs = make(map[string]string)

		for attrsMap.Next() {
			keyField, err := attrsMap.Key()
			if err != nil {
				return err
			}

			valueField, err := attrsMap.Value()
			if err != nil {
				return err
			}

			key, err := keyField.ByteArray()
			if err != nil {
				return err
			}

			value, err := keyField.ByteArray()
			if err != nil {
				return err
			}

			m.Attrs[key] = value
		}

		return nil
	}

As with the Writer implementation, the Reader also supports reflection. When an object without
floor.Marshaller implementation is passed to the Scan function, it will use reflection to analyze
the structure of the object and fill all fields from the data read from the current record in
the parquet file. The mapping of parquet data types to Go data types is equivalent to the
Writer implementation.

The object you want to have populated by Scan needs to be passed as a pointer, and on the top level
needs to be a struct. Struct fields are matched up with parquet columns by converting the Go field name
to lowercase. If the struct field is equal to the parquet column name, it's a positive match. The exact
mechanics of this may change in the future.

*/
package floor
