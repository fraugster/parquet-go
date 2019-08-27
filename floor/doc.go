/*

Package floor provides a high-level interface to read from and write to parquet files. It works
in conjunction and on top of the goparquet package.

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

By default, floor will use reflection to map your data structures to parquet schemas. Alternatively,
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
		for attrName, attrValue := range r.attrs {
			kvPair := attrMap.Add()
			kvPair.Key().SetByteArray([]byte(attrName))
			kvPair.Value().SetByteArray([]byte(attrValue))
		}
		return nil
	}

Reflection does this work automatically for you, but in turn you are hit by a slight performance penalty for using reflection,
and you lose some flexibility in how you structure your Go structs in relation to your parquet schema definition. If the object
that you want to write does not implement the floor.Marshaller interface, then (*Writer).Write will inspect it via reflection.
You can only write objects that are either a struct or a *struct. It will then iterate the struct's field, attempting to
decode each field according to its data type. Boolean types and numeric types will be mapped to their parquet equivalents.

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

*/
package floor
