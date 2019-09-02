module github.com/fraugster/parquet-go/cmd/parquet-tool

go 1.12

require (
	github.com/spf13/cobra v0.0.5
	github.com/fraugster/parquet-go v0.0.0-00010101000000-000000000000
)

replace github.com/fraugster/parquet-go => ../..
