# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- Nothing so far

## [v0.2.1] - 2020-11-04
- Release to correct missing changelog.

## [v0.2.0] - 2020-11-04
- Added `csv2parquet` command to convert CSV files into parquet files
- Fixed issue in `parquet-tool cat` that wouldn't print any records if no limit was provided
- Added support for backward compatible MAPs and LISTs in schema validation
- Added ValidateStrict method to validate without regard for backwards compatibility

## [v0.1.1] - 2020-05-26
- Added high-level interface to access file and column metadata

## [v0.1.0] - 2020-04-24
- Initial release

[Unreleased]: https://github.com/fraugster/parquet-go/compare/v0.2.1...HEAD
[v0.2.1]: https://github.com/fraugster/parquet-go/releases/tag/v0.2.1
[v0.2.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.2.0
[v0.1.1]: https://github.com/fraugster/parquet-go/releases/tag/v0.1.1
[v0.1.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.1.0
