# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Changed extra's name back to `aio`

## [6.0.0] - 2025-11-26

### Added
- Support Delos and Gov SG clouds

### Changed

- \[BREAKING\] Minimum supported Python version is now 3.9, to align with other Azure SDKs.
- The project's infrastructure has been updated:
  - `uv` is used to manage packages
  - `ruff` is used for linting and formatting
  - `basedpyright` is used for type checking
  - `poe` is used to manage tasks
  - `pdoc` is used to generate documentation

## [5.0.5] - 2025-07-20

### Added
- More endpoints supported by default.

## [5.0.4] - 2025-06-24

### Changed

- CloudSettings will now reuse the HTTP session from the query client for sync clients.

### Fixed

- CloudSettings now cached by authority (schema, host and port) instead of full URL

## [5.0.3] - 2025-05-04

### Fixed

- Fixed Ingest from dataframe
- Dependency issues
- Typing issues



## [5.0.2] - 2025-05-30

### Fixed

- Fixed bug in `ingest_from_dataframe` with some ingestion properties combinations.

## [5.0.1] - 2025-03-12

### Fixed

- Dependency issues

## [5.0.0] - 2025-03-10

### Changed
- [BREAKING] Dropped support for python version 3.7, as it is on EOL for over year.
- [BREAKING] Aligned the Connection String Builder keywords with the rest of the SDKs.
This means that some keywords were removed, and they will no longer be parsed as part of the Connection String.  
Building the Connection String using the builder method will still work as expected.  
The following keywords have been removed:
    - `msi_auth` / `msi_authentication`
    - `msi_params` / `msi_type`
    - `interactive_login`
    - `az_cli`
- [BREAKING] `ingest_from_dataframe` - Added `data_format` parameter. It can be None (default), 'json' or 'csv'.
  Based on how panda's csv serialization works, dynamic data will not be serialized correctly.
  By default, the data will be serialized as json to avoid this issue.
  However, that may break if a CSV mapping is used.
  Therefore, when passing the None value, the data will be json by default, or csv if a csv mapping is used.
  Also, it is possible to pass 'csv' or 'json' to force the serialization type.

## [4.6.3] - 2025-01-08

### Fixed
- Explicitly export members in `__init__.py` via `__all__`

## [4.6.2] - 2025-01-07

### Fixed
- Added `py.typed` markers
- Fixed semantic error handling

## [4.6.1] - 2024-09-30

### Added
- Support more kusto endpoints by default

### Fixed

- Better python errors when getting 401 and other http errors

## [4.5.1] - 2024-06-19

### Fixed

- Fixed bug in `dataframe_from_result_table` with some command results.

## [4.5.0] - 2024-06-18

### Added

- `dataframe_from_result_table` now accepts optional converters for specific columns or types.

### Fixed

- Compatibility with numpy 2.0

## [4.4.1] - 2024-05-06

### Fixed

- Fixed bug in ManagedIdentity close method
- Fixed bug in IngestionResult repr

## [4.4.0] - 2024-04-08

### Added

- Streaming ingestion from blob

### Fixed

- Managed streaming fallback to queued
- Fixed token providers not being closed properly
- Internal test fixes
- Pandas support the new string type, if available
- Removed test folders from wheel

### Changed

- Changed binary files data format compression to false

## [4.3.1] - 2023-12-18

### Fixed

- Pandas now correctly parses all dates

## [4.3.0] - 2023-12-12

### Added

- Added smart retry ability for queued ingestion.
- Support new playfab domain
- Added no-authenticaion option

### Fixed

- Santaize secrets from blob urls
- Correctly handle closing of token providers in async (NOTE: now using async providers in sync clients will correctly
  raise an exception)
- Fixed proxy not passing correctly in some cases
- Fixed exception handling in web requests
- Internal fixes for environment variables
- Fixed documentation on E2E tests, and made it possible to test on a clean cluster

## [4.2.0] - 2023-05-18

### Added

- Added Initial Catalog (Default Database) parameter to ConnectionStringBuilder
- Added callback parameter to device code
- Added method to manually set the cache for CloudSettings

### Changed

- Urls with one item after the path (i.e https://test.com/abc) will now be treated as cluster and initial catalog (ie.
  the cluster is "https://test.com" and the initial catalog is "abc").
    - This is to align our behaviour with the .NET SDK

### Fixed

- Some edge cases in url parsing
- IgnoreFirstRecord now works properly
- Internal code improvement for telemetry

## [4.1.4] - 2023-04-16

### Fixed

- Unicode headers are now espaced using '?', to align with the service

## [4.1.3] - 2023-03-26

### Added

- Add new trident endpoint support

### Security

- Redirects are no longer allowed

## [4.1.2] - 2023-02-22

### Fixed

- Fixed resource parsing to be inline with other SDKs
- More resources are properly closed
- Internal testing fixes

## [4.1.1] - 2023-02-12

### Fixed

- Fixed context not awaited in traces, caused warning
- Fixed setup.py file, #453

## [4.1.0] - 2023-02-09

### Added

- Tracing support for the SDK
- Use opentelemetry to trace the flow of operations when querying or ingesting
- Add support for a TokenProvider to authenticate access.
- KustoConnectionStringBuilder.with_token_provider() /KustoConnectionStringBuilder.with_async_token_provider()

### Fixed

- Use proper np types instead of strings, fixes #447
- samples file was cut short
- Send http headers under the new unified format

### Changed

- Adding Microsoft SECURITY.MD

## [4.0.2] - 2022-12-15

### Added

- Add trident endpoint support

### Fixed

- Added more serialization options for "msi authentication" and "msi params" correctly
- Make error parsing less restrictive to support more errors

## [4.0.1] - 2022-11-30

### Added

- Added flag to handle nullable bools in pandas, fixes #436.

### Fixed

- Fixed source release not including a file.

## [4.0.0] - 2022-11-23

### Added

- [BREAKING] Trusted endpoints validation - by default, kusto will only connect to known endpoints.
- Added closability to the clients
- Make numpy dependency optional by @spektom
- Sample app improvements by @ronmonetaMicro
- Added Ignorefirstrecord ingestion property
- Fixed AttributeError when unpickling HTTPAdapterWithSocketOptions.
- Enum serialization fix
- Internal and testing improvements by @enmoed

### Changed

- Use "organizations" as the default tenant to support more cases

## [3.1.3] - 2022-06-08

### Added

- Allow passing service url with port
- Expanded mappings to support all options for all data formats

### Fixed

- Use "organizations" as the default tenant to support more cases

### Changed

- Docs improvements by @nerd2ninja in #393
