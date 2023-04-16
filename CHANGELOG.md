# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
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