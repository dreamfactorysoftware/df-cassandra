# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
## [0.12.2] - 2018-02-25
### Fixed
- Fix column discovery 

## [0.12.1] - 2018-01-25
### Changed
- Adhere to base database changes

## [0.12.0] - 2017-12-28
### Added
- DF-1224 Added ability to set different default limits (max_records_returned) per service
- Added package discovery
- DF-1186 Added exceptions for missing data when generating relationships
### Changed
- Separated resources from resource handlers

## [0.11.0] - 2017-11-03
### Changed
- Move preferred schema naming to service level
- Fix port casting as integer

## [0.10.0] - 2017-09-18
### Added
- DF-1060 Support for data retrieval (GET) caching and configuration
### Fixed
- Cleanup primary and unique key handling

## [0.9.1] - 2017-08-30
### Added
- Support list, set, map, and tuple data types
### Changed
- Update to the php driver 1.3 version interface

## [0.9.0] - 2017-08-17
### Changed
- Reworking API doc usage and generation
- Bug fixes for service caching
- Set config-based cache prefix

## [0.8.0] - 2017-07-27
- Cleanup service config usage

## [0.7.0] - 2017-06-05
### Changed
- Cleanup - removal of php-utils dependency

## [0.6.1] - 2017-04-25
### Fixed
- Fixed upsert response

## [0.6.0] - 2017-04-21
### Fixed
- DF-1077 Fixed support for UUID and Timestamp data types

## [0.5.1] - 2017-04-11
### Changed
- Updated to better handle native types

## [0.5.1] - 2017-04-11
### Changed
- Updated to better handle native types

## [0.5.0] - 2017-03-03
- Major restructuring to upgrade to Laravel 5.4 and be more dynamically available

## [0.4.0] - 2017-01-16
### Changed
- Adhere to refactored df-core, see df-database
- Cleanup schema management issues

## [0.3.0] - 2016-11-17
### Added
- DF-888 Adding support for offset (allows pagination)
- DB base class changes to support field configuration across all database types.

## [0.2.0] - 2016-10-03
### Changed
- DF-826 Protecting secret key using service config rework from df-core

## 0.1.0 - 2016-08-15
First official release of this library.

[Unreleased]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.12.2...HEAD
[0.12.2]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.12.1...0.12.2
[0.12.1]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.12.0...0.12.1
[0.12.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.11.0...0.12.0
[0.11.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.10.0...0.11.0
[0.10.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.9.1...0.10.0
[0.9.1]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.9.0...0.9.1
[0.9.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.8.0...0.9.0
[0.8.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.7.0...0.8.0
[0.7.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.6.1...0.7.0
[0.6.1]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.6.0...0.6.1
[0.6.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.5.1...0.6.0
[0.5.1]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.5.0...0.5.1
[0.5.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.4.0...0.5.0
[0.4.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/dreamfactorysoftware/df-cassandra/compare/0.1.0...0.2.0
