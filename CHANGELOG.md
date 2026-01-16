# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
(post 4.2.0 release)
### Added
- Support for new OGC endpoints
### Changed
### Deprecated
### Removed
- Removed functions calling old WPST endpoints
### Fixed
### Security

## [4.2.0]
### Added
### Changed
- listJobs no longer takes username as an argument, you can only list jobs for the current `MAAP_PGT` token user
- submit_job gets the username from the `MAAP_PGT` token and not username being submitted as an argument 
### Deprecated
### Removed
### Fixed
### Security

## [4.1.0]
### Added
- [feature/secrets](https://github.com/MAAP-Project/Community/issues/1048): Added support for user secrets management
- [feature/jobs-list-query](https://github.com/MAAP-Project/maap-api-nasa/issues/130): Added query filter parameters to listJobs endpoint
- [issues/95](https://github.com/MAAP-Project/maap-py/issues/95): Added CICD pipeline
- [feature/pagination](https://github.com/MAAP-Project/Community/issues/1027): Added pagination support for listJobs endpoint

### Changed
- [community-909](https://github.com/MAAP-Project/Community/issues/909): Removed need to track maap.cfg

## [4.0.1]
### Added
- [issues/95](https://github.com/MAAP-Project/maap-py/issues/95): Added github action workflow for publishing and switched to poetry for builds

## [4.0.0]
### Added
- Started tracking changelog

[Unreleased]: https://github.com/MAAP-Project/maap-py/compare/v4.2.0...develop
[4.2.0]: https://github.com/MAAP-Project/maap-py/compare/v4.1.0...v4.2.0
[4.1.0]: https://github.com/MAAP-Project/maap-py/compare/v4.0.1...v4.1.0
[4.0.1]: https://github.com/MAAP-Project/maap-py/compare/v4.0.0...v4.0.1
[4.0.0]: https://github.com/MAAP-Project/maap-py/compare/1cd11b6e05781d757b8bad7e6e899855ce3e3682...v4.0.0