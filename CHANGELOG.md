# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
(post 4.1.1 release)
### Added
### Changed
- listJobs no longer takes username as an argument, you can only list jobs for the current `MAAP_PGT` token user
- submitJob gets the username from the `MAAP_PGT` token and not username being submitted as an argument 
### Deprecated
### Removed
### Fixed
### Security

## [4.0.1]
### Added
- [issues/95](https://github.com/MAAP-Project/maap-py/issues/95): Added github action workflow for publishing and switched to poetry for builds

## [4.0.0]
### Added
- Started tracking changelog

[Unreleased]: https://github.com/MAAP-Project/maap-py/compare/v4.0.1...develop
[4.0.1]: https://github.com/MAAP-Project/maap-py/compare/v4.0.0...v4.0.1
[4.0.0]: https://github.com/MAAP-Project/maap-py/compare/1cd11b6e05781d757b8bad7e6e899855ce3e3682...v4.0.0