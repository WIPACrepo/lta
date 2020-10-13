# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- for new features
### Changed
- for changes in existing functionality
### Deprecated
- for soon-to-be removed features
### Removed
- for now removed features
### Fixed
- for any bug fixes
### Security
- in case of vulnerabilities

## [0.20.0] - 2020-10-13
### Fixed
- Locator properly parses File Catalog locations when choosing bundles

## [0.19.0] - 2020-10-09
### Fixed
- NerscVerifier now uses full path when adding locations to File Catalog records

## [0.18.0] - 2020-09-01
### Fixed
- Fixed locator to properly query LTA REST server for TransferRequests

## [0.17.0] - 2020-08-25
### Changed
- Format of quarantine reason messages for Bundles

## [0.16.0] - 2020-08-18
### Added
- Added NerscRetriever component for reading bundle files from HPSS at NERSC

## [0.15.0] - 2020-08-10
### Added
- Added DEBUG logging for MongoDB queries in the REST server

## [0.14.0] - 2020-08-06
### Added
- New LTA_MAX_BODY_SIZE configuration; not yet functional in underlying library
### Changed
- Updated requirements including new rest-tools with 503 backoff

## [0.13.0] - 2020-07-30
### Changed
- Modified Picker and Locator to use File Catalog pagination for large record sets

## [0.12.0] - 2020-06-09
### Changed
- Coverage report no longer includes lta_cmd or transfer service implementations
### Removed
- Bundler no longer adds a row to the deprecated JADE LTA database

## [0.11.0] - 2020-06-01
### Added
- NerscVerifier will now PATCH a file catalog record that already exists for the bundle

## [0.10.1] - 2020-04-30
### Fixed
- TransferRequestFinisher will actually finish Bundles and TransferRequests

## [0.10.0] - 2020-04-27
### Changed
- SiteMoveVerifier will ignore Rucio and run checksums on its own in some cases

## [0.9.1] - 2020-04-24
### Fixed
- PATCH /status now adds component-specific status data properly

## [0.9.0] - 2020-04-23
### Added
- Added new /status/nersc route to LTA DB
### Fixed
- Relaxed RucioDetacher to act on a best effort basis

## [0.8.0] - 2020-04-10
### Added
- Created new Deleter module
- Created new RucioStager module
- Created new TransferRequestFinisher module
- Added scripts to bin directory to run new modules
### Changed
- Added check in ltacmd to prevent duplicate transfer requests
- Added check in ltacmd to normalize transfer request path

## [0.7.0] - 2020-04-02
### Changed
- Renamed Deleter module to RucioDetacher module
- Modified RucioDetacher to detact from both source and destination datasets
- Modified RucioDetacher to put bundles into "detached" status after processing

## [0.6.0] - 2020-03-29
### Fixed
- Deleter queries the LTA DB properly to fix TransferRequests

## [0.5.0] - 2020-03-26
### Added
- New work_priority_timestamp to LTA DB
- New commands: ltacmd {bundle,request} priority reset
### Changed
- Modified command to display priority date for bundle and request

## [0.4.0] - 2020-03-26
### Changed
- Refactored Deleter component to be make it ready for Kubernetes deployment

## [0.3.0] - 2020-03-24
### Fixed
- Fixed source location for taping in NerscMover component

## [0.2.1] - 2020-03-14
### Added
- Added better logging to RucioClient to help debug Rucio error codes
- Added --status flag to display status in ltacmd bundle ls

## [0.2.0] - 2020-03-11
### Security
- Replicator now takes RUCIO_PASSWORD from environment
### Fixed
- Replicator handling of errors while replicating Bundles

## [0.1.1] - 2020-03-09
### Fixed
- Picker handling of errors while querying the File Catalog

## [0.1.0] - 2020-02-25
### Added
- bin/site-move-verifier.sh was created to run a SiteMoveVerifier component
### Changed
- Added /dump directory to .gitignore for developer convenience
- Modified docker-deps-{down,up}.sh to handle jade_lta_test container better
- Modified ltacmd to display the reason a TransferRequest or Bundle is quarantined
- replicator.py will now quarantine bundles that fail processing for some reason
- site_move_verifier.py will now quarantine bundles that fail processing for some reason
- Modified ltacmd script to have test/production configuration lines
- Updated Python requirements to latest available versions
- resources/rucio-workbench.sh has the correct information for production Rucio
- rucio_workbench.py had some minor development changes
- resources/test-data-reset.sh was modified to use resources directory scripts
- resources/test-data-reset.sh was modified to test Rucio-talking components
### Fixed
- bin/replicator.sh was sorely outdated, and has been brought up to date
- bundler.py not to fail hard when unable to reach MySQL dependency
- picker.py now awaits the function to quarantine a bad transfer request
- replicator.py now queries the LTA DB with the correct SOURCE_SITE

## [0.0.31] - 2020-02-11
### Changed
- NerscVerifier trims down the bundle metadata added to the File Catalog

## [0.0.30] - 2020-02-11
### Changed
- Allow MongoDB to use the hash index, but don't move the filtering to Python

## [0.0.29] - 2020-02-10
### Changed
- ltacmd catalog commands use queries more friendly to MongoDB look up

## [0.0.28] - 2020-02-10
### Fixed
- NerscMover and NerscVerifier now use full path for hsi command

## [0.0.27] - 2020-01-30
### Changed
- Updated Python package requirements to latest versions

## [0.0.26] - 2020-01-29
### Changed
- Added --days to ltacmd status {component} to cull old status heartbeats

## [0.0.25] - 2020-01-27
### Fixed
- Fixed ltacmd bundle overdue not showing quarantined bundles
- Modified ltacmd bundle update-status to remove quarantine reason
- Fixed PATCH methods in Deleter, NerscVerifier, Picker, Replicator, and SiteMoveVerifier

## [0.0.24] - 2020-01-27
### Fixed
- nersc-mover.sh script now uses correct TAPE_BASE_PATH
- NerscMover component provides correct source path to HSI command
- PATCH methods in NerscMover only patch necessary fields in the bundle
- ltacmd bundle overdue no longer requires Python 3.7+

## [0.0.23] - 2020-01-23
### Added
- Added a new command: 'ltacmd bundle overdue'
- Added a new command: 'ltacmd bundle update-status'
- Added a new command: 'ltacmd request update-status'
### Fixed
- Whitelisted archival components in command: 'ltacmd status'

## [0.0.22] - 2020-01-22
### Fixed
- SiteMoveVerifier will unclaim bundles that are not yet ready for verification

## [0.0.21] - 2020-01-22
### Added
- SiteMoveVerifier now reports myquota command at NERSC as status

## [0.0.20] - 2020-01-21
### Added
- All components have new configuration RUN_ONCE_AND_DIE; default false
### Changed
- Bundler no longer has BUNDLE_ONCE_AND_DIE

## [0.0.19] - 2020-01-15
### Changed
- Picker now cooks catalog records before using them as bundle metadata

## [0.0.18] - 2020-01-14
### Added
- Added a new command: ltacmd request rm
- Added new LTA DB route: GET /status/{component-type}/count
### Changed
- Refactored some commands: ltacmd {bundle,request} {ls,status}

## [0.0.17] - 2020-01-13
### Fixed
- Fixed bug in human readable output of command ltacmd request new
- Picker will quarantine a TransferRequest if it cannot make a bundle

## [0.0.16] - 2020-01-06
### Changed
- Command: ltacmd request status now shows information about bundles

## [0.0.15] - 2020-01-03
### Added
- LTA DB logs its configuration for debugging purposes

## [0.0.14] - 2020-01-03
### Fixed
- Modified /Bundles/actions/pop to allow query by destination

## [0.0.13] - 2019-12-12
### Changed
- Modified the order of bundling steps to accommodate JADE-LTA

## [0.0.12] - 2019-12-05
### Changed
- Modified: ltacmd catalog check to be more verbose in non-JSON mode
- Modified: ltacmd catalog load to be more verbose in non-JSON mode

## [0.0.11] - 2019-12-05
### Added
- Command: ltacmd catalog display

## [0.0.10] - 2019-12-04
### Fixed
- Bug in the way ltacmd was enumerating files on disk

## [0.0.9] - 2019-12-04
### Added
- Command: ltacmd catalog check
- Command: ltacmd catalog load
- Command: ltacmd request estimate
### Removed
- Removed component daemon stuff out of ltacmd

## [0.0.8] - 2019-11-22
### Fixed
- Normalized roles for routes in the LTA REST server; added 'admin' to all
- Updated requirements to latest versions

## [0.0.7] - 2019-11-19
### Fixed
- Fixed the way authentication credentials are provided to MongoDB

## [0.0.6] - 2019-11-15
### Added
- Authentication credentials now provided to MongoDB

## [0.0.5] - 2019-11-13
### Changed
- Started tracking versions again because deployment to production

## [0.0.4] - 2019-01-03
### Added
- Status heartbeat reporting on an independent thread
- Independent sleep configuration for heartbeat and worker threads
- Configuration variable HEARTBEAT_SLEEP_DURATION_SECONDS added
- Configuration variable WORK_SLEEP_DURATION_SECONDS added
- Added application requirement requests-futures
- Added unit testing requirement pytest-asyncio
### Changed
- Use of requests changed to FuturesSession of requests-futures
- Unit tests modified for async nature of heartbeat function
- Picker documentation in doc/admin.md
### Removed
- Configuration variable SLEEP_DURATION_SECONDS removed

## [0.0.3] - 2018-12-18
### Added
- Administrator documentation in doc/admin.md
- Configuration dictionary creation in config.py
- First draft of Picker component in picker.py
- Requirements: pytest-mock and requests
### Changed
- Clean task in snake script removes another directory
### Fixed
- developers@iwe e-mail in setup.py
- lots of little flake8 issues in setup.py
- hashbang in snake script
- formatting cruft in snake script

## [0.0.2] - 2018-12-12
### Added
- Changelog for the project
- Configuration for some tools to setup.cfg
- Project helper script: snake
### Changed
- Updated documentation in README.md

## 0.0.1 - 2018-12-10
### Added
- Project setup scripts

[Unreleased]: https://github.com/WIPACrepo/lta/compare/v0.14.0...HEAD
[0.14.0]: https://github.com/WIPACrepo/lta/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/WIPACrepo/lta/compare/v0.12.0...v0.13.0
[0.10.0]: https://github.com/WIPACrepo/lta/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/WIPACrepo/lta/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/WIPACrepo/lta/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/WIPACrepo/lta/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/WIPACrepo/lta/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/WIPACrepo/lta/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/WIPACrepo/lta/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/WIPACrepo/lta/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/WIPACrepo/lta/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/WIPACrepo/lta/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/WIPACrepo/lta/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/WIPACrepo/lta/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/WIPACrepo/lta/compare/v0.0.31...v0.1.0
[0.0.31]: https://github.com/WIPACrepo/lta/compare/v0.0.30...v0.0.31
[0.0.30]: https://github.com/WIPACrepo/lta/compare/v0.0.29...v0.0.30
[0.0.29]: https://github.com/WIPACrepo/lta/compare/v0.0.28...v0.0.29
[0.0.28]: https://github.com/WIPACrepo/lta/compare/v0.0.27...v0.0.28
[0.0.27]: https://github.com/WIPACrepo/lta/compare/v0.0.26...v0.0.27
[0.0.26]: https://github.com/WIPACrepo/lta/compare/v0.0.25...v0.0.26
[0.0.25]: https://github.com/WIPACrepo/lta/compare/v0.0.24...v0.0.25
[0.0.24]: https://github.com/WIPACrepo/lta/compare/v0.0.23...v0.0.24
[0.0.23]: https://github.com/WIPACrepo/lta/compare/v0.0.22...v0.0.23
[0.0.22]: https://github.com/WIPACrepo/lta/compare/v0.0.21...v0.0.22
[0.0.21]: https://github.com/WIPACrepo/lta/compare/v0.0.20...v0.0.21
[0.0.20]: https://github.com/WIPACrepo/lta/compare/v0.0.19...v0.0.20
[0.0.19]: https://github.com/WIPACrepo/lta/compare/v0.0.18...v0.0.19
[0.0.18]: https://github.com/WIPACrepo/lta/compare/v0.0.17...v0.0.18
[0.0.17]: https://github.com/WIPACrepo/lta/compare/v0.0.16...v0.0.17
[0.0.16]: https://github.com/WIPACrepo/lta/compare/v0.0.15...v0.0.16
[0.0.15]: https://github.com/WIPACrepo/lta/compare/v0.0.14...v0.0.15
[0.0.14]: https://github.com/WIPACrepo/lta/compare/v0.0.13...v0.0.14
[0.0.13]: https://github.com/WIPACrepo/lta/compare/v0.0.12...v0.0.13
[0.0.12]: https://github.com/WIPACrepo/lta/compare/v0.0.11...v0.0.12
[0.0.11]: https://github.com/WIPACrepo/lta/compare/v0.0.10...v0.0.11
[0.0.10]: https://github.com/WIPACrepo/lta/compare/v0.0.9...v0.0.10
[0.0.9]: https://github.com/WIPACrepo/lta/compare/v0.0.8...v0.0.9
[0.0.8]: https://github.com/WIPACrepo/lta/compare/v0.0.7...v0.0.8
[0.0.7]: https://github.com/WIPACrepo/lta/compare/v0.0.6...v0.0.7
[0.0.6]: https://github.com/WIPACrepo/lta/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/WIPACrepo/lta/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/WIPACrepo/lta/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/WIPACrepo/lta/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/WIPACrepo/lta/compare/v0.0.1...v0.0.2
