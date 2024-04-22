# Changelog

<!--next-version-placeholder-->

## v0.41.34 (2024-04-22)

### Other

* Bump wipac-rest-tools ([#280](https://github.com/WIPACrepo/lta/issues/280)) ([`1494e0b`](https://github.com/WIPACrepo/lta/commit/1494e0bab17b4d4ddaf18363ba335951a0a1c08c))
* Try another semantic release fix ([`758c605`](https://github.com/WIPACrepo/lta/commit/758c60557080d6b77b4601e5efdc5e6c75bd771c))
* Fix up ci actions ([#278](https://github.com/WIPACrepo/lta/issues/278)) ([`7824614`](https://github.com/WIPACrepo/lta/commit/7824614eb4e85c7cf7287387398e5c17d0e5c758))
* Use github container registry ([#276](https://github.com/WIPACrepo/lta/issues/276)) ([`ff88925`](https://github.com/WIPACrepo/lta/commit/ff889252b6583fcbbec987b844e240558622d9be))
* Fix monitoring script syntax ([#277](https://github.com/WIPACrepo/lta/issues/277)) ([`187bb14`](https://github.com/WIPACrepo/lta/commit/187bb1413a7bb3dddf2da38c8a36bbfb6b48db4e))
* Update to more modern tornado asyncio wait syntax ([#275](https://github.com/WIPACrepo/lta/issues/275)) ([`42b64d4`](https://github.com/WIPACrepo/lta/commit/42b64d446dcff1011671696e010426432f1928ff))
* Paths and Pins ([#274](https://github.com/WIPACrepo/lta/issues/274)) ([`7c5cd53`](https://github.com/WIPACrepo/lta/commit/7c5cd53e065829d261622c394ed2e37739adc9d7))
* Fix tests ([#273](https://github.com/WIPACrepo/lta/issues/273)) ([`c9f9323`](https://github.com/WIPACrepo/lta/commit/c9f9323531740eab03232803818cde1ba903b943))
* Modify nersc_controller to avoid getting OOM killed ([#272](https://github.com/WIPACrepo/lta/issues/272)) ([`6fa23c5`](https://github.com/WIPACrepo/lta/commit/6fa23c5e76a5fccae9250ea979046dec022c6d8c))
* Ltacmd catalog path ([#270](https://github.com/WIPACrepo/lta/issues/270)) ([`1d39a31`](https://github.com/WIPACrepo/lta/commit/1d39a31d5d1901c51e0f7c60fed8d39c4f21ee2d))
* Update scrontab to prune slurm logs ([#269](https://github.com/WIPACrepo/lta/issues/269)) ([`c53611b`](https://github.com/WIPACrepo/lta/commit/c53611b304f99290b4cd858418e89469542f3ab3))
* Metrics Reporting with Prometheus ([#268](https://github.com/WIPACrepo/lta/issues/268)) ([`9a638a0`](https://github.com/WIPACrepo/lta/commit/9a638a034f2d353c40758e56ba44bcfaaf16bf5c))
* Save status to original_status when moving to quarantine ([#267](https://github.com/WIPACrepo/lta/issues/267)) ([`126867e`](https://github.com/WIPACrepo/lta/commit/126867e9ae8ae16163bc786229bb299ccb42cfa3))
* Progress monitor ([#257](https://github.com/WIPACrepo/lta/issues/257)) ([`52e86ff`](https://github.com/WIPACrepo/lta/commit/52e86ff176dad5511ec0ae253e9e254c1d997016))
* Bundler removes broken files of previous attempts ([#261](https://github.com/WIPACrepo/lta/issues/261)) ([`d11e1df`](https://github.com/WIPACrepo/lta/commit/d11e1df15c8e4963d006a3397da2e3ca891567fd))
* Bump py-versions CI release v2.1 ([#259](https://github.com/WIPACrepo/lta/issues/259)) ([`bf64bb1`](https://github.com/WIPACrepo/lta/commit/bf64bb182b24401e71ab56c0fd1fba5c2670ac3e))
* Request minimum and maximum time for slurm jobs at NERSC ([#258](https://github.com/WIPACrepo/lta/issues/258)) ([`3e2df4f`](https://github.com/WIPACrepo/lta/commit/3e2df4fcabb6d9226cc06860247fbf12533749ca))
* Nersc-verifier should use configured hpss_avail path ([#256](https://github.com/WIPACrepo/lta/issues/256)) ([`a2ab4cd`](https://github.com/WIPACrepo/lta/commit/a2ab4cd5e8d6950e1f88c18a875648152066d539))
* LTA should use the long-term-archive client secret ([#255](https://github.com/WIPACrepo/lta/issues/255)) ([`2888a26`](https://github.com/WIPACrepo/lta/commit/2888a26ff677f9dc74af5a6194608499dcf92c4c))
* Updated call to hpss_avail command for Perlmutter ([#254](https://github.com/WIPACrepo/lta/issues/254)) ([`be05c3d`](https://github.com/WIPACrepo/lta/commit/be05c3d78316d8fd7a0b6f289abaf2b0f8d10058))
* Added scrontab job to clean Slurm logs ([#253](https://github.com/WIPACrepo/lta/issues/253)) ([`462224c`](https://github.com/WIPACrepo/lta/commit/462224c0d557994e461f402a1c70a7def8cccf5a))
* Update LTA configuration to use Community File System instead of Perlmutter Scratch ([#252](https://github.com/WIPACrepo/lta/issues/252)) ([`c334d15`](https://github.com/WIPACrepo/lta/commit/c334d15e1ea89da98b79544cfe978b535b61feba))
* Cori Scratch is dead, long live Perlmutter Scratch ([#250](https://github.com/WIPACrepo/lta/issues/250)) ([`8f6a12b`](https://github.com/WIPACrepo/lta/commit/8f6a12b8daafe31b01f025cfb36a08b4910774fd))
* Add -debug flag to grid-proxy-init ([#249](https://github.com/WIPACrepo/lta/issues/249)) ([`5c4cf50`](https://github.com/WIPACrepo/lta/commit/5c4cf502e432808bafa3896ca587c3251bc4041b))
* Add Globus tools like globus-url-copy to Docker containers ([#248](https://github.com/WIPACrepo/lta/issues/248)) ([`006880c`](https://github.com/WIPACrepo/lta/commit/006880c6a6b1aa959656343c604ad83a958ba96c))
* Update File Catalog creds supplied to ltacmd ([#247](https://github.com/WIPACrepo/lta/issues/247)) ([`fae7161`](https://github.com/WIPACrepo/lta/commit/fae7161efc88c40110d5931c3c0b35d66367a2ce))
* Small changes that didn't make the last commit ([#246](https://github.com/WIPACrepo/lta/issues/246)) ([`048487f`](https://github.com/WIPACrepo/lta/commit/048487f8f99c041d11d2593120cdbfa6aca7c468))
* Create new controller script for NERSC ([#245](https://github.com/WIPACrepo/lta/issues/245)) ([`a030f8f`](https://github.com/WIPACrepo/lta/commit/a030f8fc47d9d9c8ce5decb7482b4462abcb9ad1))
* New Auth Deployment ([#244](https://github.com/WIPACrepo/lta/issues/244)) ([`3418b5a`](https://github.com/WIPACrepo/lta/commit/3418b5a5dbf91d46543bc6cd78b54a4dd70371f9))
* Support new authentication for File Catalog ([#243](https://github.com/WIPACrepo/lta/issues/243)) ([`406945c`](https://github.com/WIPACrepo/lta/commit/406945cf5cb0f51a37a7c9b2b880a98192b6a668))
* Modified auth configuration; fixed logging ([#242](https://github.com/WIPACrepo/lta/issues/242)) ([`ec23977`](https://github.com/WIPACrepo/lta/commit/ec23977b2e738a4f92577b1306c0a5a9337efab4))
* Update LTA authentication ([#241](https://github.com/WIPACrepo/lta/issues/241)) ([`96f7cbf`](https://github.com/WIPACrepo/lta/commit/96f7cbfd83a5207905fe337c2bbd06f5bbe5e408))
* Fix the return path ([#240](https://github.com/WIPACrepo/lta/issues/240)) ([`54fe5fa`](https://github.com/WIPACrepo/lta/commit/54fe5fadc5235cd7bcf184a8cf1a639ec3deb9d4))
* Modify parameters for small transfer requests ([#235](https://github.com/WIPACrepo/lta/issues/235)) ([`95913b2`](https://github.com/WIPACrepo/lta/commit/95913b29b1c82119693fd086df56d4d877584133))
* Jadetools from lta-vm-2 ([#234](https://github.com/WIPACrepo/lta/issues/234)) ([`6c3bc11`](https://github.com/WIPACrepo/lta/commit/6c3bc11d1a06a82772e8c7149ad8c3308f977340))
* Use better event loop management ([#233](https://github.com/WIPACrepo/lta/issues/233)) ([`bc5267b`](https://github.com/WIPACrepo/lta/commit/bc5267bba716d34a8499b9c42aab9f53371cb0df))
* Make explicit dependency on pyjwt with crypto ([`ae48e62`](https://github.com/WIPACrepo/lta/commit/ae48e6206b4a2da51167799cbd0aaabdc4e04680))

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

## [0.40.0] - 2021-02-18
### Removed
- Removed deprecated Rucio components and their dependencies

## [0.39.0] - 2021-02-18
### Changed
- RucioStager has been renamed RateLimiter

## [0.38.0] - 2021-02-18
### Fixed
- DesyVerifier component now populates the File Catalog from verified bundles
- DesyVerifier and NerscVerifier stop recording constituent files in the File Catalog record of the bundle

## [0.37.0] - 2021-02-15
### Changed
- Modified modules to use standardized configuration for interconnection

## [0.36.0] - 2021-02-11
### Fixed
- DesyMoveVerifier verifies bundles at the final destination at DESY

## [0.35.0] - 2021-02-05
### Fixed
- GridFTP Replicator now computes the correct path for USE_FULL_BUNDLE_PATH

## [0.34.0] - 2021-02-05
### Added
- Added DesyStager component to stage files for transfer to DESY

## [0.33.0] - 2021-02-02
### Changed
- Modified the way GridFTP Replicator builds paths to use GridFTP
### Fixed
- Changed the way errors in globus-url-copy are handled due to spurious returncode
- Added DEST_SITE to GridFTP Replicator so instances can discriminate by destination

## [0.32.0] - 2021-02-01
### Added
- GridFTP Replicator module for replicating files using GridFTP
- USE_FULL_BUNDLE_PATH option for GridFTP Replicator and SiteMoveVerifier

## [0.31.0] - 2020-12-28
### Changed
- LTA DB routes for GET /Bundles and GET /Bundles/{uuid} are more efficient
- Modified ltacmd script to take advantage of GET efficiencies in LTA DB

## [0.30.0] - 2020-12-17
### Changed
- Replicator can now register files at the Rucio destination with the Data Warehouse path.

## [0.29.0] - 2020-11-23
### Changed
- SiteMoveVerifier now waits for Rucio to indicate a file is complete

## [0.28.0] - 2020-11-20
### Added
- DesyVerifier verifies files were properly copied to DESY

## [0.27.0] - 2020-11-12
### Added
- DesyMoveVerifier verifies Rucio completion for bundles moved to DESY

## [0.26.0] - 2020-10-30
### Fixed
- Unpacker specifies the correct file and path to update in the File Catalog

## [0.25.0] - 2020-10-29
### Fixed
- Unpacker uses the correct field in the bundle to compute paths

## [0.24.0] - 2020-10-16
### Fixed
- NerscRetriever now uses the correct ordering for arguments to hsi get

## [0.23.0] - 2020-10-16
### Fixed
- Locator now populates bundle_path, checksum, size, and verified fields

## [0.22.0] - 2020-10-16
### Fixed
- Locator now populates claimed and files fields
- NerscRetriever now looks for bundles in status located
- ltacmd status commands now work for return requests and bundles

## [0.21.0] - 2020-10-13
### Added
- Script to drive NerscRetriever at NERSC

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
