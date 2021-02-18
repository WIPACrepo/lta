# pipelines.md
A schematic of the LTA data archival pipelines

## Naming Conventions
A note about naming conventions. Components should be named after their
pipeline and type, so for example the second deleter in Pipe0 would be
named:

`COMPONENT_NAME`: `pipe0-deleter1`

While the picker for Pipe2 would be named:

`COMPONENT_NAME`: `pipe2-picker`

## TODO: More Informatation
The components specified in the pipelines below need more information.
For example, if the component makes use of a working directory, updating
the schematic to contain that information would be useful. Additionally,
where a component is running (VM, Cluster Node, Old k8s, New k8s) would
also be helpful for operators and system administrators.



## Pipe0: WIPAC -> NERSC
Transfer of bundle archives from WIPAC to NERSC
`DEST_SITE`: `NERSC`
`SOURCE_SITE`: `WIPAC`

### ltacmd
Command creates a TransferRequest in the LTA DB:

    ltacmd request new --source WIPAC --dest NERSC --path /data/exp/...

### picker
Use File Catalog to build bundle specifications from provided path
`INPUT_STATUS`: `ethereal` (doesn't matter, code doesn't use it)
`OUTPUT_STATUS`: `specified`

### bundler
Create bundle archives from bundle specifications
`INPUT_STATUS`: `specified`
`OUTPUT_STATUS`: `created`

### rucio-stager (TODO: Rename as rate-limiter)
Move bundle archives from creation directory to staging directory according to rate limit
`INPUT_STATUS`: `created`
`OUTPUT_STATUS`: `staged`

### gridftp-replicator
Use `globus-url-copy` to copy bundle archives to NERSC DTN for staging to tape
`INPUT_STATUS`: `staged`
`OUTPUT_STATUS`: `transferring`

### site-move-verifier
Verify checksum of bundle archives at NERSC DTN
`INPUT_STATUS`: `transferring`
`OUTPUT_STATUS`: `taping`

### nersc-mover
Copy the file to HPSS at NERSC
`INPUT_STATUS`: `taping`
`OUTPUT_STATUS`: `verifying`

### nersc-verifier
Verify checksum of bundle archive in HPSS, register files in the File Catalog
`INPUT_STATUS`: `verifying`
`OUTPUT_STATUS`: `completed`

### deleter (deleter0)
Delete the staging file at WIPAC
`INPUT_STATUS`: `completed`
`OUTPUT_STATUS`: `source-deleted`

### deleter (deleter1)
Delete the staging file at NERSC
`INPUT_STATUS`: `source-deleted`
`OUTPUT_STATUS`: `deleted`

### transfer-request-finisher
Update the TransferRequest for the finished bundle archive
`INPUT_STATUS`: `deleted`
`OUTPUT_STATUS`: `finished`



## Pipe1: NERSC -> WIPAC
Transfer of bundle archives from NERSC to WIPAC
`DEST_SITE`: `WIPAC`
`SOURCE_SITE`: `NERSC`

### ltacmd
Command creates a TransferRequest in the LTA DB:

    ltacmd request new --source NERSC --dest WIPAC --path /data/exp/...

### locator
Use File Catalog to identify bundle archives to be recalled
`INPUT_STATUS`: `ethereal` (doesn't matter, code doesn't use it)
`OUTPUT_STATUS`: `located`

### nersc-retriever
Copy bundle archives from HPSS to NERSC DTN for staging
`INPUT_STATUS`: `located`
`OUTPUT_STATUS`: `staged`

### gridftp-replicator
Use `globus-url-copy` to copy bundle archives to WIPAC
`INPUT_STATUS`: `staged`
`OUTPUT_STATUS`: `transferring`

### site-move-verifier
Verify checksum of bundle archives at WIPAC
`INPUT_STATUS`: `transferring`
`OUTPUT_STATUS`: `unpacking`

### unpacker
Unpack bundle archives to WIPAC Data Warehouse, register files in the File Catalog
`INPUT_STATUS`: `unpacking`
`OUTPUT_STATUS`: `completed`

### deleter (deleter0)
Delete the staging file at NERSC
`INPUT_STATUS`: `completed`
`OUTPUT_STATUS`: `source-deleted`

### deleter (deleter1)
Delete the staging file at WIPAC
`INPUT_STATUS`: `source-deleted`
`OUTPUT_STATUS`: `deleted`

### transfer-request-finisher
Update the TransferRequest for the finished bundle archive
`INPUT_STATUS`: `deleted`
`OUTPUT_STATUS`: `finished`



## Pipe2: WIPAC -> DESY
Transfer of bundle archives from WIPAC to DESY
`DEST_SITE`: `DESY`
`SOURCE_SITE`: `WIPAC`

### ltacmd
Command creates a TransferRequest in the LTA DB:

    ltacmd request new --source WIPAC --dest DESY --path /data/exp/...

### picker
Use File Catalog to build bundle specifications from provided path
`INPUT_STATUS`: `ethereal` (doesn't matter, code doesn't use it)
`OUTPUT_STATUS`: `specified`

### bundler
Create bundle archives from bundle specifications
`INPUT_STATUS`: `specified`
`OUTPUT_STATUS`: `created`

### rucio-stager (TODO: Rename as rate-limiter)
Move bundle archives from creation directory to staging directory according to rate limit
`INPUT_STATUS`: `created`
`OUTPUT_STATUS`: `staged`

### gridftp-replicator
Use `globus-url-copy` to copy bundle archives to archival destination at DESY
`INPUT_STATUS`: `staged`
`OUTPUT_STATUS`: `transferring`

### desy-move-verifier
Verify checksum of bundle archive at archival destination at DESY
`INPUT_STATUS`: `transferring`
`OUTPUT_STATUS`: `verifying`

### desy-verifier
Register files in the File Catalog for bundle archive verified at DESY
`INPUT_STATUS`: `verifying`
`OUTPUT_STATUS`: `completed`

### deleter
Delete the staging file at WIPAC
`INPUT_STATUS`: `completed`
`OUTPUT_STATUS`: `deleted`

### transfer-request-finisher
Update the TransferRequest for the finished bundle archive
`INPUT_STATUS`: `deleted`
`OUTPUT_STATUS`: `finished`



## Pipe3: DESY -> WIPAC
Transfer of bundle archives from DESY to WIPAC
`DEST_SITE`: `WIPAC`
`SOURCE_SITE`: `DESY`

### ltacmd
Command creates a TransferRequest in the LTA DB:

    ltacmd request new --source DESY --dest WIPAC --path /data/exp/...

### locator
Use File Catalog to identify bundle archives to be recalled
`INPUT_STATUS`: `ethereal` (doesn't matter, code doesn't use it)
`OUTPUT_STATUS`: `located`

### gridftp-replicator
Use `globus-url-copy` to copy bundle archives to WIPAC
`INPUT_STATUS`: `located`
`OUTPUT_STATUS`: `transferring`

### site-move-verifier
Verify checksum of bundle archives at WIPAC
`INPUT_STATUS`: `transferring`
`OUTPUT_STATUS`: `unpacking`

### unpacker
Unpack bundle archives to WIPAC Data Warehouse, register files in the File Catalog
`INPUT_STATUS`: `unpacking`
`OUTPUT_STATUS`: `completed`

### deleter
Delete the staging file at WIPAC
`INPUT_STATUS`: `completed`
`OUTPUT_STATUS`: `deleted`

### transfer-request-finisher
Update the TransferRequest for the finished bundle archive
`INPUT_STATUS`: `deleted`
`OUTPUT_STATUS`: `finished`
