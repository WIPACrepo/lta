# transfer.md
Administrator's Guide to TransferService configuration and usage.

## TransferService
The Replicator and Site-Verifier components use a TransferService
instance in order to move files between sites. There is a function
that will instantiate and configure a TransferService object according
to a provided dictionary.

    from lta.transfer import instantiate

    xfer_service = instantiate(TRANSFER_CONFIG)

By default there is only one field in the configuration dictionary:

    TRANSFER_CONFIG = {
        "name": "name.of.my.transfer.Class"
    }

At the time of this writing, the following TransferService subclasses
exist:

    lta.transfer.copy.CopyTransferService
    lta.transfer.move.MoveTransferService
    lta.transfer.rucio.RucioTransferService

The specific configuration details of each service are described in
their own subsections of this document (below).

### Interface
The TransferService abstract class offers two methods to start and monitor
a transfer. The `start` method instructs the TransferService to begin a
transfer. The `status` method allows a module to query the TransferService
as to the state of a transfer.

#### start
The `start` method takes a transfer specification dictionary. This is the
bundle metadata as provided by the Long Term Archive database. Roughly, it
would have the following fields available:

    {
        “type”: “Bundle”,
        “uuid”: $UUID,
        “status”: “created”,
        “create_timestamp”: $ISO8601,
        “update_timestamp”: $ISO8601,
        “request”: $TransferRequest_UUID,
        “source”: “WIPAC”,
        “dest”: “NERSC”,
        “path”: “/data/exp/IceCube/2014/unbiased/PFRaw/1109”,
        “files”: [
            { $FILE_CATALOG }
        ]
        “bundle_path”: $WORK/$UUID.zip,
        “size”: $SIZE,
        “checksum”: {
            “adler32”: $ADLER32,
            “sha512”: $SHA512,
        }
        “verified”: false,
        “claimed”: true,
        “claimant”: $HOST-$MODULE,
        “claim_timestamp”: $ISO8601,
    }

The `start` method returns a transfer reference string. This string can be
used to query the transfer service about the state of the transfer in the
future.

#### status
The `status` method takes a transfer reference string (provided by the
`start` method as a return value) and returns information about the
transfer. The dictionary has the following values:

    {
        "ref": "transfer-reference-string",
        "create_timestamp": "ISO8601 date transfer was created"
        "completed": False,
        "status": "STATUS_CODE"
    }

The `status` field may contain one of the following status codes:

    COMPLETED   - Transfer has finished
    CREATED     - Transfer has been created, but not started
    ERROR       - Transfer has stopped due to an error
    PROCESSING  - Transfer has been started and is on-going
    UNKNOWN     - Provided transfer reference is unknown to the TransferService

## CopyTransferService
CopyTransferService emulates a file transfer by executing a local file
copy (`/bin/cp`) between source and destination.

### config
The configuration for CopyTransferService has three fields:

    {
        "name": "lta.transfer.copy.CopyTransferService",
        "source": "/base/source/path",
        "dest": "/base/dest/path",
    }

The `source` and `dest` fields act as roots for the file copy; they
are `os.path.join`'d to the source and dest fields of the transfer
prior to sending them to the `/bin/cp` command.

## MoveTransferService
MoveTransferService emulates a file transfer by executing a local file
move (`/bin/mv`) between source and destination.

### config
The configuration for MoveTransferService has three fields:

    {
        "name": "lta.transfer.move.MoveTransferService",
        "source": "/base/source/path",
        "dest": "/base/dest/path",
    }

The `source` and `dest` fields act as roots for the file move; they
are `os.path.join`'d to the source and dest fields of the transfer
prior to sending them to the `/bin/mv` command.

## RucioTransferService
RucioTransferService uses Rucio to copy a Data Indentifier (DID) from one
Rucio Storage Element (RSE) to another.

### config
The configuration for RucioTransferService has eight fields:

    {
        "name": "lta.transfer.rucio.RucioTransferService",
        "account": "root",
        "password": "hunter2",  # http://bash.org/?244321
        "pfn": "gsiftp://gridftp.icecube.wisc.edu:2811/mnt/lfss/rucio-test/LTA-ND-A",
        "rest_url": "http://rucio.icecube.wisc.edu:30475/",
        "rse": "LTA-ND-A",
        "scope": "lta",
        "sites": {
            "desy": "dataset-desy",
            "nersc": "dataset-nersc",
            "wipac": "dataset-wipac",
        },
        "username": "icecube",
    }

The fields have the following meaning:
* `name` - The name of the RucioTransferService class to instantiate
* `account` - The Rucio account to use for interacting with Rucio
* `password` - The password part of the credentials
* `pfn` - The Physical File Name (PFN) prefix for WIPAC
* `rest_url` - The URL through which to interact with Rucio's REST interface
* `rse` - The name of the Rucio storage element in which to register replicas
* `scope` - The scope to use when creating replicas in Rucio
* `sites` - Dictionary of the Rucio dataset associated with each site
* `username` - The username part of the credentials
