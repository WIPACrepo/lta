# admin.md
Administrator's Guide to Long Term Archive (LTA)

## Audience
If you are involved in setting up, configuring, maintaining,
or troubleshooting a Long Term Archive system then this document
is for you.

Here you can find information like what environment variables
provide configuration to Long Term Archive components.

## Components

### picker
The `picker` component selects files that are required for bundling.

The work cycle of the `picker` is as follows:

1. Ask the LTA DB for the next TransferRequest to be picked
2. Ask the File Catalog about the files indicated by the TransferRequest
3. Update the LTA DB with Files needed for bundling
4. Return the TransferRequest to the LTA DB as picked
5. Repeat again starting at Step 1

#### Configuration
These configuration variables should be defined in the environment
where the picker is intended to be run. Remember that all environment
variables are strings; integers should be quoted as strings.

- `FILE_CATALOG_REST_URL`: URL to the File Catalog's REST API
- `HEARTBEAT_SLEEP_DURATION_SECONDS`: Number of seconds to sleep between heartbeats
- `LTA_REST_URL`: URL to the LTA's REST API
- `PICKER_NAME`: Name of the picker instance
- `WORK_SLEEP_DURATION_SECONDS`: Number of seconds to sleep between work cycles

### LTA DB

#### Configuration
These configuration variables should be defined in the environment
where the LTA DB is intended to be run. Remember that all environment
variables are strings; integers should be quoted as strings.

`LTA_SITE_CONFIG`: Path to a JSON file specifying site configuration.

##### LTA_SITE_CONFIG Schema
The JSON provided as site configuration is an object with a single top
level field `sites`, an object with site names as keys:

    {
        "sites": {
            "DESY": ...
            "NERSC": ...
            "WIPAC": ...
        }
    }

Each site name has a site object as a value.

- `bundle_size` is the preferred bundle size of the site.

An example site object:

    {
        "bundle_size": 1000000000
    }
