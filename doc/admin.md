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

1. PATCH a status heartbeat to the LTA REST API
2. Ask the REST DB for the next TransferRequest to be picked
    1. If none, then PATCH a status heartbeat and sleep.
3. Ask the File Catalog about the files indicated by the TransferRequest
4. Update the REST DB with Files needed for bundling
5. Return the TransferRequest to the REST DB as picked
6. Repeat again starting at Step 1

#### Configuration
These configuration variables should be defined in the environment
where the picker is intended to be run. Remember that all environment
variables are strings; integers should be quoted as strings.

`FILE_CATALOG_REST_URL`: URL to the File Catalog's REST API
`LTA_REST_URL`: URL to the LTA's REST API
`PICKER_NAME`: Name of the picker instance
`SLEEP_DURATION_SECONDS`: Number of seconds to sleep between work cycles
