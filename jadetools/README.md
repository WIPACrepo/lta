# jadetools
These are the `jadetools` from `lta-vm-2`, located at:

    /home/jadelta/dumpcontrol/DumpStream/jadetools

These scripts coordinate most of the LTA v2 pipeline components that are
run at WIPAC.

## crontab
Command and control for the LTA components is handled via the `crontab` of
the `jadelta` user on host `lta-vm-2`. The crontab as of this writing has
been copied to the repository at:

    crontab.jadelta.lta-vm-2

This starts the bash script `runcoord` located at:

    /home/jadelta/dumpcontrol/DumpStream/jadetools/runcoord

## runcoord
The script `runcoord` is responsible for setting up the global Python 3
environment from cmvfs and LTA's virtual environment.

The script starts the Python script `coordinate2.py`:

    python coordinate2.py >& "logs/docoord${nnn}.log" &

## coordinate2.py
This script is the main command and control script. It uses a configuration
file `Interface.json` to decide what and how many LTA component processes
should be running, and which hosts can be used to run them.

Some of this script relies on a utility function `getoutputerrorsimplecommand`
which is a try/except wrapper around `subprocess.Popen` that takes a timeout
duration as an argument. This is used to normalize best-effort communication
with the LTA component hosts, which may or may not be available sometimes.

The main class of the script is `coordinate` and has a main method of `Launch`.

The constructor, `coordinate.__init__` reads the `Interface.json` configuration
file then pings all of the hosts under the `"available"` key (mapped to
`"cluster"` in the code) to see which hosts are alive for querying later. The
hosts that respond to the ping are collected in `candidatePool`.

`Launch` begins by querying each host in the `candidatePool` list using a
script `getmex` over ssh. This `getmex` script lists every process owned by
`jadelta` running on the host, then checks it against the configuration JSON.

    workerscripts/getmex

If the text of the `"key"` field for that component type appears in the process
line, it is counted in `countModule` as an instance of that component type that
is running on a host somewhere.

    config["pipes"][i]["types"][j]["key"]

If the `"hot"` field for that component type is truthy, it is also counted
in `candidatePool` as a CPU-heavy job (hot job) running on that specific host.

    config["pipes"][i]["types"][j]["hot"]

Now that the script has a count of the number of each component running, and a
count of the number of CPU intensive jobs are running on each host, it's time
to decide if any more LTA component processes need to be started somewhere.

The script iterates through all the pipes and types, and checks two fields
`"count"` and `"on"` to determine if more components of a particular type need
to be run.

    config["pipes"][i]["types"][j]["count"]
    config["pipes"][i]["types"][j]["on"]

`"on"` is a flag that controls if the component will be run at all. If so,
then `"count"` is used to determine if we have enough running. It is compared
to the type count in `countModule`, and more are run if necessary.

The `candidatePool` is used to select hosts to run components on. The key
`"hotlimit"` from the config determines how many CPU-heavy jobs (hot jobs)
are allowed to run on any given host.

Once we identify a host that isn't overloaded, we start a new instance of
the LTA component process with the script named in the `"submitter"` key:

    config["pipes"][i]["types"][j]["submitter"]

After iterating through all the hosts, pipes, and types, we've hopefully got
the correct number of LTA components running on our candidate hosts.

## Appendix

### LTA Request Lifecycle
An LTA request flows through the following process from creation to finish.
Each line indicates an LTA component and where that LTA component lives, or
is coordinated from.

    ltacmd request new          LTA REST service (LTA DB)

    picker                      k8s (runs-on)
    bundler                     lta-vm-2 (coordinated-by)
    rate-limiter                lta-vm-2 (coordinated-by)
    gridftp-replicator          lta-vm-2 (coordinated-by)
    site-move-verifier          cori.nersc.gov (runs-on)
    nersc-mover                 cori.nersc.gov (runs-on)
    nersc-move-verifier         cori.nersc.gov (runs-on)
    deleter0                    lta-vm-2 (coordinated-by)
    deleter1                    cori.nersc.gov (runs-on)
    transfer-request-finisher   k8s (runs-on)
