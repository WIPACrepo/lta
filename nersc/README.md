# README.md
This is some documentation for administration of LTA on NERSC systems.

## Background
NERSC ran a system called Cori and it was used to coordinate LTA components
for many years. NERSC has a new system called Perlmutter and this system is
used to coordinate LTA components now.

## collabssh
On Cori, there was a command `collabsu` that worked something like `su` on
regular Linux systems. By entering a password, one could switch to a
collaboration account.

This command is not available on Perlmutter, but there is a script
`sshproxy.sh` that will generate a proxy SSH key that can be used to login.

See: https://docs.nersc.gov/connect/mfa/#sshproxy

I've created a script `collabssh` that uses this and then immediately uses
ssh to login to Perlmutter using the generated key.

## Setting up LTA at NERSC
After logging into the `icecubed` account, we'll need to get a copy of the
LTA software at NERSC:

    cd ~
    git clone https://github.com/WIPACrepo/lta.git

Next we need to configure the Python virutal environment and install the
Python dependencies of LTA:

    cd ~/lta
    mkdir slurm-logs
    ${HOME}/py310/bin/python3.10 -m venv env
    source env/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt -r requirements-dev.txt

With these steps complete, the LTA software is ready to execute at NERSC.
Finally we need to give LTA the client secret, so it can authenticate with
keycloak and talk to the LTA REST service.

    echo -n "$YOUR_CLIENT_SECRET_HERE" >keycloak-client-secret
    chmod 400 keycloak-client-secret

## Using scrontab
NERSC has a crontab service integrated into their slurm queue called
[scrontab](https://docs.nersc.gov/jobs/workflow/scrontab/)

The file can be viewed with `scrontab -l` and edited with `scrontab -e`.
An example scrontab is provided in the nersc directory in this repository.
