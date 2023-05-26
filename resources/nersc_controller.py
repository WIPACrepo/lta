# nersc_controller.py
"""Implement the NERSC controller for LTA jobs."""

import asyncio
import json
import logging
from subprocess import PIPE, run
from typing import Any, cast, Dict

from wipac_dev_tools.enviro_tools import from_environment, KeySpec

Context = Dict[str, Any]
JsonObj = Dict[str, Any]

EXPECTED_CONFIG: KeySpec = {
    "JOB_TIME": "12:00:00",
    "JOB_TIME_MIN": "6:00:00",
    "LOG_LEVEL": "DEBUG",
    "LOG_PATH": "/global/homes/i/icecubed/lta/nersc_controller.log",
    "LTA_BIN_DIR": "/global/homes/i/icecubed/lta/bin",
    "SACCT_PATH": "/usr/bin/sacct",
    "SBATCH_PATH": "/usr/bin/sbatch",
    "SLURM_LOG_DIR": "/global/homes/i/icecubed/lta/slurm-logs",
    "SQUEUE_PATH": "/usr/bin/squeue",
}

# HSI_JOBS is a list of jobs that use the HPSS tape system;
# NERSC limits the number of jobs that can use HSI concurrently
HSI_JOBS = [
    "pipe0-nersc-mover",
    "pipe0-nersc-verifier"
]

# JOB_LIMITS sets a limit on the maximum number of active jobs
# of each type; two special keys also limit the number of concurrent
# jobs that access the HPSS tape system (`hsi`) and the total number
# of jobs in the slurm queue at one time (`total`)
JOB_LIMITS = {
    "hsi": 10,
    "pipe0-nersc-deleter": 2,
    "pipe0-nersc-mover": 10,
    "pipe0-nersc-verifier": 10,
    "pipe0-site-move-verifier": 10,
    "total": 15,
}

# JOB_PRIORITY indicates a priority order for job types when creating
# new jobs. Typically this is in reverse order of the pipeline, as
# later stages without work will terminate quickly, while later stages
# that have work must run in order to prevent starvation.
JOB_PRIORITY = [
    "pipe0-nersc-deleter",
    "pipe0-nersc-verifier",
    "pipe0-nersc-mover",
    "pipe0-site-move-verifier",
]

# JOB_STATES are the states of a job in the slurm queue that count as
# "active" jobs. We're not worried about cancelled, completed, etc.
# jobs, but rather those that are consuming resources or may soon do
# so in the future.
JOB_STATES = [
    "PENDING",    # PD
    "RUNNING",    # R
    "REQUIRED",   # RQ
    "RESIZING",   # RS
    "SUSPENDED",  # S
]

LOG = logging.getLogger(__name__)


class FailedCommandException(Exception):
    """Running a subprocess failed for some reason."""
    def __init__(self, command: str):
        self.command = command

    def __str__(self) -> str:
        return f"subprocess.run({self.command}) failed"


def add_job_to_slurm_queue(context: Context, name: str) -> None:
    """Add a new job to the slurm queue."""
    LOG.info(f"Scheduling job type '{name}' in the slurm queue")
    job_time = context["JOB_TIME"]
    job_time_min = context["JOB_TIME_MIN"]
    lta_bin_dir = context["LTA_BIN_DIR"]
    sbatch_path = context["SBATCH_PATH"]
    slurm_log_dir = context["SLURM_LOG_DIR"]

    # run the sacct command to determine our jobs currently running in the slurm queue
    #     sbatch_path            The path to the 'sbatch' command
    #     --account=m1093        IceCube's project (m1093) at NERSC
    #     --output=slurm.log     The log file used by the job
    #     --qos=xfer             Add the job to the xfer queue
    #     --time=HH:MM:SS        Time limit for the job
    #     --time-min=HH:MM:SS    Minimum time for the job
    #     name.sh                The script to be run in the slurm queue
    args = [sbatch_path, "--account=m1093", f"--output={slurm_log_dir}/slurm-{name}-%j.out", "--qos=xfer", f"--time={job_time}", f"--time-min={job_time_min}", f"{lta_bin_dir}/{name}.sh"]
    LOG.info(f"Running command: {args}")
    completed_process = run(args, stdout=PIPE, stderr=PIPE)

    # if our command failed
    if completed_process.returncode != 0:
        LOG.error("Command to add a job to the slurm queue failed")
        LOG.info(f"Command: {completed_process.args}")
        LOG.info(f"returncode: {completed_process.returncode}")
        LOG.info(f"stdout: {str(completed_process.stdout)}")
        LOG.info(f"stderr: {str(completed_process.stderr)}")
        raise FailedCommandException(f"{completed_process.args}")


def count_jobs_by_name(sacct: JsonObj) -> JsonObj:
    """Convert the sacct output into a count of jobs by name."""
    result: JsonObj = {
        "hsi": 0,
        "total": 0,
    }
    for job_type in JOB_PRIORITY:
        result[job_type] = 0

    # for each job, add them up by name
    jobs = sacct["jobs"]
    for job in jobs:
        # make sure it's one of our active jobs
        if job["account"] != 'm1093':
            continue
        if job["user_name"] != 'icecubed':
            continue
        if job["job_state"] not in JOB_STATES:
            continue
        # make sure it's one of the jobs that we care about
        # (i.e.: ignore 'nersc-controller' and such like)
        name = get_name(job["name"])
        if name not in JOB_PRIORITY:
            LOG.warning(f"Ignoring job {name=}")
            continue
        # since it's one of ours, add it to the totals
        result[name] = result[name] + 1
        if name in HSI_JOBS:
            result["hsi"] = result["hsi"] + 1
        result["total"] = result["total"] + 1

    # return the job count to the caller
    LOG.info(f"job_counts: {result=}")
    return result


async def do_work(context: Context) -> None:
    """Check and schedule LTA component jobs in slurm."""
    # check the current slurm queue
    sacct = get_active_jobs(context)
    # determine how many jobs of each type are running
    job_counts = count_jobs_by_name(sacct)
    job_slots = JOB_LIMITS["total"] - job_counts["total"]
    hsi_slots = JOB_LIMITS["hsi"] - job_counts["hsi"]
    # run down the priority list of job types to submit
    for job_type in JOB_PRIORITY:
        LOG.debug(f"Checking {job_type}; {job_slots=} {hsi_slots=}")
        # if we've run out of job slots, just bail out now
        if job_slots < 1:
            LOG.debug("We ran out of job slots!")
            return
        # if we've already got enough of this type, skip this type
        type_slots = JOB_LIMITS[job_type] - job_counts[job_type]
        if type_slots < 1:
            LOG.debug("Too many of this job type are already running!")
            continue
        # if we've already got enough HSI jobs, skip this type
        if job_type in HSI_JOBS:
            if hsi_slots < 1:
                LOG.debug("Too many HSI jobs are already running!")
                continue
        # having survived the gauntlet we will now launch this job
        add_job_to_slurm_queue(context, job_type)
        job_slots = job_slots - 1
        if job_type in HSI_JOBS:
            hsi_slots = hsi_slots - 1
    # log about the fact that we're done
    LOG.debug("All done checking slurm and scheduling jobs.")


# def get_active_jobs(context: Context) -> JsonObj:
#     """Check the slurm queue for currently running jobs."""
#     LOG.info("Checking slurm queue for currently running jobs")
#     sacct_path = context["SACCT_PATH"]

#     # run the sacct command to determine our jobs currently running in the slurm queue
#     #     sacct_path             The path to the 'sacct' command
#     #     --account=m1093        IceCube's project (m1093) at NERSC
#     #     --json                 Please give me the output in JSON format (easy to parse)
#     #     --state=PD,R,RQ,RS,S   Give me the jobs in the following states:
#     #                                PD = PENDING
#     #                                R  = RUNNING
#     #                                RQ = REQUEUED
#     #                                RS = RESIZING
#     #                                S  = SUSPENDED
#     args = [sacct_path, "--account=m1093", "--json", "--state=PD,R,RQ,RS,S"]
#     LOG.info(f"Running command: {args}")
#     completed_process = run(args, stdout=PIPE, stderr=PIPE)

#     # if our command failed
#     if completed_process.returncode != 0:
#         LOG.error("Command to check the slurm queue failed")
#         LOG.info(f"Command: {completed_process.args}")
#         LOG.info(f"returncode: {completed_process.returncode}")
#         LOG.info(f"stdout: {str(completed_process.stdout)}")
#         LOG.info(f"stderr: {str(completed_process.stderr)}")
#         raise FailedCommandException(f"{completed_process.args}")

#     # otherwise, we succeeded; output is on stdout
#     # {"jobs": [{ ... }, { ... }]}
#     result = completed_process.stdout.decode("utf-8")
#     sacct_output = json.loads(result)

#     return cast(JsonObj, sacct_output)


def get_active_jobs(context: Context) -> JsonObj:
    """Check the slurm queue for currently running jobs."""
    LOG.info("Checking slurm queue for currently running jobs")
    squeue_path = context["SQUEUE_PATH"]

    # run the squeue command to determine our jobs currently running in the slurm queue
    #     squeue_path            The path to the 'squeue' command
    #     --json                 Please give me the output in JSON format (easy to parse)
    args = [squeue_path, "--json"]
    LOG.info(f"Running command: {args}")
    completed_process = run(args, stdout=PIPE, stderr=PIPE)

    # if our command failed
    if completed_process.returncode != 0:
        LOG.error("Command to check the slurm queue failed")
        LOG.info(f"Command: {completed_process.args}")
        LOG.info(f"returncode: {completed_process.returncode}")
        LOG.info(f"stdout: {str(completed_process.stdout)}")
        LOG.info(f"stderr: {str(completed_process.stderr)}")
        raise FailedCommandException(f"{completed_process.args}")

    # otherwise, we succeeded; output is on stdout
    # {"jobs": [{ ... }, { ... }]}
    result = completed_process.stdout.decode("utf-8")
    sacct_output = json.loads(result)

    return cast(JsonObj, sacct_output)


def get_name(job_name: str) -> str:
    """Remove .sh ending if necessary."""
    if job_name.endswith(".sh"):
        return job_name[:-3]

    return job_name


# -----------------------------------------------------------------------------


async def main(context: Context) -> None:
    """Perform asynchronous setup tasks and start the application."""
    LOG.info("Starting asynchronous code")
    await do_work(context)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Perform synchronous setup tasks and start the application."""
    config = from_environment(EXPECTED_CONFIG)

    log_level = getattr(logging, cast(str, config["LOG_LEVEL"]).upper())
    log_path = cast(str, config["LOG_PATH"])
    logging.basicConfig(
        filename=log_path,
        style="{",
        format="{asctime} [{threadName}] {levelname:5} ({filename}:{lineno}) - {message}",
        level=log_level,
    )
    LOG.info("Starting synchronous code")

    context: Context = {
        "JOB_TIME": cast(str, config["JOB_TIME"]),
        "JOB_TIME_MIN": cast(str, config["JOB_TIME_MIN"]),
        "LOG_LEVEL": cast(str, config["LOG_LEVEL"]),
        "LOG_PATH": cast(str, config["LOG_PATH"]),
        "LTA_BIN_DIR": cast(str, config["LTA_BIN_DIR"]),
        "SACCT_PATH": cast(str, config["SACCT_PATH"]),
        "SBATCH_PATH": cast(str, config["SBATCH_PATH"]),
        "SLURM_LOG_DIR": cast(str, config["SLURM_LOG_DIR"]),
        "SQUEUE_PATH": cast(str, config["SQUEUE_PATH"]),
    }

    asyncio.run(main(context))

    LOG.info("Ending synchronous code")


if __name__ == '__main__':
    main_sync()
