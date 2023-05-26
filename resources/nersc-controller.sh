#!/usr/bin/env bash
# nersc-controller.sh

# source the Python virtual environment
cd ${HOME}/lta
source env/bin/activate

# run the controller script
export JOB_TIME=${JOB_TIME:="12:00:00"}
export JOB_TIME_MIN=${JOB_TIME_MIN:="6:00:00"}
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LOG_PATH=${LOG_PATH:="${HOME}/lta/nersc_controller.log"}
export LTA_BIN_DIR=${LTA_BIN_DIR:="${HOME}/lta/bin"}
export SACCT_PATH=${SACCT_PATH:="/usr/bin/sacct"}
export SBATCH_PATH=${SBATCH_PATH:="/usr/bin/sbatch"}
export SLURM_LOG_DIR=${SLURM_LOG_DIR:="${HOME}/lta/slurm-logs"}
export SQUEUE_PATH=${SQUEUE_PATH:="/usr/bin/squeue"}
python3 -m resources.nersc_controller
