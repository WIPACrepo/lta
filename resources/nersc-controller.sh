#!/usr/bin/env bash
# nersc-controller.sh

# source the Python virtual environment
cd ${HOME}/lta
source env/bin/activate

# run the controller script
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LOG_PATH=${LOG_PATH:="${HOME}/lta/nersc_controller.log"}
export LTA_BIN_DIR=${LTA_BIN_DIR:="${HOME}/lta/bin"}
export SACCT_PATH=${SACCT_PATH:="/opt/esslurm/bin/sacct"}
export SBATCH_PATH=${SACCT_PATH:="/opt/esslurm/bin/sbatch"}
export SLURM_LOG_DIR=${SLURM_LOG_DIR:="${HOME}/lta/slurm-logs"}
python3 -m resources.nersc_controller
