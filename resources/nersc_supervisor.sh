#!/bin/bash
# nersc_supervisor.sh
# Cron job.
# Argument:  optional integer between 0 and 2 that controls the log level.
#  Default is 0

# We rely on slurm jobs to do the grunt work of archiving,
#  de-archiving, and verifying bundles.
# This checks to see how many of them are running, and
#  starts new ones if something falls short.  Mostly the
#  new jobs will find nothing to do and quit, but that
#  doesn't matter.
#
# We have 2 distinct ways of determining whether there are slurm jobs running.
# Ask sacct about the job history, and figure out how many jobs have not completed.
#  This may be for a variety of reasons, listed below
# Ask squeue for which jobs are currently running.  If jobs have failed they
#  won't appear here, but will appear in the previous list.
#
# The second method is definitive for determining whether or not to
#  submit a new job.  However, the sacct information tells me if something
#  needs to be looked at.
#
# The sacct info could be accumulated with a different script, if desired.
#
# The names here are open to change at the pleasure of
#  Patrick and David.
# James Bellinger	22-Jan-2020
#
###############
# Assumptions #
###############
#
#  The slurm command options have not changed since 22-Jan-2020
#
#  The names of the scripts are as in 22-Jan-2020, with
# a XXX.sh script which defines the environment and invokes
# a XXX.py script which does the phone-home etc.
#
#  The scripts to invoke are in the directory ABOVE the
# directory containing this script.
#
#################
# Error returns # 
#################
# 0 	OK
# 1	Failed to "sacct" -- slurm job history
# 2	Failed to "squeue" -- get slurm activity
# 3	Failed to "sbatch" -- submit job
#
# sacct job state codes as of 24-Jan-2020:
#       BF  BOOT_FAIL       Job  terminated due to launch failure, typically due to a hardware failure (e.g. unable to boot the node or block and the
#                           job can not be requeued).
#       CA  CANCELLED       Job was explicitly cancelled by the user or system administrator.  The job may or may not have been initiated.
#       CD  COMPLETED       Job has terminated all processes on all nodes with an exit code of zero.
#       DL  DEADLINE        Job terminated on deadline.
#       F   FAILED          Job terminated with non-zero exit code or other failure condition.
#       NF  NODE_FAIL       Job terminated due to failure of one or more allocated nodes.
#       OOM OUT_OF_MEMORY   Job experienced out of memory error.
#       PD  PENDING         Job is awaiting resource allocation.
#       PR  PREEMPTED       Job terminated due to preemption.
#       R   RUNNING         Job currently has an allocation.
#       RQ  REQUEUED        Job was requeued.
#       RS  RESIZING        Job is about to change size.
#       RV  REVOKED         Sibling was removed from cluster due to other cluster starting the job.
#       S   SUSPENDED       Job has an allocation, but execution has been suspended and CPUs have been released for other jobs.
#       TO  TIMEOUT         Job terminated upon reaching its time limit.

#
# Abbreviation:  NS_ = NERSC_Supervisor
#

# Definitions

declare -i NS_LOG_DETAIL count expectedcount fc activeJobs
#declare -i incomplete

export NS_BASE=/global/homes/i/icecubed/NEWLTA/lta
export NS_LOG="${NS_BASE}/ns.log"
export NS_SLURM_LOG="${NS_BASE}/SLURMLOGS"
export NS_SLURM_LOG_SEEN="${NS_SLURM_LOG}/seen"
NS_SCRIPT_PATH="${NS_BASE}/bin"
export NS_SCRIPT_PATH
export SBATCH=/usr/bin/sbatch
export SQUEUE=/usr/bin/squeue
export SACCT=/usr/bin/sacct
#export NS_CONTROL="${NS_BASE}/NS_CONTROL.json"
export NS_PHONE_HOME=False
##########
# CONTROL
# This string controls what we want to see at all times. 
#   0 verifiers, 0 retrievers, 3 mover
export NS_DESIRED="nersc-mover:3 nersc-verifier:1 nersc-retriever:0"

###
# Functions follow

###
# logging
function logit {
  # invocation:  logit # "message"
  # # is the logging level; higher means less important
  # message is quoted
  declare -i level
  if [[ "$2" == "" ]]; then return; fi
  level=$1
  # sanity check
  if [[ ${NS_LOG_DETAIL} -eq 0 ]]; then return; fi
 
  if [[ ${NS_LOG_DETAIL} -ge ${level} ]]
    then
      echo "$(date) $2" >> "${NS_LOG}"
    fi
  return 0
 }

###
# Return info about slurm jobs
function getrunning {
  # no arguments
  logit 2 "getrunning"
  if ! rawinfo=$(${SQUEUE} -h -o "%.18i %.25j %.2t %.10M %.42k %R" -u icecubed -q xfer -M escori)
    then
      logit 0 "SLURM is not working"
      return 1
    fi
  echo "${rawinfo}"
  return 0
}

###
# Launch a process of the appropriate type
function launch {
  # argument:  class of job to submit:  e.g. nersc_mover
  # I expect a shell script which in turn invokes a python script
  #  but that can be changed at will
  if [[ ! -f ${NS_SCRIPT_PATH}/$1".sh" ]]
    then
      logit 0 "Submit script $1.sh was not found"
      return 1
    fi
  logit 2 "launch $1"
  if ! ${SBATCH}  -o "${NS_SLURM_LOG}/slurm-$1-%j.out" -q xfer -M escori -t 12:00:00 "${NS_SCRIPT_PATH}/$1.sh"
    then
      logit 0 "Submit of $1 failed"
      return 1
    fi
  return 0
}

###
# Check the slurm job activity information, count the incomplete entries

function countIncomplete {
  # No arguments
  #
  logit 2 'Entering countIncomplete'
  twoweeksbefore=$(/usr/bin/date +%s | /usr/bin/awk '{printf("%s\n",strftime("%Y-%m-%d",$1));}')
  # NOTA BENE:  The same job number can appear several times in the output.
  # Therefore I have to sort -u the result.
  # bash will treat the contents of "listing" as a single line, of course.
  listing=$(${SACCT} -n -b -S "${twoweeksbefore}" -M escori -p | /usr/bin/awk '{split($0,b,"|");split(b[1],a,".");if(b[2]!="COMPLETED")print a[1],b[2],b[3],"|";}' |/usr/bin/sort -u)
  if [[ "${listing}" == "" ]]
    then
        echo 0
        return 0
    fi
  # Steve doesn't think I need to log the incomplete info
  #logit 2 "${listing}"
  # Count the number of not-completed, skipping the terminal "|" if any
  echo "${listing}" | /usr/bin/awk '{n=split($0,a,"|");m=n-1;if(m<0)m=0;print m;}'
  return 0
}

###################################################################
# This is where the main work really begins.
#############################################
#

# First find and set the logging level

if [[ "$1" == "" ]]
  then
    NS_LOG_DETAIL=0
  else
    NS_LOG_DETAIL=$1
  fi
# Log (or not, depending on the level)
logit 1 "Invocation"

# Not used yet




##################
# Start the work #
##################

#if ! incomplete=$(countIncomplete)
#  then
#    exit 1
#  fi

###
# How many slurm jobs are running right now?
###
if ! whatWeHave=$(getrunning)
  then
    exit 2
  fi

###
# Loop over the list of expected job types
# At the moment this info is hard-wired in NS_DESIRED, but
# we should load from a json initialization file instead
# Keeping track of activeJobs is future-proofing the system
# I should count them all and compare with maxjobs before
# submitting anything.
###
activeJobs=0
for desired in ${NS_DESIRED}
  do
    class=$(echo "${desired}" | /usr/bin/awk '{split($1,a,":");print a[1];}')
    expectedcount=$(echo "${desired}" | /usr/bin/awk '{split($1,a,":");print a[2];}')
    count=0
    for chunks in ${whatWeHave}
      do
         fc=$(echo "${chunks}" | awk -v var="${class}.sh" '{n=split($0,a,var);print n-1}')
         count=$(( count + fc ))
      done
      activeJobs=$(( activeJobs + count ))
    ###
    # Do we need to do anything?
    ###
    if [[ ${count} -lt ${expectedcount} ]]
      then
        # only launch 1 at a time
        if ! launch "${class}"
          then exit 3; fi
        logit 2 "Launching ${class} ${count} ${expectedcount}"
      fi
  done

########
# Done #
########
exit 0
