#!/bin/bash
# nersc_supervisor.sh
# We rely on slurm jobs to do the grunt work of archiving,
#  de-archiving, and verifying bundles.
# This checks to see how many of them are running, and
#  starts new ones if something falls short.  Mostly the
#  new jobs will find nothing to do and quit, but that
#  doesn't matter.
# The names here are open to change at the pleasure of
#  Patrick and David.
# James Bellinger	22-Jan-2020
#
# General logic and error handling tested on cobalt on 21-Jan
#
###############
# Assumptions #
###############
#  The scripts that this invokes print a START at the
# start and an END at the end.  This lets us keep
# track of what has failed.
# This is currently NOT TRUE
#
#  The sbatch options have not changed since 22-Jan-2020
#
#  The names of the scripts are as in 22-Jan-2020, with
# a XXX.sh script which defines the environment and invokes
# a XXX.py script which does the phone-home etc.
#
#  The scripts to invoke are in the directory ABOVE the
# directory containing this script.
# 
#
# Abbreviation:  NS_ = NERSC_Supervisor
#
###
# Functions follow

###
# logging
function logit {
  declare -i level
  if [[ "$2" == "" ]]; then return; fi
  level=$1
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
  logit 2 "getrunning"
  if ! rawinfo=$(SQUEUE -u icecubed -q xfer -t 12:00:00 -M escori)
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
  if [[ ! -f ${NS_SCRIPT_PATH}/$1".sh" ]]
    then
      logit 0 "Submit script $1.sh was not found"
      return 1
    fi
  logit 2 "launch $1"
  if ! SBATCH  --comments="${1}" -o "${NS_SLURM_LOG}/slurm-$1-%j.out" "${NS_SCRIPT_PATH}/$1.sh"
    then
      logit 0 "Submit of $1 failed"
      return 1
    fi
  return 0
}

###
# Inspect the slurm logs
# If completed, move to "seen"
# If not completed, count and make that available to the main
#  program for comparison w/ the number of slurm jobs.  We
#  can have a race condition in which one of the incomplete
#  slurm jobs completes by the time we ask what's active.
# I figure the process itself (and its heartbeat) will manage
#  the error reporting
function cleanupLogs {
  declare -i unfinished
  logit 2 "cleanupLogs"
  if [[ ! -d ${NS_SLURM_LOG} ]]
    then
      logit 0 "Misconfiguration with slurm log ${NS_SLURM_LOG}"
      return 1
    fi
  #
  fileList=$(/usr/bin/ls "${NS_SLURM_LOG}/"*.out)
  if [[ "${fileList}" == "" ]]; then return; fi
  unfinished=0
  for filename in ${fileList}
    do
       stuff=$(/usr/bin/grep -Ei '^START|^END' "${filename}")
       bc=$(echo "${stuff}" | /usr/bin/awk 'BEGIN{x=0;}{if($1=="START")x=x+1;if($1=="END")x=x+2;}END{print x;}')
       if [[ "${bc}" == "3" ]]
         then
           if ! /usr/bin/mv "${filename}" "${NS_SLURM_LOG_SEEN}/"
             then return $?; fi
         else
           unfinished=$(( unfinished + 1 ))
         fi
    done
  echo "${unfinished}"
  return 0
}

###
# Check the slurm job activity information, count the incomplete entries

function countIncomplete {
  #
  logit 2 'Entering countIncomplete'
  twoweeksbefore=$(/usr/bin/date +%s | /usr/bin/awk '{printf("%s\n",strftime("%Y-%m-%d",$1));}')
  listing=$(/usr/bin/sacct -n -b -S "${twoweeksbefore}" -M escori -p | /usr/bin/awk '{split($0,b,"|");split(b[1],a,".");if(b[2]!="COMPLETED")print a[1],b[2],b[3],"|";}' |/usr/bin/sort -u)
  logit 2 "${listing}"
  echo "${listing}" | /usr/bin/awk '{n=split($0,a,"|");print n-1;}'
  return 0
}



#####
# More initialization
# This is where the main work really begins.
###
#
declare -i NS_LOG_DETAIL count expectedcount fc incomplete activeJobs

export NS_DEBUG="True"

###############################
# Setup for debugging/testing #
###############################

if [[ ${NS_DEBUG} == "True" ]]
 then
   export NS_FAIL_TEST_SBATCH=0		# Vary these at will
   export NS_FAIL_TEST_SQUEUE=0
   export NS_LOG=/scratch/jbellinger/ns.log
   export NS_SLURM_LOG=/scratch/jbellinger
   export NS_SLURM_LOG_SEEN=/scratch/jbellinger/seen
   NS_SCRIPT_PATH=$(pwd)
   export NS_SCRIPT_PATH
   export NS_FAKE_QUEUE="234567 bigmem slurm-nersc-mover-234567.out icecubed PD 0:00 1 ()\n 765432 bigmem slurm-nersc-verifier-765432.out icecubed PD 0:00 1 ()"
   function SBATCH {
      fakejob=$(date +%s | awk '{print substr($1,4,6);}')
      fname=$(echo "$3" | /usr/bin/sed "s/%j/${fakejob}/")
      echo "START" >> "${fname}"
      echo "END" >> "${fname}"
      return ${NS_FAIL_TEST_SBATCH}
   }
   function SQUEUE {
      base="CLUSTER: escori\n        JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)\n"
      base=${base}"\n"${NS_FAKE_QUEUE}
      echo "${base}"
      return ${NS_FAIL_TEST_SQUEUE}
   }
 else
   export NS_LOG=/global/homes/i/icecubed/LTA/ns.log
   export NS_SLURM_LOG=/global/homes/i/icecubed/LTA/SLURMLOGS
   export NS_SLURM_LOG_SEEN=/global/homes/i/icecubed/LTA/SLURMLOGS/seen
   NS_SCRIPT_PATH=$( dirname "$(cd "$( dirname "{BASH_SOURCE[0]}" )" && pwd )" )
   export NS_SCRIPT_PATH
   function SBATCH {
     answer=$(/usr/bin/sbatch "$@")
     echo "${answer}"
   }
   function SQUEUE {
     answer=$(/usr/bin/squeue "$@")
     echo "${answer}"
   }
 fi
#############################
# Done w/ DEBUG definitions #
#############################

export NS_PHONE_HOME=False
export NS_LOG_DETAIL=2

### Main event
logit 1 "Invocation"

# If we are going to be in communication w/ Madison
# First pass is not going to be
#
if [[ ${NS_PHONE_HOME} == "True" ]]
  then
    export FILE_CATALOG_REST_TOKEN=${FILE_CATALOG_REST_TOKEN:="$(solicit-token.sh)"}
    export FILE_CATALOG_REST_URL=${FILE_CATALOG_REST_URL:="http://127.0.0.1:8889"}
    export LTA_SITE_CONFIG=${LTA_SITE_CONFIG:="etc/site.json"}
  fi
#
# Not used (yet?)
#export NS_CONTROL=/global/homes/i/icecubed/LTA/NS_CONTROL.json
#
export NS_DESIRED="nersc-mover:1 nersc-verifier:0 nersc-retriever:0"

##################
# Start the work #
##################

###
# Clean up the slurm log files and count how many didn't finish
###
#if ! incomplete=$(cleanupLogs)
if ! incomplete=$(countIncomplete)
  then
    exit 1
  fi

###
# How many slurm jobs are running right now?
###
if ! whatWeHave=$(getrunning)
  then
    exit 1
  fi

###
# Loop over the list of expected job types
# At the moment this info is hard-wired in NS_DESIRED, but
# we could load from a json initialization file instead
###
activeJobs=0
for desired in ${NS_DESIRED}
  do
    class=$(echo "${desired}" | /usr/bin/awk '{split($1,a,":");print a[1];}')
    expectedcount=$(echo "${desired}" | /usr/bin/awk '{split($1,a,":");print a[2];}')
    count=0
    for chunks in ${whatWeHave}
      do
         # ? Check that this works.  Might have to use (n-1)/2 if comment appears
         fc=$(echo "${chunks}" | awk -v var="${class}" '{n=split($0,a,var);print n-1}')
         count=$(( count + fc ))
      done
      count=$(( count / 2 ))		# Should appear in comment and name
      activeJobs=$(( activeJobs + count ))
    ###
    # Do we need to do anything?
    ###
    if [[ ${count} -lt ${expectedcount} ]]
      then
        # only launch 1 at a time
        if ! launch "${class}"
          then exit 1; fi
        logit 2 "Launching ${class} ${count} ${expectedcount}"
      fi
  done
  ###
  # Add a warning if things seem out of sync.  Likely only temporary
  ###
  if [[ ${activeJobs} -lt ${incomplete} ]]
    then
      logit 1 "More incomplete files than active jobs:  ${incomplete} ${activeJobs}"
    fi

########
# Done #
########
exit 0
