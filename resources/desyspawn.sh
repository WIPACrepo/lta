#!/bin/bash
# Set for jbelling as the user.
# Assumes we have ice-wgs{1..5} as available hosts
# Won't run if the initial acrontab host isn't up.
# Sample acrontab entry for running every 15 minutes on ice-wgs1:
# 5,20,35,50 * * * * ice-wgs1 /afs/ifh.de/user/j/jbelling/LTA/lta/resources/desyspawn.sh
datestring=$(/usr/bin/date +%s)
/afs/ifh.de/user/j/jbelling/LTA/lta/bin/pipe2-desy-site-move-verifier.sh >& /afs/ifh.de/user/j/jbelling/LTA/lta/log/pipe2-desy-site-move-verifier-"${datestring}".log &
