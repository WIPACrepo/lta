#!/bin/bash
cd /global/homes/i/icecubed/NEWLTA/lta/bin
#echo "START site-move-verifier.sh $(date)"
./pipe0-site-move-verifier.sh
#echo "START nersc-mover.sh $(date)"
./pipe0-nersc-mover.sh
#echo "START nersc-verifier.sh $(date)"
./pipe0-nersc-verifier.sh
