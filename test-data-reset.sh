#!/usr/bin/env bash
test-data-helper.sh clear-catalog
test-data-helper.sh clear-lta-transfer-requests
test-data-helper.sh add-catalog WIPAC /data/exp/IceCube/2013/filtered/PFFilt/1109
make-transfer-request.sh WIPAC:/data/exp/IceCube/2013/filtered/PFFilt/1109 NERSC:/data/exp/IceCube/2013/filtered/PFFilt/1109