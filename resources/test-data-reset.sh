#!/usr/bin/env bash
echo "Clearing files from the File Catalog"
resources/test-data-helper.sh clear-catalog
echo "Clearing Bundles from the LTA DB"
resources/test-data-helper.sh clear-lta-bundles
echo "Clearing TransferRequests from the LTA DB"
resources/test-data-helper.sh clear-lta-transfer-requests
echo "Registering test files with the File Catalog"
resources/test-data-helper.sh add-catalog WIPAC /data/exp/IceCube/2013/filtered/PFFilt/1109
echo "Creating a TransferRequest to move the test files"
resources/make-transfer-request.sh WIPAC NERSC /data/exp/IceCube/2013/filtered/PFFilt/1109
