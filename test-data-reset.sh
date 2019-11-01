#!/usr/bin/env bash
echo "Clearing files from the File Catalog"
test-data-helper.sh clear-catalog
echo "Clearing Bundles from the LTA DB"
test-data-helper.sh clear-lta-bundles
echo "Clearing TransferRequests from the LTA DB"
test-data-helper.sh clear-lta-transfer-requests
echo "Registering test files with the File Catalog"
test-data-helper.sh add-catalog WIPAC /data/exp/IceCube/2013/filtered/PFFilt/1109
echo "Creating a TransferRequest to move the test files"
make-transfer-request.sh WIPAC WIPAC-B /data/exp/IceCube/2013/filtered/PFFilt/1109
