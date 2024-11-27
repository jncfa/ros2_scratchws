#!/bin/bash
set -e

if [[ ! -v WORKSPACE_DIR ]]; then
    echo "WORKSPACE_DIR is not set"
    exit 1
fi

# Run colcon build from WORKSPACE_DIR
pushd "${WORKSPACE_DIR}" > /dev/null
trap 'popd' RETURN

if [ -f install/setup.bash ]; then source install/setup.bash; fi
colcon test --merge-install
colcon test-result --verbose
