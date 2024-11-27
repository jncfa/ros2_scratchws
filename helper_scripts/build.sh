#!/bin/bash
set -e

if [[ ! -v WORKSPACE_DIR ]]; then
    echo "WORKSPACE_DIR is not set"
    exit 1
fi

# Run colcon build from WORKSPACE_DIR
pushd "${WORKSPACE_DIR}" > /dev/null
trap 'popd' RETURN

# Set the default build type and pass all parameters
BUILD_TYPE=RelWithDebInfo
colcon build \
        --merge-install \
        --symlink-install \
        --cmake-args "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}" "-DCMAKE_EXPORT_COMPILE_COMMANDS=On" ${@}