#!/bin/bash
set -e

if [[ ! -v WORKSPACE_DIR ]]; then
    echo "WORKSPACE_DIR is not set"
    exit 1
fi

# Run colcon build from WORKSPACE_DIR
pushd "${WORKSPACE_DIR}" > /dev/null
trap 'popd' RETURN

sudo apt-get update
rosdep update --rosdistro="${ROS_DISTRO}"
rosdep install --from-paths src --ignore-src -y --rosdistro="${ROS_DISTRO}"
