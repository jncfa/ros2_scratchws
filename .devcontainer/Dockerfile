ARG VARIANT=dev
ARG ROS_DISTRO=humble

FROM jncfa/ros2:${ROS_DISTRO}-${VARIANT}

# ** [Optional] Uncomment this section to install additional packages. **
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
   && apt-get -y install --no-install-recommends \
   pre-commit \
   clangd \
   python3-colcon-clean \
   python3-colcon-mixin \
   # pip installs
   && pip3 install --break-system-packages \
   git+https://github.com/ruffsl/colcon-cache.git \
   # Clean up
   && apt-get autoremove -y \
   && apt-get clean -y \
   && rm -rf /var/lib/apt/lists/*
ENV DEBIAN_FRONTEND=dialog

# Set up auto-source of workspace for ros user
ARG WORKSPACE
RUN echo "export WORKSPACE_DIR='${WORKSPACE}'" >> /home/ros/.bashrc

RUN echo "if [ -f ${WORKSPACE}/install/setup.bash ]; then source ${WORKSPACE}/install/setup.bash; fi" >> /home/ros/.bashrc
RUN echo "if [ -f ${WORKSPACE}/.env ]; then source ${WORKSPACE}/.env; fi" >> /home/ros/.bashrc

# add extra colcon mixins (couldnt find a way to do it on the image, so this will run only once)
RUN echo "colcon mixin add default https://raw.githubusercontent.com/colcon/colcon-mixin-repository/master/index.yaml >& /dev/null && \
   colcon mixin add my-extras https://raw.githubusercontent.com/jncfa/extra-colcon-mixins/main/index.yaml >& /dev/null && \
   colcon mixin update" >> /home/ros/.bashrc