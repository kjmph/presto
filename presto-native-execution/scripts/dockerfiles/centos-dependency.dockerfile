# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM quay.io/centos/centos:stream9

# Set this when build arm with common flags
# from https://github.com/facebookincubator/velox/pull/14366
ARG ARM_BUILD_TARGET

# This defaults to 12.9 but can be overridden with a build arg
ARG CUDA_VERSION

ENV PROMPT_ALWAYS_RESPOND=y
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-12/root/bin/g++
ENV ARM_BUILD_TARGET=${ARM_BUILD_TARGET}
ENV CUDA_VERSION=${CUDA_VERSION:-12.9}
ENV UCX_VERSION="1.19.0"

RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
# Copy extra script called during setup.
# from https://github.com/facebookincubator/velox/pull/14016
COPY velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch /velox
COPY CMake/arrow/arrow-flight.patch /scripts
ENV VELOX_ARROW_CMAKE_PATCH=/velox/cmake-compatibility.patch
ENV EXTRA_ARROW_PATCH=/scripts/arrow-flight.patch
RUN bash -c "mkdir build && \
    (cd build && ../scripts/setup-centos.sh && \
                 ../scripts/setup-adapters.sh && \
                 source ../velox/scripts/setup-centos9.sh && \
                 source ../velox/scripts/setup-centos-adapters.sh && \
                 install_adapters && \
                 install_clang15 && \
                 install_cuda ${CUDA_VERSION}) && \
    rm -rf build"

ARG UCX_LOCAL_SOURCE=scripts
ARG UCX_LOCAL_SOURCE_HASH=none
RUN --mount=type=bind,source=${UCX_LOCAL_SOURCE},target=/local_ucx_source,ro \
    bash -c "mkdir build && \
    echo UCX_LOCAL_SOURCE_HASH=${UCX_LOCAL_SOURCE_HASH} && \
    (cd build && source ../velox/scripts/setup-centos9.sh && \
                 export UCX_LOCAL_SOURCE=/local_ucx_source && \
                 source ../velox/scripts/setup-centos-adapters.sh && \
                 install_ucx) && \
    rm -rf build"

RUN mkdir -p /opt/presto-ucx-build && \
    printf '%s\n' "${UCX_VERSION}" > /opt/presto-ucx-build/requested_version && \
    printf '%s\n' "${UCX_LOCAL_SOURCE_HASH}" > /opt/presto-ucx-build/local_source_hash && \
    (ucx_info -v 2>&1 || true) > /opt/presto-ucx-build/ucx_info_v.txt && \
    (ldconfig && ldconfig -p | grep -E 'libuc[pst]|libucs' 2>&1 || true) > /opt/presto-ucx-build/ldconfig_ucx.txt

# put CUDA binaries on the PATH
ENV PATH=/usr/local/cuda/bin:${PATH}

# configuration for nvidia-container-toolkit
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES="compute,utility"
