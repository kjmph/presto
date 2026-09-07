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
ARG CUDA_VERSION
ARG UCX_VERSION

ENV PROMPT_ALWAYS_RESPOND=y
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-12/root/bin/g++
ENV ARM_BUILD_TARGET=${ARM_BUILD_TARGET}
ENV CUDA_VERSION=${CUDA_VERSION:-13.0}
ENV UCX_VERSION=${UCX_VERSION:-1.20.1}

RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
# Copy extra script called during setup.
# from https://github.com/facebookincubator/velox/pull/14016
COPY velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch /velox
COPY velox/CMake/resolve_dependency_modules/arrow/arrow-testing-boost.patch /velox
ENV VELOX_ARROW_CMAKE_PATCH="/velox/cmake-compatibility.patch /velox/arrow-testing-boost.patch"
COPY velox/CMake/resolve_dependency_modules/fbthrift/compactv1-protocol-refiller.patch /velox
ENV VELOX_FBTHRIFT_CMAKE_PATCH=/velox/compactv1-protocol-refiller.patch
# from https://github.com/facebookincubator/velox/pull/18470
COPY velox/CMake/resolve_dependency_modules/openzl/openzl-cxx-standard.patch /velox
ENV VELOX_OPENZL_CMAKE_PATCH=/velox/openzl-cxx-standard.patch
COPY CMake/arrow/arrow-flight.patch /scripts
ENV EXTRA_ARROW_PATCH=/scripts/arrow-flight.patch
RUN bash -c "mkdir build && \
    (cd build && export VELOX_BUILD_SHARED=ON && \
                 ../scripts/setup-centos.sh && \
                 ../scripts/setup-adapters.sh && \
                 source ../velox/scripts/setup-centos9.sh && \
                 source ../velox/scripts/setup-centos-adapters.sh && \
                 install_adapters && \
                 install_clang15 && \
                 install_cuda ${CUDA_VERSION}) && \
    rm -rf build"

# Build UCX after CUDA so its CUDA transports are compiled against the selected
# toolkit.  A caller may bind an exact UCX source tree into this layer; the
# default path deliberately contains no autogen.sh, so ordinary builds continue
# to use UCX_VERSION through Velox's installer.
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

# Record what the build requested as well as what UCX reports.  The latter is
# authoritative when an exact local source replaces UCX_VERSION.
RUN mkdir -p /opt/presto-ucx-build && \
    printf '%s\n' "${UCX_VERSION}" > /opt/presto-ucx-build/requested_version && \
    printf '%s\n' "${UCX_LOCAL_SOURCE_HASH}" > /opt/presto-ucx-build/local_source_hash && \
    ldconfig && \
    ucx_info -v > /opt/presto-ucx-build/ucx_info_v.txt 2>&1 && \
    ldconfig -p | grep -E 'libuc[pst]|libucs' \
      > /opt/presto-ucx-build/ldconfig_ucx.txt && \
    ucx_library="$(ucx_info -v | awk '/Library path:/{print $4; exit}')" && \
    ucx_lib_dir="$(dirname "${ucx_library}")" && \
    artifacts='libucm.so libucp.so libucs.so libuct.so' && \
    if [ "${UCX_LOCAL_SOURCE_HASH}" != none ]; then \
      artifacts="${artifacts} ucx/libuct_cuda.so ucx/libuct_ib_efa.so"; \
    fi && \
    : > /opt/presto-ucx-build/installed_artifacts.sha256 && \
    for artifact in ${artifacts}; do \
      resolved="$(readlink -f "${ucx_lib_dir}/${artifact}")" && \
      test -f "${resolved}" && \
      digest="$(sha256sum "${resolved}" | awk '{print $1}')" && \
      printf '%s  %s\n' "${digest}" "${artifact}" \
        >> /opt/presto-ucx-build/installed_artifacts.sha256; \
    done && \
    sha256sum /opt/presto-ucx-build/installed_artifacts.sha256 | \
      awk '{print $1}' \
      > /opt/presto-ucx-build/installed_artifacts_manifest_sha256

# Install sccache for optional S3-backed compile caching.
# Use NVIDIA's RAPIDS fork, not upstream mozilla/sccache: upstream has no nvcc device
# LTO support (no -ltoir/.ltoir handling as of v0.17.0 and main), so cudf's JIT LTO
# fragments -- nvcc -x cu -rdc=true -fatbin with code=[compute_XX,lto_XX] -- die with
# "fatbinary fatal : Could not open input file 'kernel.ptx'". The fork implements those
# paths and is what RAPIDS CI uses.
# See: https://github.com/rapidsai/sccache
ARG SCCACHE_VERSION="0.17.0-rapids.2"
RUN wget -q -O- "https://github.com/rapidsai/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-$(uname -m)-unknown-linux-musl.tar.gz" \
    | tar -C /usr/bin -zf - --wildcards --strip-components=1 -x '*/sccache' && \
    chmod +x /usr/bin/sccache && \
    sccache --version

# put CUDA binaries on the PATH
ENV PATH=/usr/local/cuda/bin:${PATH}

# configuration for nvidia-container-toolkit
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES="compute,utility"
