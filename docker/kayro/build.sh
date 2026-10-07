#!/usr/bin/env bash
set -euo pipefail
# Run on an isolated ARM64 Linux builder, never on the operator laptop.
root=$(git rev-parse --show-toplevel)
cd "$root"
test "$(uname -m)" = aarch64
image=${1:?Usage: docker/kayro/build.sh IMAGE}
test -z "$(git status --porcelain --untracked-files=no)"
git submodule update --init --recursive --depth 1
mkdir -p build-release
CC=clang-21 CXX=clang++-21 cmake -S . -B build-release -G Ninja \
  -DCOMPILER_CACHE=disabled -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_THINLTO=ON \
  -DENABLE_TESTS=OFF -DENABLE_RUST=ON -DENABLE_UTILS=OFF -DENABLE_EXAMPLES=OFF \
  -DCMAKE_TOOLCHAIN_FILE="$root/cmake/linux/toolchain-aarch64.cmake" \
  -DBUILD_STRIPPED_BINARY=ON -DENABLE_CLICKHOUSE_SELF_EXTRACTING=ON \
  -DSPLIT_DEBUG_SYMBOLS=ON -DCMAKE_INSTALL_PREFIX=/usr \
  > build-release/configure.log 2>&1
cmake --build build-release --target clickhouse > build-release/compile.log 2>&1
mkdir -p build-release/kayro-image
cp build-release/programs/stripped/bin/clickhouse build-release/kayro-image/clickhouse
revision=$(git rev-parse HEAD)
binary_sha=$(sha256sum build-release/kayro-image/clickhouse | cut -d ' ' -f 1)
docker build -f docker/kayro/Dockerfile --build-arg SOURCE_COMMIT="$revision" \
  --build-arg BINARY_SHA256="$binary_sha" -t "$image" build-release/kayro-image
