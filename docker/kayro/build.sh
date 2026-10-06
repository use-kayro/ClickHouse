#!/usr/bin/env bash
set -euo pipefail
# Run on an isolated ARM64 Linux builder, never on the operator laptop.
root=$(git rev-parse --show-toplevel)
cd "$root"
test "$(uname -m)" = aarch64
image=${1:?Usage: docker/kayro/build.sh IMAGE}
test -z "$(git status --porcelain --untracked-files=no)"
git submodule update --init --recursive --depth 1
mkdir -p build
CC=clang-21 CXX=clang++-21 cmake -S . -B build -G Ninja \
  -DCOMPILER_CACHE=disabled -DCMAKE_BUILD_TYPE=Release -DENABLE_THINLTO=OFF \
  -DENABLE_TESTS=OFF -DENABLE_RUST=OFF -DENABLE_UTILS=OFF -DENABLE_EXAMPLES=OFF \
  > build/configure.log 2>&1
cmake --build build --target clickhouse > build/compile.log 2>&1
mkdir -p build/kayro-image
cp build/programs/clickhouse build/kayro-image/clickhouse
revision=$(git rev-parse HEAD)
binary_sha=$(sha256sum build/kayro-image/clickhouse | cut -d ' ' -f 1)
docker build -f docker/kayro/Dockerfile --build-arg SOURCE_COMMIT="$revision" \
  --build-arg BINARY_SHA256="$binary_sha" -t "$image" build/kayro-image
