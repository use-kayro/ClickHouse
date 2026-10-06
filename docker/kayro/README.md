# Kayro CAS recovery candidate

Based on Altinity tag `v26.6.4.20001.altinityantalya`, source
`2073b1f88dec885444fac97edb8179ad1753c1ad`. Only asynchronous table construction
retries the typed transient CAS mount-state exception. Fencing, terminal failures,
and generic write retry/convergence errors retain their behavior.

## Build

Use an isolated ARM64 Ubuntu 24.04 AWS builder with Docker, CMake, Ninja,
Clang/LLVM/LLD 21.1.8, nasm, yasm, gawk and pkg-config installed:

```sh
bash docker/kayro/build.sh REGISTRY/kayro/clickhouse:26.6.4.20001-kayro-cas1-rc1
```

The script builds this checkout, replaces the server binary in the pinned Altinity
image, checks its SHA-256, and records the source commit in OCI labels. It does not
push or change any deployed node. Use the registry digest, not a movable tag, in a
future rollout. Build logs remain under `build/`.

This release candidate preserves the flags used in the successful AWS comparison:
Release, thin LTO/tests/Rust/utilities/examples disabled. It is NOT yet qualified
as feature-equivalent to the official distribution or approved for production.
The server version string stays upstream; identify this build by digest and labels.

## Validation

The standalone fault harness is in `tests/integration/test_cas_load_recovery/`.
On 2026-10-06 the unmodified build remained failed after CAS recovered. This patch
recovered writer, reader and writer again, preserving exact rows and the table UUID;
a new row replicated after each recovery. Terminal disk refusal remained intact.
Shutdown during the outage completed in 3.61 seconds, exit 0.

These synthetic tests do not establish production-load or RLS parity. Evidence is
tracked in TEC-736 and use-kayro/kayro-infra PR #182. No production change is part
of building or publishing this image.
