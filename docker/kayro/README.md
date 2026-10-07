# Kayro CAS recovery candidate

Based on Altinity tag `v26.6.4.20001.altinityantalya`, source
`2073b1f88dec885444fac97edb8179ad1753c1ad`. Only asynchronous table construction
retries the typed transient CAS mount-state exception. Fencing, terminal failures,
and generic write retry/convergence errors retain their behavior.

## Build

Use an isolated ARM64 Ubuntu 24.04 AWS builder with Docker, CMake, Ninja,
Clang/LLVM/LLD 21.1.8, nasm, yasm, gawk, pkg-config, and Rust
`nightly-2026-03-22` (with `rust-src`) installed:

For the isolated 64 GiB builder, reserve 64 GiB of temporary swap on its scratch
EBS volume before linking the full debug/ThinLTO binary. This is builder-only;
never run these commands on a serving ClickHouse node. The swap file is removed
with the disposable builder volume:

```sh
sudo fallocate -l 64G /opt/tec-736/build.swap
sudo chmod 600 /opt/tec-736/build.swap
sudo mkswap /opt/tec-736/build.swap
sudo swapon /opt/tec-736/build.swap
```

```sh
bash docker/kayro/build.sh REGISTRY/kayro/clickhouse:26.6.4.20001-kayro-cas1-rc2
```

The script builds this checkout, replaces the server binary in the pinned Altinity
image using the upstream split-debug artifact, checks its SHA-256, and records
the source commit in OCI labels. The complete binary stays in the build directory. It does not
push or change any deployed node. Use the registry digest, not a movable tag, in a
future rollout. Build logs remain under `build-release/`.

RC1 disabled Rust and was rejected by feature comparison: DeltaLake engines and
table functions plus Wasmtime were missing. RC2 enables Rust and uses
`RelWithDebInfo`, ThinLTO and the upstream ARM64 sysroot. Tests/utilities/examples
remain disabled; those are build targets, not server features. Feature comparison
and fault/RLS checks must pass on the resulting image before production rollout.
The server version string stays upstream; identify this build by digest and labels.

## Validation

The standalone fault harness is in `tests/integration/test_cas_load_recovery/`.
On 2026-10-06 the unmodified build remained failed after CAS recovered. This patch
recovered writer, reader and writer again, preserving exact rows and the table UUID;
a new row replicated after each recovery. Terminal disk refusal remained intact.
Shutdown during the outage completed in 3.61 seconds, exit 0.

The final RC2 image also passed comparison with the pinned official image:
1,842 functions, 114 formats, 81 engines, 91 table functions and 1,713 settings
matched exactly; enabled libraries include DeltaLake and Wasmtime. Build metadata
is different, as expected for a downstream build.

RC2 recovered writer and reader without a recovery restart, preserved the table
UUID and replicated new rows. Identical queries by two restricted users returned
disjoint, nonempty results before and after recovery; policy modification was
refused. Terminal disk refusal remained intact. Shutdown during an outage took
3.87 seconds with exit 0. These are isolated synthetic tests, not a production-load
qualification or a guarantee against storage/network outages.

Evidence is tracked in TEC-736 and attached to the RC2 release. No production
change is part of building or publishing this image.

## Publish

The publishing host needs AWS CLI v2 and permission to push to the existing ECR
repository. Installing the AWS CLI ZIP distribution also requires `unzip`.
Authenticate with the instance role; do not copy personal AWS credentials into
the image or build context. After a successful build:

```sh
registry=474668394551.dkr.ecr.us-east-2.amazonaws.com
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin "$registry"
docker push "$registry/kayro/clickhouse:26.6.4.20001-kayro-cas1-rc2"
docker logout "$registry"
```

The ECR repository is declared in `use-kayro/kayro-infra`, PR #184. No deployment
is performed by these commands. Preserve the returned digest in the release record.
