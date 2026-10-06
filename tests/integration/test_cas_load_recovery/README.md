# Standalone CAS loading regression

Run only in an isolated Linux Docker environment with synthetic data. Docker
Compose creates private test storage, Keeper, writer, reader and fault proxies.
No production credentials or endpoints are used. This is a standalone reproduction,
not yet wired into the upstream pytest integration runner.

```sh
cd tests/integration/test_cas_load_recovery
export CLICKHOUSE_IMAGE=REGISTRY/kayro/clickhouse:26.6.4.20001-kayro-cas1-rc1
python3 prepare.py
docker compose up -d store keeper writer-proxy reader-proxy
python3 lab.py init
python3 lab.py loading --role writer --expect-recovery
python3 lab.py loading --role reader --expect-recovery
python3 lab.py loading --role writer --expect-recovery
python3 lab.py shutdown --role writer
docker compose down -v
```

Always run `docker compose down -v` after saving `.lab/results.jsonl`, including
on test failure. For the unmodified control omit `--expect-recovery`: that test
requires the cached `ASYNC_LOAD_WAIT_FAILED`/`NETWORK_ERROR` after storage recovery.
Tests and compiles belong on AWS; redirect output to a build log for review.
