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
python3 lab.py rls --role writer
python3 lab.py rls --role reader
python3 lab.py loading --role writer --expect-recovery
python3 lab.py loading --role reader --expect-recovery
python3 lab.py loading --role writer --expect-recovery
python3 lab.py shutdown --role writer
docker compose down -v
```

Always run `docker compose down -v` after saving `.lab/results.jsonl`, including
on test failure. Since TEC-851 step 3c a table loads from its Keeper part records and
the pool holds only blob bodies: the loading scenario interrupts the whole pool while
the server starts and requires the load to succeed and the server to recover without
a restart; there is no mount lease to lose and no `SYSTEM CAS FORGET` to test.
Tests and compiles belong on AWS; redirect output to a build log for review.
