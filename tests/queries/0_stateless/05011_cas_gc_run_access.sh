#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Intent (E1):
#  1) A role granted only "SYSTEM CAS GC RUN" is ALLOWED to run "SYSTEM CAS GC RUN".
#  2) A user with ZERO grants gets ACCESS_DENIED on the plain
#     "SYSTEM CAS GC RUN 'no_such_disk'" -- the privilege check runs
#     before disk resolution, so denial fires even though the named disk does not exist (it would
#     otherwise be UNKNOWN_DISK).
# (No CA disk needs to exist: the privilege check fires before any disk I/O; assert on the specific
#  error codes. The `default` disk always exists and is
#  never content-addressed, so it deterministically yields BAD_ARGUMENTS once a check is passed.)

${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS user_test_05011;
CREATE USER user_test_05011 IDENTIFIED WITH plaintext_password BY 'user_test_05011';
REVOKE ALL ON *.* FROM user_test_05011;
GRANT SYSTEM CAS GC RUN ON *.* TO user_test_05011;
"""

# GC-only role: the per-round GC is allowed (fails later, on the disk-type check).
${CLICKHOUSE_CLIENT} --multiline --user user_test_05011 --password user_test_05011 -q """
SYSTEM CAS GC RUN default; -- { serverError BAD_ARGUMENTS }
"""

# A zero-grant user is denied before the disk is even resolved: naming a disk that does not exist
# still yields ACCESS_DENIED, not UNKNOWN_DISK.
${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS user_test_05011_zero_grants;
CREATE USER user_test_05011_zero_grants IDENTIFIED WITH plaintext_password BY 'user_test_05011_zero_grants';
REVOKE ALL ON *.* FROM user_test_05011_zero_grants;
"""

${CLICKHOUSE_CLIENT} --multiline --user user_test_05011_zero_grants --password user_test_05011_zero_grants -q """
SYSTEM CAS GC RUN 'no_such_disk'; -- { serverError ACCESS_DENIED }
"""

${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS user_test_05011;
DROP USER IF EXISTS user_test_05011_zero_grants;
"""
