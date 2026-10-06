#!/usr/bin/env python3
"""Compare server capabilities in two isolated local Docker images."""
import json
import subprocess
import sys
from pathlib import Path

original, candidate, output = sys.argv[1:]
queries = {
    'build_options': 'SELECT name,value FROM system.build_options ORDER BY name',
    'functions': 'SELECT name,is_aggregate,alias_to FROM system.functions ORDER BY name',
    'formats': 'SELECT name,is_input,is_output FROM system.formats ORDER BY name',
    'engines': 'SELECT name FROM system.table_engines ORDER BY name',
    'table_functions': 'SELECT name FROM system.table_functions ORDER BY name',
    'settings': 'SELECT name,type FROM system.settings ORDER BY name',
}
results = {}
for label, image in [('official', original), ('candidate', candidate)]:
    results[label] = {}
    for surface, query in queries.items():
        run = subprocess.run([
            'docker', 'run', '--rm', '--network', 'none', '--memory', '2g',
            '--entrypoint', '/usr/bin/clickhouse', image, 'local', '--query',
            query + ' FORMAT JSONEachRow',
        ], capture_output=True, text=True, check=True, timeout=120)
        results[label][surface] = {row['name']: row for row in map(json.loads, run.stdout.splitlines())}
Path(output).write_text(json.dumps(results, indent=2) + '\n')
failed = False
for surface in queries:
    before, after = results['official'][surface], results['candidate'][surface]
    changed = [key for key in before.keys() & after.keys() if before[key] != after[key]]
    missing, added = sorted(before.keys() - after.keys()), sorted(after.keys() - before.keys())
    # Paths/toolchain identity may differ; enabled server libraries may not disappear.
    if surface == 'build_options':
        failed |= any(key.startswith('USE_') and before[key]['value'] == '1'
                      and after.get(key, {}).get('value') != '1' for key in before)
    else:
        failed |= bool(missing or added or changed)
    print(json.dumps({'surface': surface, 'official_count': len(before), 'candidate_count': len(after),
                      'missing': missing, 'added': added, 'changed': sorted(changed)}), flush=True)
if failed:
    sys.exit('Server capability parity failed; inspect the recorded comparison')
