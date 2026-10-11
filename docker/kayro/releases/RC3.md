# RC3: o GC do CAS só faz read-ahead do que listou

O GC do CAS deixa de pedir ao S3 chaves que não existem. O líder do GC fazia cerca de 2,4 milhões de GET com 404 por dia por causa do read-ahead; com o RC3, o read-ahead só busca chaves que o LIST da própria rodada já viu, e o mount de um namespace é resolvido só pelos server roots listados.

| Campo | Valor |
|---|---|
| Commit | `d2e59f1c2658cf82a73b05975575a0f7fff9569b` (head do PR #1) |
| Merge | `9700e6edde6ed883565ccadbed5edb3a8a9796e2` em `kayro/26.6-cas-load-recovery`, 11/10 20:08:32Z, árvore idêntica a `d2e59f1c` |
| Base | RC2 (`294ea1e`; compilado `fdc4d38f`). Nenhum commit do RC2 toca o código mudado aqui |
| PR | https://github.com/use-kayro/ClickHouse/pull/1 (branch `kayro/gc-readahead-listed-keys`) |
| Imagem | `474668394551.dkr.ecr.us-east-2.amazonaws.com/kayro/clickhouse@sha256:6469a0c1da2508ba2d01cd04c6870861f94df8f80a68e71f7f6751f1eac6acfa` |
| Tag da imagem | `kayro/clickhouse:d2e59f1c2658cf82a73b05975575a0f7fff9569b` |
| `BUILD_COMPLETE` | 11/10 20:05:39Z, builder workspace `rc3b` (destruído às 20:10:15Z) |
| SHA-256 do binário | não registrado na evidência copiada |
| Linear | TEC-843 (patch), TEC-844 (canário e rollout), TEC-842 (builder) |
| Tag git / release | nenhuma |
| Upstream | PR na Altinity **não aberto**. Precisa de rebase em `antalya-26.6`, que mudou `Gc/CasGc.cpp` e `Gc/CasGcReadAhead.h` depois da tag (`c4ed6fc881` 01/10, `bbdbb2c7a0` 07/10). Corpo pronto em `tec-843-fork-upstream-pr-body.md` (máquina do operador). Espera o resultado do canário |

## O problema

Medido em 11/10, somente leitura:
- 63,9 GET 404 por troca de época por stream (mediana), ou seja, `4 × cas_gc_read_concurrency` com o padrão 16: a janela de read-ahead inteira.
- 792 streams; 568 com 2 ou mais épocas e só 74 com `_snap`; 3.876 épocas com um único registro, o `epoch_seal` que a recovery grava para toda tabela a cada remount.
- 580 dos 604 streams decodificados são tabelas vivas: não é lixo de migração.
- Outros 96 mil GET 404 por dia em `gc/server-roots/<prefix>/mount`.

**Causa.** `hintRefLogsWithinEpoch` (e o lookahead da intake) pede a próxima janela de posições da época atual por aritmética, limitada só por `committed_through`, que já está numa época posterior. A walk só descobre onde a época acaba ao decodificar o selo, então cada troca de época pede uma janela de ids que nunca existem (`CASGCReadAheadWasted`). O sweep de órfãos refaz a visão de proteção de cada namespace a partir de `_snap` ou do início a cada página, e cada remount acrescenta uma época só com selo a cada tabela. O desperdício é 64 × épocas × visitas e cresce a cada remount.

`floorForNamespace` lia a chave de mount de todo prefixo do namespace, do mais longo ao mais curto: para `writer/store/913/<uuid>@cas@`, dois 404 por página antes de chegar a `writer`.

## O que muda

A rodada já tem as duas listagens em memória: a fase 4 lista `cas/ns/stream/` (`RefScanSummary::logs_by_life`) e a fase 3 lista `gc/server-roots/` (`computeHeartbeatFloor`). O RC3 usa essas listagens. Nenhum estado, cache, setting ou formato novo.

- `hintRefLogsWithinEpoch(..., listed)` só faz hint de ids presentes na listagem. Ids de uma época são densos, então o primeiro id não listado encerra a sequência: ou está depois do selo, ou foi escrito depois do LIST e é lido inline. O lookahead da intake para do mesmo jeito.
- `HeartbeatFloor::listed_srids` volta para a rodada e vai até `fold` e `planManifestCursorPage`; `floorForNamespace` lê só os prefixos listados.
- `floorForNamespace`, `prefixEligible` e `sweepNamespace` exigem `const std::set<String> & listed_server_roots`. fsck, rebuild e decommission chamam `listServerRoots` uma vez, fora dos loops.
- `docker/kayro/build.sh`: a label de versão da imagem vem da tag com que ela é construída (antes era fixa em `rc2`).

**O que não muda.** O contrato do `GcReadAhead` continua: guarda resultados, nunca decisões. Toda leitura, decodificação, contador, hold e evento acontece onde acontecia. Uma chave que o LIST omitiu é lida inline pela chave exata. Concorrência 1 não faz hint, como antes. A decisão do GC é idêntica.

### Mudança de comportamento conhecida

Um mount criado depois da listagem de `gc/server-roots/` feita pela rodada só é visto na listagem seguinte, em todos os caminhos (commit `260d77d8`). O sweep já se comportava assim; o check de precommit morto do fold, não. É a direção conservadora: mount não encontrado → prefixo retido, barreira mantida, rebuild protege a mais. Só acontece depois de `DROP POOL MEMBER`. Fora de uma rodada, `listServerRoots` custa um LIST mais um GET por slot de mount, uma vez por chamada de fsck, rebuild ou decommission.

## Commits (PR #1)

| Commit | O que faz |
|---|---|
| `9ade0105` | hint só de ref logs listados; mounts resolvidos pelos server roots listados |
| `da8927da` | `docker/kayro`: label de versão = tag do build |
| `52283346` | atualiza `ExpectedFoldSignature` em `gtest_cas_ref_catalog.cpp` (o fold ganhou `const std::set<String> &` entre `const RefPlan &` e `UniversePolicy`) |
| `e307ff47` | `/enxuto`: remove o descarte de hints no cruzamento de selo (código morto: nenhum id depois de um selo é listado) e enxuga os testes de read-ahead |
| `260d77d8` | `/enxuto`, decisão do dono "faz o mais elegante": um modo só para resolver mount, sempre pelos roots listados; o caminho `nullptr` (tentar todo prefixo) foi apagado |
| `d2e59f1c` | testes: nomeia todos os campos do inicializador `RoundListingsForTest` (`-Wmissing-designated-field-initializers`) |

Tabela completa de achados e vereditos da revisão `/enxuto`: comentário de 11/10 15:36Z no PR #1.

## Testes novos

- `CASOrphanSweepRequests.EpochCrossingsWasteNoHintAndReadExactlyTheInlineKeys`: `CASGCReadAheadWasted == 0`, a última época ainda tem read-ahead, nenhuma posição depois de um selo é lida, e o conjunto de GETs da página é idêntico inline e com concorrência 16.
- `CASGCArithmeticIntake.ReadAheadAcrossRemountEpochsWastesNothingAndDecidesAsInline`: rodada completa com seis trocas de época e um LIST que omite um registro no meio; concorrência 1 e 16 com métricas, cursor e in-degrees idênticos.
- `CASOrphanSweepRequests.FloorIsReadOncePerNamespacePerPage`: 1 leitura de floor em vez de 3; root ausente custa 0.
- Integração `test_cas_gc_s3::test_gc_read_ahead_wastes_nothing_across_epoch_changes`: 5 restarts (3 com insert, 2 ociosos), `CASGCReadAheadHit > 0`, `Wasted == 0` antes do DROP, objetos da tabela dropada recuperados.

## Builds

| Commit | Digest | Resultado |
|---|---|---|
| `da8927da` | `sha256:2cc431e769c1491da5b8f1ca905488c646c1649df859c83140a37ef7bb2a67ea` (push 14:14Z) | **Descartada.** Paridade e fault/RLS passaram; gtests não compilaram (`gtest_cas_ref_catalog.cpp:55`, `static_assert` da assinatura do fold). `test_cas_gc_s3` não rodou. Builder `default` destruído às 14:59:30Z |
| `260d77d8` | `sha256:e5c35d6012cbe5059a6cce32075e7c83f927daac28a77d2b7a1d71d7b37062d7` (push 17:33Z) | **Descartada.** Paridade e fault/RLS passaram (shutdown 4,01 s); gtests não compilaram: `cas_sweep_test_support.h:25:71: missing field 'ref_logs' initializer` em 3 TUs. Ninja parou em 15660/17367 |
| `d2e59f1c` | `sha256:6469a0c1da2508ba2d01cd04c6870861f94df8f80a68e71f7f6751f1eac6acfa` | **Verde**, abaixo |

## Portão em `d2e59f1c` (workspace `rc3b`, 18:22–20:05Z)

| Passo | Resultado |
|---|---|
| Imagem | `docker/kayro/build.sh …/kayro/clickhouse:d2e59f1c…`, 18:22:36Z → 19:37:50Z |
| `compare-features.py` | igual à oficial: funções 1842, formatos 114, engines 81, table functions 91, settings 1713; nada faltando ou sobrando |
| `test_cas_load_recovery` (fault/RLS) | RLS writer e reader; 3 recuperações de carregamento sem restart (writer, reader, writer); shutdown durante a falha 1,9 s, exit 0 |
| Gtests CAS | **2527/2527** (0 falhas, 0 erros, 2 desabilitados pré-existentes: `CASProtocol.DISABLED_RevalidateAbsentTreeDepRecreates`, `CASProtocol.DISABLED_AdoptTreeOfReclaimedTreeFailsClosedAtAdoptTime`) |
| `test_cas_gc_s3` | **2/2**: `test_gc_reclaims_dropped_blobs`, `test_gc_read_ahead_wastes_nothing_across_epoch_changes` |

Custo do builder `rc3b`: cerca de 4,4 h de c7g.8xlarge, ~US$ 5,3. Evidência na máquina do operador: `/tmp/cost-plan-600-20261010/rc3b/d2e59f1c/` (`evidence.tar.xz` sha256 `d104a1e0ab605631ffb0737a3a4d2de3590b68cb2ad89cdd1be0d60e01090a41`).

## Config do kayro-infra que acompanha

| PR | Merge | O que mudou |
|---|---|---|
| #216 | `cd9d2a2`, 11/10 11:12Z | `merge-tree.xml.tftpl` por papel: readers com `replicated_can_become_leader=0` e `always_fetch_merged_part=1`; `configure-reader-role.py` (TEC-836) |
| #218 | `cc538bb`, 11:25Z | `cas_gc_interval_sec` 60 → 600 em todos os papéis (TEC-835/837). Continua no RC3 |
| #219 | `afed28c`, 11:58Z | `cas_gc_read_concurrency=1` em todos os papéis (mitigação até o RC3, TEC-838) |
| #221 | `aa24df0`, 12:30Z | `configure-reader-role.py --storage`: role e storage num restart só (TEC-845) |
| #222 | `21bacf0`, 15:26Z | `configure-image.py --storage`: imagem e storage num restart só; reader-mdb sem `cas_gc_read_concurrency` para o canário |
| #229 | `d7a5722`, 20:31Z | `cas_gc_read_concurrency` sai de **todos** os papéis: com o RC3 o padrão (16) não desperdiça. A remoção da setting da lista `tunable` do `configure-filesystem-cache.py` e o bump de `clickhouse_shared_image` ficam para um PR depois do writer |

O #228 (só o reader compartilhado sem a linha) foi fechado sem merge e substituído pelo #229.

Ordem que importa: #219 foi aplicado nos três nós **antes** do RC3, por decisão do coordenador (economia imediata). Por isso o RC3 sobe junto com a remoção da linha, no mesmo restart (`--storage`); só assim o read-ahead volta ao padrão e o fix é medido.

## Rollout (TEC-844, 11/10)

Imagem anterior (rollback): RC2 `sha256:ecb7ee0add4cdf9887baaffa9b20599e966eea1c91e754482acb2b1eda534718`. Storage renderizado do `main` do kayro-infra (`d7a5722` para o compartilhado e o writer).

Regra de aceite (decisão do dono, 11/10 ~20:00Z): por evidência, não por relógio. Para o reader-mdb: 2 h cheias; `CASGCReadAheadWasted == 0` com `CASGCReadAheadHit > 0`; o GC ainda apaga e `pending_reclaim` não cresce; nenhum readonly, atraso 0; `NoSuchKey` sem Warning/Error e dentro da base em Debug (581–1.060/h com read-ahead desligado); lease do writer intacto 5 min depois do restart.

| Nó | Apply | Resultado |
|---|---|---|
| Reader-mdb `i-0bc5ccb44d4141059` | 20:45:05Z → 20:45:13Z; container às 20:45:10Z | **Aceito às 21:46Z.** CAS `reader-mdb` live na época 15 (era 14); loader 196 OK, 0 falhas; mdb 54 tabelas, 0 readonly, atraso 0. Rajada de LIST: 160.347 em 10 min (27.682 throttled). Lease do writer intacto (época 33). `NoSuchKey` 186 (20:45–21:00) e 108 (21:00–21:45), só Debug. Nenhuma classe de erro nova. Backups: `kayro-clickhouse.service.pre-image-20261011T204506017766Z`, `server.xml.pre-image-20261011T204506018408Z` |
| Reader compartilhado `i-02369cef982eb67d2` | 21:55:05Z → 21:55:28Z; container às 21:55:11Z | Health OK às 22:06Z: CAS `reader` live na época 13; loader 1.223 OK, 0 falhas; 16 bancos, 0 readonly, atraso 0. A primeira consulta a `system.replicas` ficou bloqueada ~582 s durante o start. Nenhuma classe de erro nova. Leases do writer (época 33) e do reader-mdb intactos. Backups: `…pre-image-20261011T215505839721Z` e `…20261011T215505841755Z`. Aceite final: não registrado quando este arquivo foi escrito |
| Writer `i-02e3809e0b416b25e` | **planejado** para 11/10 22:33Z, se a observação do compartilhado (21:55–22:25Z) sair limpa | Guarda de INSERT; depois do restart, `system.settings` tem que mostrar `wait_for_async_insert=1` e `async_insert_busy_timeout_max_ms=200` (limpa a sobra do perfil `kayro_batch`, kayro-infra #225, TEC-847). Resultado: não registrado quando este arquivo foi escrito |

### Medição do fix no líder do GC

O restart do reader-mdb soltou o lease do GC: o reader compartilhado (ainda RC2) virou líder às 20:55:25Z, então o critério wasted/hit não pôde ser lido no reader-mdb e passou para "o nó que liderar o GC já no RC3". Depois do restart do compartilhado, **o reader-mdb (RC3) voltou a liderar o GC por volta de 22:05Z** (`is_leader=1` às 22:07:02Z). É nele que se lê `CASGCReadAheadWasted == 0` com `Hit > 0`, com concorrência padrão:

```sql
SELECT toStartOfHour(event_time) h, sum(ProfileEvent_S3GetObject) get, sum(ProfileEvent_S3ListObjects) list,
  sum(ProfileEvent_S3ReadRequestsErrors) read_err, sum(ProfileEvent_CASGCReadAheadWasted) wasted, sum(ProfileEvent_CASGCReadAheadHit) hit
FROM system.metric_log WHERE event_time >= toStartOfHour(now()) - INTERVAL 3 HOUR GROUP BY h ORDER BY h;
```

Primeiras rodadas do reader-mdb como líder no RC3, com concorrência padrão (16):

| Rodada | Hora | `CASGCReadAheadWasted` | `CASGCReadAheadHit` | GET 404 | Observação |
|---|---|---:|---:|---:|---|
| 1 | 22:07Z | 19 | 2.908 | 337 | rodada de tomada da liderança |
| 2 | 22:18Z | 1 | 2.213 | 2 | |

Referência no RC2: ~1.000 GET 404 por hora no reader-mdb com read-ahead desligado.

Leitura: `CASGCReadAheadWasted` não conta só 404. Ele também conta slots de read-ahead esperados que a rodada não chegou a consumir, então um valor pequeno e diferente de zero não significa chave inexistente pedida ao S3. O número que mostra o fix é o de 404 por rodada: 2 na rodada 2, contra ~1.000/h no RC2.

Se o desperdício voltar a crescer, a saída é só config (`cas_gc_read_concurrency=1`), sem efeito nas decisões do GC.

Linha de base do reader-mdb no RC2 com concorrência 1, como líder (19:56Z): GET 89.615 / 76.221 / 35.365 por hora (16:00, 17:00, 18:00Z), read_err 604–1.060/h, `pending_reclaim` 472.

## Depois do rollout

- PR no kayro-infra: `clickhouse_shared_image` → digest do RC3 e saída de `cas_gc_read_concurrency` da lista `tunable`.
- Este código morre no passo 3 da milestone 6 (ver [ROADMAP](ROADMAP.md)), junto com o read-ahead e o sweep que ele corrige. Até lá é a mitigação.
- Não resolvido no RC3: sem `_snap`, a visão de proteção ainda refaz cada namespace desde o início a cada página (a maior parte dos ~956 mil GETs bem-sucedidos por dia), e a tempestade de LIST no restart (TEC-845).
