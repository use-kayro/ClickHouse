# Passo 2c: o GC espera o startup das tabelas em vez de comparar contagens

Corrige uma pausa do GC do CAS no writer: depois do primeiro DROP de uma tabela que existia no boot, o GC passava a achar que alguma tabela não estava carregada e não apagava mais nada até o próximo restart. Nenhum dado se perdeu; só a recuperação de espaço parou. Sem mudança de protocolo, sem setting nova, sem formato novo.

| Campo | Valor |
|---|---|
| PR | https://github.com/use-kayro/ClickHouse/pull/12 (branch `kayro/gc-loaded-tables`), head `1550abe87523a982a589ea04890f006e45ef3c27`, depois da revisão `/enxuto` |
| Merge | `703cb49f4ed74d16ac166138cf1671c4abec3992` em `kayro/26.6-cas-load-recovery` (head `1550abe8` preservado) |
| Base | passo 2b (`a6355a99`) |
| Imagem | `474668394551.dkr.ecr.us-east-2.amazonaws.com/kayro/clickhouse@sha256:6dcd83ffb0cc11258fe279e4f166e248926391446cb3208535efdc6cb6a8db02` |
| Tag da imagem | `kayro/clickhouse:1550abe87523a982a589ea04890f006e45ef3c27` (head do PR #12) |
| Linear | TEC-850 |
| Em produção | nos três nós desde 12/10 21:37:25Z (writer → reader-mdb → reader compartilhado), na 2ª tentativa; a 1ª fez rollback automático por falso negativo da gate |

## Incidente (12/10)

- No writer, a última rodada do GC que apagou foi às 07:11:43Z. A primeira "CAS GC: some table on the disk cannot be read this round (...); removing nothing" saiu às 07:21:43Z. Até 12:31Z foram 32 rodadas sem apagar nada.
- As 499 réplicas estavam saudáveis. Nada foi perdido.

**Causa.**
- O provider do passo 2 (`casKeeperRecordSourcesOnDisk`, `src/Storages/MergeTree/CasKeeperPartRecord.cpp`) iterava com `skip_not_loaded=true` e inferia "tabela não carregada" de `loaded < getAllTableNames().size()`.
- `DatabaseOrdinary::getAllTableNames` inclui os nomes de `startup_table`, que nunca são apagados depois de DROP, DETACH, RENAME ou EXCHANGE (só `stopLoading` limpa). O primeiro DROP de uma tabela da época do boot deixa um nome fantasma, e a contagem fica maior até o próximo restart.

**Gatilho em produção.**
- A carga do mdb às 07:16Z substituiu duas MVs refreshable, `mv_cliente_sku` e `mv_coorte_sku`: CREATE `__stage`, EXCHANGE, DROP da antiga.
- O DROP da MV antiga derruba a tabela `.inner_id` dela, criada no boot.
- As tabelas de staging das cargas horárias são criadas e dropadas na mesma carga, depois do boot. Por isso os 1.410 DROP/RENAME/EXCHANGE desde 06:01Z não dispararam o problema.

## O que muda

- O provider itera com `skip_not_loaded=false`. Assim `DatabaseOrdinary::getTablesIterator` espera as tarefas de startup do banco antes de tirar o snapshot, e o snapshot já tem todas as tabelas, carregadas e iniciadas.
- A comparação de contagens e o contador `loaded` saem.
- Ficam as checagens de tabela detached e de tabela não replicada no disco.
- `DatabaseOrdinary` não é tocado: o GC fica correto com ou sem o vazamento de `startup_table`.

### Mudança de contrato

- Se o load ou o startup de uma tabela falhou, ou foi cancelado pelo `stopLoading` do shutdown, a espera lança `ASYNC_LOAD_WAIT_FAILED`. A rodada falha ("CA GC round failed (will retry next tick)") em vez de logar "cannot be read". Nos dois casos nada é apagado.
- Uma rodada manual (`SYSTEM CAS GC RUN`) durante o boot segura `gc_scheduler_mutex` até todas as tabelas terem iniciado.

## Teste

`test_cas_gc_s3::test_gc_after_drop_of_boot_table`: cria uma tabela replicada descartável, reinicia o servidor e faz DROP dela; depois insere numa tabela CAS, faz DROP PARTITION e exige que os blobs e os `cas_parts` sumam na rodada seguinte. Sem a correção, nenhuma rodada apaga e o teste falha.

### Portão do builder (`gcfix`, imagem de `1550abe8`)

| Verificação | Resultado |
|---|---|
| Gtests CAS | **1.946/1.946** passaram, 0 falhas (2 desabilitados) |
| `test_cas_gc_s3` | **2/2**: `test_gc_reclaims_dropped_blobs` e `test_gc_after_drop_of_boot_table`. Rodado duas vezes: em paralelo (14:34–14:38Z) e na receita (15:25–15:26Z) |
| Harness fault/RLS (`test_cas_load_recovery`) | OK: `loading_fault_observed`, `loading_recovered_without_restart`, `shutdown_during_fault` exit 0 |

## Upstream

O vazamento de `startup_table` está corrigido em ClickHouse/ClickHouse#106238 (commit `22658b49`, 29/06/2026), presente no master e no 26.7 em diante, não na base 26.6 deste fork. O cherry-pick é uma questão separada; esta correção vale com ou sem ele.

## Regra de deploy

Sem mudança de protocolo: qualquer ordem, um nó por vez. Só o writer roda rodadas que apagam, então é nele que a correção aparece.

**Rollback:** a imagem do passo 2b, `sha256:1912bde084b7a39c86af8498715628da23be3554d42da1341aeafcb0cd34ba03`. Voltar para ela traz o bug de volta (a pausa depois do primeiro DROP de tabela do boot), e nada mais.

## Imagem de release

`474668394551.dkr.ecr.us-east-2.amazonaws.com/kayro/clickhouse@sha256:6dcd83ffb0cc11258fe279e4f166e248926391446cb3208535efdc6cb6a8db02`, construída do head `1550abe87523a982a589ea04890f006e45ef3c27` no builder `gcfix`:

- início 13:19:21Z;
- push 14:33:52Z;
- `BUILD_COMPLETE` 15:26:45Z.

Paridade de features: `compare-features.py` contra a base oficial fixada, idêntica em build_options, functions, formats, engines, table_functions e settings (`parity.json`, 14:29:01Z).

## Deploy (12/10)

Orquestrado por systemd no builder `gcfix`, não do Mac. Ordem writer → reader-mdb → reader compartilhado. Rollback preparado: passo 2b, `sha256:1912bde084b7a39c86af8498715628da23be3554d42da1341aeafcb0cd34ba03`.

### 1ª tentativa: rollback automático por falso negativo da gate

- Writer: apply 17:30:05Z, saudável 17:33:10Z.
- A gate da fase A esperava 17 min pela linha "CA GC round 1" e desistiu às 17:50:04Z com `rounds=0`. Rollback automático do writer para `1912bde0` às 17:50:07Z. Readers não foram tocados.
- **Causa: falso negativo da gate, não defeito da imagem.** A linha da rodada só sai no **fim** da rodada, e a rodada 1 tinha 46.638 registros condenados acumulados nas ~10 h de pausa. Na imagem antiga, depois do rollback, a mesma rodada 1 fechou às 18:10:21Z (19,4 min depois do mount, `condemned=46638`), e as rodadas 2–13 apagaram o backlog (46.537 na rodada 2).
- No intervalo 17:30–17:51Z, nenhum erro do GC e nenhum "removing nothing".
- O vazamento de `startup_table` é por processo e um restart o zera. Por isso a imagem antiga voltou a apagar depois do rollback, até o próximo DROP de tabela do boot.
- Gate corrigida: espera até 25 min pela rodada 1 fechar (`writer-gate.sh`, 75 × 20 s).

### 2ª tentativa

| Nó | Apply | Saudável | Observação |
|---|---|---|---|
| Writer `i-02e3809e0b416b25e` | 20:51:12Z | 20:54:13Z | rodada 1 às 21:02:06Z (`condemned=313 deleted=0`); `WRITER_GATE_OK` 21:02:24Z |
| Reader-mdb `i-0bc5ccb44d4141059` | 21:30:10Z | 21:30:35Z | |
| Reader compartilhado `i-02369cef982eb67d2` | 21:30:35Z | 21:37:25Z | |

**Prova do fix em produção (fase B):** a tabela-sonda `default.gc_probe_boot_20261012`, uma tabela do boot com 0 linhas, foi dropada às 21:02:25Z. A rodada 2, às 21:12:19Z, deu `condemned=0 deleted=313`, sem "removing nothing". `PHASE B OK` às 21:13:33Z.

`SEQUENCE DONE` às 21:37:25Z.

**Depois do deploy:**
- 0 réplicas readonly nos três nós (501 / 54 / 446);
- a lista de tabelas Replicated do writer é igual à do reader compartilhado (563, mesmo hash);
- `cepera.customer_embeddings`: 16.714 = 16.714.

**kayro-infra:** PR #238 põe o digest `6dcd83ff` no tfvars; o plan só muda outputs.
