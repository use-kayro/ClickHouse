# Passo 2: GC do CAS derivado dos registros no Keeper

O GC do pool deixa de listar o S3 e de reler os ref logs para saber o que está vivo. Um blob está vivo enquanto algum registro de parte no Keeper (do [passo 1](step1-keeper-part-record.md)) o referencia. Só o writer apaga. A rodada antiga do GC, que listava e relia o bucket, foi apagada do código.

| Campo | Valor |
|---|---|
| PR | https://github.com/use-kayro/ClickHouse/pull/4 (branch `kayro/keeper-gc`), empilhado no #2 |
| Merge | `59caa364606b5745e84641e387e706668b82a205` em `kayro/26.6-cas-load-recovery`, 12/10 01:14:54Z |
| Head do código | `023e6c46293f3507d8651950877ba21c7dfd5fad` |
| Base | passo 1 (PR #2, merge `b71585e`), sobre o RC3 |
| Imagem | `TODO` |
| Linear | TEC-850 |
| Em produção | nenhum nó (deploy pendente) |

Commits:
- `7c6c4c8` teste `test_cas_reader_pin`;
- `2c32d91` o GC derivado dos registros;
- `4d473c2` a remoção da rodada antiga;
- `18be279` porte para tabelas replicadas dos testes que verificavam a recuperação do GC antigo;
- `023e6c4` correções da revisão: tabelas vindas do `DatabaseCatalog`; o mark devolve o que marcou; registros de outro pool; nenhum GET de `gc/state` no DROP; o relatório mostra só o que a rodada faz; gtests `KeeperGc`.

## Regra de deploy

**Writer primeiro, depois um reader por vez. Um reader só recebe esta imagem depois que o writer já roda com ela.**

Os readers no RC3 ainda rodam o GC antigo e podem tomar o lease dele enquanto o writer não faz mais heartbeat. Por isso os dois readers sobem um logo depois do outro, sem intervalo longo entre eles.

Antes do deploy, ver as pré-condições abaixo (em especial: nenhuma tabela detached no writer).

## O que muda

- **Vivacidade pelo Keeper.** Um blob está vivo enquanto algum registro `<zk>/cas_parts/<p>` o referencia. O registro sobrevive à remoção da parte nas réplicas, então ele é a fila persistente de delete.
- **Só o writer apaga.**
- **Custo.** Uma rodada não lista nada além do `gc/server-roots/` do heartbeat floor.
- **Continua como antes:** a condenação em duas fases `.meta = Condemned`, o delete por ETag exato e o heartbeat floor (decisão de 11/10, revisada).
- **Sai:** a rodada antiga inteira (lease, fold, in-degree, read-ahead, orphan sweep, namespace janitor), cerca de 35,8 mil linhas com seus testes.
- **Não muda:** as escritas de metadata S3 do writer (isso é o passo 3c) e o caminho de attach dos readers (passo 1).

### Rodada N: marcar

- Um registro cuja parte nenhuma réplica lista em `/replicas/*/parts` recebe um `set` com os próprios bytes, com checagem de versão. O mtime passa a dizer quando o GC o encontrou sem réplica. É a marca de condenado: nenhum campo novo, nenhum nó novo.
- Os blobs que só registros não listados referenciam são condenados como antes: lê o ETag, grava `.meta = Condemned` e drena.
- Depois que os marcadores estão duráveis, a rodada anota `Pool::peekNextBuildSeq()` como o corte de build do blob.

### Rodada N+k: remover

Um registro ainda não listado `old_parts_lifetime` depois da marca é removido num **multi-op por registro**:

```
CheckNotExists(<zk>/replicas/<r>/parts/<part>)   para todo r em getChildren(<zk>/replicas)
Check(<zk>/replicas/<writer>/is_active, v)         \  a epoch do writer lida nesta rodada
Check(<zk>/replicas/<writer>/host, v)              /  (nada novo: o par de ativação do RMT)
Remove(<zk>/cas_parts/<part>, record_version)        a versão da marca
```

Depois, cada blob condenado que nenhum registro restante referencia é apagado por ETag exato. Um blob que um writer re-enviou desde a condenação tem ETag novo e é poupado.

Sem mudança no ReplicatedMergeTree, sem lock novo, sem campo novo.

### Outras regras

- **Quais tabelas.** Vêm do `DatabaseCatalog`, por um provider que o Storages instala no startup (`casKeeperRecordSourcesOnDisk`): sem registry, lock ou registro por tabela. A rodada não roda enquanto:
  - alguma tabela no disco não for replicada (não tem registros, então os blobs dela são desconhecidos);
  - alguma tabela ainda não estiver carregada, ou estiver detached (`system.detached_tables`);
  - alguma tabela estiver read-only, por exemplo porque falhou ao iniciar.
- **Ausência transitória não precisa de guarda.** Uma tabela fora do catálogo por um momento (reload, `RESTART REPLICA`) é segura: um blob só sai depois que a rodada da condenação e a da deleção não acharam registro que o referencie, e toda rodada poupa o que uma parte listada referencia.
- **Só o writer.** A rodada só roda onde toda tabela do disco é escrita ali (`is_leader && !is_readonly`, ou seja, `replicated_can_become_leader`). No não-writer, o snapshot retorna antes de ler registros: as rodadas dos readers não fazem leitura no Keeper nem requisição ao S3.
- **Builds em andamento.** Um blob só é apagado depois que `Pool::minActive()`, lido antes dos registros, passou do corte de build dele. Toda build deste servidor que existia quando o blob foi condenado já terminou: commitou (e o registro está no snapshot) ou foi abandonada. Uma build que começa depois do marcador vê `Condemned` e re-envia. É o watermark de build em processo que já existe (`min_active_build_sequence`); ele substitui a leitura das staged edges do writer pelos ref logs, que precisaria do LIST que este passo remove.
- **DROP.** A remoção de namespace no writer não faz mais GET de `gc/state`; `removal_started_round` fica no formato da linha, selado em 0.
- **Partes sem registro.** Nenhum blob é apagado enquanto uma parte listada não tiver registro (os blobs dela são desconhecidos). A rodada loga isso. Cobre a janela do MOVE PARTITION antes do backfill de boot. Um registro de outro pool da policy da tabela conta como registrado para este pool. Uma parte que outra réplica lista num disco não CAS, e que esta réplica não tem, conta como sem registro só transitoriamente.
- **Re-marcação (`marked_records`).** `markCasRecords` devolve as partes que de fato marcou, e só elas entram em `marked_records`. Um registro fora dele é marcado de novo, o que reinicia a retenção: registro marcado por um processo anterior, ou listado de novo desde a marca. Um restart refaz as marcas.
- **Memória.** Condenações e marcas ficam em memória. Um restart as refaz antes de apagar qualquer coisa.

### Invariante

> Um registro só é removido por um multi-op que prova, naquele instante, que nenhuma réplica lista a parte. Todo commit que faz uma réplica listar uma parte ou cria o registro no mesmo multi-op (o produtor, com um nome de parte nunca usado antes), ou confere no mesmo multi-op o registro ou, quando o produtor não pôde criá-lo, que a réplica de origem ainda lista a parte (uma cópia: fetch, relink, ATTACH_PART, fetch do REPLACE_RANGE; #2 `283ca096`).
>
> Logo, nenhuma parte é listada com o registro removido. Blobs só são apagados depois que a remoção do registro deu certo, e só os que nenhum registro restante referencia. Um produtor não recria um registro removido: ele só cria registros de partes que está introduzindo.

## O que foi apagado (`4d473c2`, 139 arquivos, −35.762 linhas)

- Do núcleo: a classe `Cas::Gc` (`Gc/CasGc.cpp`, 4,9 mil linhas); o read-ahead (`CasGcReadAhead`), o plano de shards do GC, o namespace janitor, o drain `Removing` do catálogo (`CatalogLifecycleReconciler`) e os formatos de estado de manutenção e de outcome; a metade fold do `CasBlobInDegree` e a metade cursor-page do `CasOrphanManifestSweep`; 19 ProfileEvents que só a rodada antiga incrementava.
- Settings que só ela lia: `gc_round_*_budget` ×8, `gc_read_concurrency`, `gc_bulk_delete_chunk_keys`, `manifest_sweep_*`, `gc_snapshot_generations_to_keep`.
- Comandos: `SYSTEM CAS GC REBUILD`, `clickhouse-disks cas-gc-rebuild`, `cas-gc-dryrun`.
- Testes: cerca de 22 arquivos de gtest inteiros e os casos da rodada antiga de cerca de 40 outros; os módulos de integração `test_cas_gc_sharded` e `test_cas_gc_bulk_delete`.
- O scheduler perdeu a thread de heartbeat do lease.
- **Fica:** `CasGc.h` com `RoundReport` e os tipos de linha de fase (usados por `system.cas_gc_log`), `sweepNamespace` (usado pelo decommission) e o fsck como scrubber manual.

No kayro-infra, `cas_gc_read_concurrency` já saiu de todos os papéis e da lista `tunable` (#229, #234).

## Pré-condições

- **Writer único por configuração.** Readers montam read-only, e o kayro-infra põe `replicated_can_become_leader=0` neles (#216).
- **A retenção compara dois relógios:** o relógio do writer contra o mtime do registro no Keeper.
- **`CheckNotExists` precisa da feature `CHECK_NOT_EXISTS` do Keeper** (produção 26.2.19 tem). Sem ela, o ClickHouse cai para create+remove de `<replica>/parts/<p>`, que falha numa réplica sem o filho `parts`: a remoção falha fechada, ou seja, vazamento, nunca perda.
- **Blobs que o pipeline antigo condenou, e blobs de partes removidas antes do #2** (sem registro), só são recuperados pelo fsck. Apagar os objetos do GC antigo no bucket (`gc/`, generations, outcomes) só com consentimento explícito do dono.
- **Nenhuma tabela pode estar detached no writer** quando o passo 2 subir. Conferir `system.detached_tables`; enquanto houver uma, o GC fica pausado.

## Riscos aceitos e lacunas conhecidas

1. **Uma query de reader mais longa que `old_parts_lifetime`** (480 s, padrão mantido por decisão do dono) pode falhar com objeto ausente se coincidir com a remoção adiada. O p99 dos readers fica abaixo de 15 s, com duas queries acima de 480 s em 30 dias. A query falha e pode ser repetida; o remédio é subir a setting.
2. **`DROP TABLE` de uma tabela replicada apaga os registros junto com o caminho no Keeper,** então os blobs que só esses registros referenciavam **vazam** (nunca se perdem). `DROP PARTITION` e merges recuperam normalmente.
3. **Uma MergeTree não replicada no disco CAS, ou uma tabela detached, pausa a deleção nesse disco.** O 3c recusa MergeTree não replicada em CAS.
4. **Não protegidos no passo 2** (a fechar no 3a/3b): partes detached e tabelas do pool que o writer não hospeda.
5. **A parte vazia local que o DROP_RANGE cria** não tem registro, então o único blob dela (o hash de um arquivo vazio) nunca é recuperado. É um blob constante.
6. **Uma queda entre a remoção de um registro e o delete dos blobs dele vaza esses blobs.** Eles ficam com `.meta = Condemned` e sem registro, para o scrubber.
7. **Linhas `Removing` de namespace no catálogo não são mais finalizadas** (o drain anterior ao fold sumiu). O decommission de um membro do pool não consegue remover o slot dele, e um namespace não pode ser recriado sob o mesmo UUID de tabela depois de um DROP. DROP/CREATE normal usa UUID novo.
8. **A metadata S3 do writer de partes removidas** (manifests, ref logs) não é mais recuperada e acumula até o 3c parar de gravá-la. São objetos pequenos, sem custo de requisição.
9. **A superfície de relatório mudou.** `system.cas_gc_log`, o resultado de `SYSTEM CAS GC RUN` e os gauges de saúde do GC trazem só o que a rodada produz: `writer, round, condemned, deleted, absent, replaced, spared, fence_outs`. `outcome` perde `NotALeader`/`Deferred`, e `gc_id` some. O ClickHouse renomeia a tabela de log antiga sozinho.

## Medições

- **Custo de parse por rodada**, sintético, no formato de produção (3.700 registros × 117 arquivos com checksum): 432.900 entradas de blob derivadas em **186 ms** num core do builder. Registros de produção têm cerca de 9,6 KB (35 MB no total, medido no #2).
- **Keeper por rodada:** um multi-read em lote de todos os registros, um `getChildren` por réplica, um `set` por registro recém-desprotegido e um multi-op por registro removível.
- **S3 por rodada:** 1 LIST do heartbeat floor, 1 HEAD por blob condenado ou apagado, e o marcador de condenação mais o delete exato por blob morto. Readers fazem 0 requisições.

## Portão em `023e6c4`

Builder `tec850`, RelWithDebInfo sem LTO, `ENABLE_TESTS=ON`. Árvore idêntica à de `92d2d5d` (o commit testado), menos o teste apagado `05024`.

| Portão | Resultado |
|---|---|
| Gtests CAS (`CAS*`) | **2.082 passaram, 0 falharam**. Novos: `CASKeeperGc.*` ×5 (`KeeperGc::runRound` contra uma tabela falsa: marca e condena, remoção e delete exato, remoção parcial, corte de build, poupar) e `CASKeeperRecordRemoval.*` ×3 (o multi-op no TestKeeper) |
| `test_cas_reader_pin` | **3/3**: DROP e REPLACE PARTITION durante uma query de 45 s no reader, e kill -9 do writer |
| `test_cas_gc_s3` | **passou** |
| `test_cas_replicated_relink` | **passou** (19/19) |
| `test_cas_gcs_relink_liveness` | **passou** |
| `test_cas_keeper_part_manifest` | **passou** |
| `test_cas_ref_snaplog`, `test_cas_shared_pool`, `test_cas_drop_pool_member` (portados) | **passaram** (1/1, 2/2, 2/2) |
| Stateless CAS (`04290`, `04295`, `05007`, `05010`, `05011`, `05020`, `05023`) | **7/7 OK** |
| Harness fault/RLS (`test_cas_load_recovery`) numa imagem do binário de dev | **HARNESS_OK** |

**Teste apagado:** `05024_cas_freeze_two_roots` recria uma tabela sob o mesmo UUID depois de um DROP. É a lacuna aceita 7, e o caso de FREEZE entre roots não pode ser montado sem isso.

**Linha de base no binário do #2** (só o passo 1, GC antigo): `test_cas_reader_pin[drop]` **falhou**. O GC antigo apagou 12 blobs que as partes do reader, anexadas pelo registro, serviam no meio da query. É a lacuna que este passo fecha.

**O que este portão não cobre:** o build ThinLTO de release, a paridade de features e a imagem no ECR. Isso entra na imagem de release (digest `TODO`).

## Imagem de release

Digest: `TODO`.

Imagens que cada nó roda antes deste passo: writer no passo 1 (`sha256:250b6ed91ef1b83dedaa8e74efaf0222e344f342404827a9e03fdeef45dacb58`), readers no RC3 (`sha256:6469a0c1da2508ba2d01cd04c6870861f94df8f80a68e71f7f6751f1eac6acfa`). Procedimento de rollback depois que o writer rodar o GC novo: não registrado.
