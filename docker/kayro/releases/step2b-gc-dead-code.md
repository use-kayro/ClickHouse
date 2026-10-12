# Passo 2b: código morto do GC antigo e remoção de parte segurada pelo Keeper

Imagem do [passo 2](step2-keeper-gc.md) mais dois PRs que vieram depois dele, **sem mudança de protocolo**:
- **PR #6** apaga o que só a rodada antiga do GC (a que listava o S3) alimentava: formatos que nada mais escreve, a setting `cas_gc_shards`, colunas e tipos de evento que nada mais emite.
- **PR #8** corrige o `NOT_IMPLEMENTED` que o reader compartilhado logava ao remover partes antigas anexadas pelo registro no Keeper.

Algumas remoções do PR #6 mudam o que o operador vê e o que sobe (abaixo). A principal: **uma config com `<cas_gc_shards>` deixa de subir**, e **um pool criado por esta build não abre numa build mais antiga**.

| Campo | Valor |
|---|---|
| Commit | `a6355a9986b8c9caaef7829a8873168e89859c2a` (merge do PR #6 em `kayro/26.6-cas-load-recovery`, 12/10 04:33:12Z) |
| PR #6 | https://github.com/use-kayro/ClickHouse/pull/6 (branch `kayro/keeper-gc-dead`), head `cc9ed8dc`; commits `84a60480` e `cc9ed8dc` (revisão `/enxuto`) |
| PR #8 | https://github.com/use-kayro/ClickHouse/pull/8 (branch `kayro/record-part-removal`), merge `7bcf86c`, 12/10 04:14:44Z; C++ em `fe7ed58`, testes em `7dccdf0` |
| Base | passo 2 (merge `59caa36`, código `023e6c4`) |
| Imagem | `TODO` |
| Linear | TEC-850 (PR #6), TEC-849 (PR #8) |
| Em produção | nenhum nó |

## Regra de deploy

**Qualquer ordem, um nó por vez.** Os três nós já rodam o passo 2, que tem o mesmo protocolo, então não há a sequência obrigatória do passo 2.

Antes, conferir que o `storage.xml` renderizado de nenhum papel tem `<cas_gc_shards>`: com esta imagem, o servidor não sobe com essa setting.

**Rollback:** a imagem do passo 2, `sha256:9347ff1ccd9a2a3f7ff242055cf29254e4a950f82a339f3c1fefd31e142d8a3e`. Os pools de produção já existem e abrem nas duas imagens; só um pool **criado** por esta build deixaria de abrir no passo 2.

## PR #6: o que foi apagado

139 arquivos, +738 / −10.310 linhas (`git diff f43ce2a a6355a9`).

1. **Formatos que nada escreve.** Aposentados: `gc/state` (5), `cas_run` (13), `cas_fold_seal` (14), `cas_gc_hb` (22). Saem os codecs (`CasGcStateFormat`, `CasFoldSealFormat`, `CasRecordStreamFormat`), as chaves `gc/state`, `gc/hb` e `gc/gen/*`, os renderers de inspect delas, `TextFamily::RecordStream`, `CompressionPolicy::PinnedRaw`, a convenção `object_cap == 0`, `CasByteBudget.h`, a forma em bytes do dialeto, `CasJsonWriter::clear`, `Pool::currentGcRound`, os benchmarks de run e fold-seal, e os gtests e helpers deles.
2. **`Gc/CasBlobInDegree.*`** (`zeroInDegree`, `GcRoundWorkBudget`, codec de source-edge, linhas condenadas); `Pool/CasKeyReader.*` e os parâmetros de read-ahead da recovery. No sweep: `work_budget`, `recovery_incomplete`, `listed_logs`, `SweepRetainClass`, `retain_class`, `CASGCEnumerationPages`; `warnings` vira referência obrigatória.
3. **`CasRefCatalog::deleteCompletedRemoving*`** e seus testes; `beginRemoving` perde `removal_started_round` e grava 0 (o campo continua na linha: formato Strict).
4. **`CasRefProtocol`:** `removalTxnId`, `groupRefKeys`, `planRefCleanup`, `RefTableListing`, `RefCleanupPlan`, `manifestEdgesOfTxn`, `crossEpochFromSeal`.
5. **Predicado de admissão (2) do catálogo** (reserva de fold-seal) e **`cas_gc_shards` inteiro:** setting, validação, ligação, campos de `PoolConfig`/`RefLedgerConfig`, parâmetro de `createNamespace`. `_pool_meta` deixa de gravar e de exigir `gc_shards`.
6. `ProfileEvents::CASGCNamespaceCleanupLeaks`, `ThreadName::CAS_GC_HEARTBEAT` e **15 tipos de evento** que nada emite desde o #4: `BlobRetire`, `BlobRetireReplaced`, `BlobDelete`, `ManifestDelete`, `RootAdd`, `RootRemove`, `IndegZero`, `GcFoldBegin`, `GcFoldEnd`, `GcRetireObserve`, `GcRecheckVerdict`, `GcFence`, `GcRebuild`, `GcFoldClamp`, `GcFenceOut`. Também `CasEvent::round`/`gen` e as colunas `round`/`generation` de `system.cas_log`.
7. **fsck, sweep e inspect** deixam de ler `gc/state` → fold seal → runs. No fsck: `PendingGc`, `AwaitingGc`, `StaleEdge` e `CorruptedRun` saem; todo blob sem referência é `Unaccounted`; nenhum `.meta` é lido; `awaiting_gc`, `stale_edge` e `corrupted_runs` saem de `FsckReport`, de `kFsckHardFindings` (5 → 3), da linha de resumo e da linha de `SYSTEM CAS FSCK`. `pending_gc` mantém o nome e vale 0.
8. **Docs** do CAS reescritas ou corrigidas (`garbage-collection.md` refeito a partir de `CasKeeperGc.h`, `cas_gc_log.md` com as fases `keeper_records`/`heartbeat_floor`/`mark`/`remove`, specs TLA de lease/fold marcadas como histórico). O teste stateless `05011_cas_gc_rebuild_access` vira `05011_cas_gc_run_access`.

### Mudanças que o operador vê

- **Config com `<cas_gc_shards>` não sobe.** A setting não existe mais. O kayro-infra não a contém (`git grep gc_shards` no `main` e no `origin/main` sem resultado, 12/10). Se algum nó a tem no `server.xml` por fora do template: não registrado; conferir antes do deploy.
- **`_pool_meta`:** um pool criado por esta build não tem a chave `gc_shards`, então **não abre numa build mais antiga** (que exige a chave). Pools existentes abrem aqui (o formato Tolerant ignora a chave).
- **`SYSTEM CAS FSCK`** perde as colunas `awaiting_gc`, `stale_edge` e `corrupted_runs`; a linha de resumo do fsck perde os mesmos três tokens.
- **`system.cas_log`** perde as colunas `round` e `generation`; os 15 tipos de evento acima somem.
- **O drain do decommission volta a apagar.** `sweepNamespace` largou a premissa baseada em selo e a proteção de cauda (as duas só existiam para o fold): o decommission apaga os corpos de manifest sem dono de builds aposentados, inclusive os manifests de uma tabela removida. A mensagem "upcoming GC rounds perform the final cleanup" agora diz que a linha `Removing` fica.

### Cobertura perdida

- Os quatro testes de admissão do predicado 2 (reserva de fold-seal e suas recusas de capacidade) saíram com ele; o predicado 1 (`checkCatalogObjectBytes`) mantém seu teste unitário. Nenhum teste alcança mais uma recusa de capacidade do catálogo (precisaria de um catálogo de 256 MiB).
- Os testes de renascimento com o mesmo nome (`ns_file_read_contract`, `ref_read_contract`) saíram: nenhum caminho de produção apaga uma linha `Removing`.

### Lacuna conhecida

Desde o #4 nada drena as linhas `Removing` do catálogo: o decommission nunca aposenta o slot de um membro com namespaces removidos, e um namespace removido não renasce com o mesmo nome. Os ref logs, snapshots, checkpoints e arquivos de namespace dele ficam no bucket. Apagar esses objetos só com consentimento explícito do dono.

### Portão do PR #6 (builder `tec850dead`, c7g.8xlarge, RelWithDebInfo sem LTO, `ENABLE_TESTS=ON`, em `cc9ed8dc`)

| Verificação | Resultado |
|---|---|
| `unit_tests_dbms --gtest_filter='CAS*'` | 1.946 passaram, 0 falharam |
| `test_cas_gc_s3` | 1 passou |
| `test_cas_replicated_relink` | 19 passaram |
| `test_cas_keeper_part_manifest` | 6 passaram |

Não rodados nesse portão: stateless `05020_cas_fsck` (referência atualizada) e `test_cas_drop_pool_member` (asserção de mensagem atualizada).

## PR #8: remoção de parte segurada pelo Keeper

**Problema.** Depois que o passo 2 foi para produção (12/10 03:00Z), o reader compartilhado logou `NOT_IMPLEMENTED` 161 vezes entre 03:25Z e 03:35Z:

> Cannot quickly remove directory …/delete_tmp_all_82_82_1 by removing files; fallback to recursive removal. Reason: … Cannot rewrite files of part …: its metadata is held from Keeper, and a changed Keeper-held part has nowhere to persist. (NOT_IMPLEMENTED)

`DataPartStorageOnDiskBase::clearDirectory` remove uma parte antiga em duas transações: `removeSharedFiles`, que desliga os arquivos, e depois `removeDirectory`. Numa parte que o reader anexou pelo registro (passo 1), a transação de desligar chega ao ramo de repoint de `publishStaging`, e a checagem do passo 1 que recusa escrita numa parte segurada a rejeitava. O fallback recursivo removia a parte: nada vazava, mas cada parte antiga anexada pelo registro custava uma linha de Error e um segundo caminho de remoção.

**Mudança.** `publishStaging` (`ContentAddressedTransaction.cpp`) trata remoção e escrita numa parte segurada de jeitos diferentes:
- **Só remoção** (sem build, sem entradas staged): segura o que sobra da parte, o manifest segurado menos os paths removidos, pelo `holdKeeperPart` que já existe. É o mesmo tratamento em memória que um rename (`republishRef`) ou um drop (`dropRef*`) de parte segurada já recebe.
- Depois, `removeDirectory` acha o diretório vazio e esquece a parte, como antes.
- **Escritas** continuam recusadas com `NOT_IMPLEMENTED`.

Nenhuma flag, estado ou ramo novo além do `isKeeperPart` que já existe.

**Testes.**
- gtest `CASKeeperPart.FastRemovalOfHeldPart`: repete a sequência do `clearDirectory` numa parte segurada; sem exceção; a parte segurada encolhe e depois é esquecida; zero escritas e LISTs no object store.
- `CASKeeperPart.WriteIntoHeldPartIsRefused` continua passando.
- Integração `test_cas_keeper_part_manifest::test_reader_removes_outdated_record_parts`: o reader anexa partes pelos registros; `OPTIMIZE FINAL` no writer as torna antigas; a limpeza do reader as remove sem "Cannot quickly remove" nem `NOT_IMPLEMENTED` no log; depois `system.parts` só tem as partes ativas e o disco só serve os diretórios delas (`system.remote_data_paths`).

### Portão do PR #8 (builder `tec849c`, RelWithDebInfo sem LTO, `ENABLE_TESTS=ON`; C++ em `fe7ed58`, testes em `7dccdf0`)

| Verificação | Resultado |
|---|---|
| Build | BUILD_OK |
| `unit_tests_dbms --gtest_filter='CAS*'` | RC=0; `FastRemovalOfHeldPart` e `WriteIntoHeldPartIsRefused` passaram |
| `test_cas_keeper_part_manifest` | 6 passaram (a primeira rodada falhou só no filtro de diretórios do teste novo, que contava o `format_version.txt` da tabela; corrigido em `7dccdf0`) |
| `test_cas_reader_pin` | 3 passaram |
| `test_cas_shared_pool` | 2 passaram |
| `test_cas_gc_s3` | 1 passou |
| Checagem negativa | só com a mudança de `ContentAddressedTransaction.cpp` revertida, `test_reader_removes_outdated_record_parts` falha na linha "Cannot quickly remove directory …" do reader |

Não rodado: o harness de fault (`test_cas_load_recovery`), porque outro laboratório ocupava o builder. O PR #8 não teve imagem de release própria.

O `NO_REPLICA_HAS_PART` do reader compartilhado entre 03:09Z e 03:27Z não é deste PR: é a corrida esperada entre o log de replicação e os merges de ~0,5 s do writer (ver o [passo 2](step2-keeper-gc.md), "Observado após o deploy").

## O que nenhum dos dois portões cobre

O build ThinLTO de release, a paridade de features, o harness fault/RLS na imagem e a imagem no ECR. Isso entra na imagem de release.

## Imagem de release

Digest: `TODO`.
