# Passo 1: registro da parte CAS no Keeper

A metadata de uma parte num disco CAS passa a existir também no Keeper, num registro por parte. Uma réplica consegue anexar a parte a partir desse registro e dos blobs do pool, sem buscar nada do writer e sem escrever ou listar no S3. O writer continua gravando toda a metadata S3 de hoje: nada sai do caminho S3 neste passo.

**A imagem de release passou no portão (gate verde 12/10 00:46Z).** Ela foi construída do head do PR #2, `283ca096`, que corrige um bug P0 e uma lacuna achados depois do portão de `1df4e77` (abaixo). O PR continua aberto.

3a (writer only) iniciado 12/10 ~01:10Z; readers permanecem em RC3.

| Campo | Valor |
|---|---|
| PR | https://github.com/use-kayro/ClickHouse/pull/2, aberto, não mergeado |
| Branch | `kayro/keeper-part-manifest` |
| Head | `283ca09658a4c0e405106342362c0c3b864ed4b8` (11/10 22:17Z), sobre o RC3 (`9700e6e`) |
| Commit com portão verde | `1df4e77c7357a2bd25475fa574a3713d1b74e5a5`, sobre o RC2 (`294ea1e`), **sem** o RC3 |
| Imagem | `474668394551.dkr.ecr.us-east-2.amazonaws.com/kayro/clickhouse@sha256:250b6ed91ef1b83dedaa8e74efaf0222e344f342404827a9e03fdeef45dacb58`, de `283ca096`. Gate verde 12/10 00:46Z (início exato do build não registrado) |
| Linear | TEC-849 (In Review); perguntas e decisões cruzadas em TEC-850 e TEC-851 |
| Em produção | writer: 3a iniciado 12/10 ~01:10Z. Readers no RC3 |

Os quatro primeiros commits são o patch do portão rebaseado sobre o RC3: `git diff 294ea1e 1df4e77` e `git diff 9700e6e 33ae72d4` dão o mesmo `git patch-id --stable` (`6672380d…`), 33 arquivos, +1851/−136. O quinto, `283ca096`, veio depois desse portão; a imagem de release de `283ca096` (RC3 + passo 1) passou no portão próprio em 12/10 00:46Z.

| Commit (head) | Antes do rebase (base RC2) | O que faz |
|---|---|---|
| `bff2ea7d` | `19395ca6` | registro no Keeper; réplicas anexam pela parte do registro |
| `93f08d59` | `2b4d64b7` | revisão `/enxuto`: o registro é o manifest publicado da parte |
| `635347f7` | `5b80492b` | `Fetcher` lê o registro pelo cliente Keeper da tabela, pelo nome |
| `33ae72d4` | `1df4e77c` | commit de fetch só confere o registro, nunca cria |
| `283ca096` | não existe (portão próprio verde 12/10 00:46Z) | correções do P0 do REPLACE e da lacuna do relink; produtores com create-if-absent (abaixo) |

## Regra de produção

**Esta imagem só vai para o writer até o passo 2 (PR #4, TEC-850) entrar em produção. Os readers ficam no RC3, `sha256:6469a0c1da2508ba2d01cd04c6870861f94df8f80a68e71f7f6751f1eac6acfa`.**

Um reader com esta imagem anexa partes pelo registro. Essas partes não têm ref no ref table S3 de nenhum nó, e o GC atual do pool (RC2/RC3) não as enxerga: ele apaga os blobs que a query do reader ainda lê. O `test_cas_reader_pin [drop]` contra o binário do passo 1 reproduziu isso: "GC deleted 12 blobs the reader's query still reads" (TEC-850, 11/10 22:00Z). O teste de integração do próprio passo 1 roda com GC desligado por esse motivo.

No writer, o passo 1 é o sub-passo **3a** do [ROADMAP](ROADMAP.md): escrita dupla (S3 como hoje, mais o registro) e backfill no boot.

## O registro

### Onde fica

Uma cópia por parte, no nível da tabela:

```
<zookeeper_path>/cas_parts/<part_name>
```

- O pai `cas_parts` é criado por `createNewZooKeeperNodes`, só para tabelas cuja policy tem disco CAS.
- O registro **não** é filho de `/replicas/<r>/parts/<p>`. A decisão de 11/10 na TEC-849 previa um filho do znode da parte; o PR mudou para uma cópia no nível da tabela porque ela sobrevive à remoção da parte no writer (o passo 2 a usa como fila de delete), ocupa metade do Keeper e não mexe nos znodes de parte nem no código que os remove.

### Formato `cas_part_files` v1 (congelado)

Documentado em `Formats/README.md` e `CasPartManifestFormat.h`:

```
{"type":"cas_part_files","v":1}
{"pool":"<32 hex pool_id>"}
{"path":"<file>","place":"inline","size":<n>}      uma linha por entrada Inline, em ordem canônica de path
{"n":<count>}
==> "<file>" size=<n> <==\n<n bytes crus>\n          zona de payload, um bloco por entrada
```

- O registro carrega **exatamente as entradas Inline do manifest publicado da parte**: os arquivos cujos bytes só existem dentro do manifest, como `checksums.txt`, `columns.txt`, `count.txt`, `serialization.json`, `primary.cidx`, `partition.dat`, minmax e `data.cmrk4`. Arquivos de projeção aparecem como `p.proj/...`.
- Blobs não são repetidos. O reader os deriva do `checksums.txt` inline: toda entrada que o registro não carrega é um blob de `file_size` bytes com chave `ch128` = o `file_hash` do MergeTree. Um gtest fixa `(high64<<64)|low64 == poolContentHash(ch128)`, inclusive para arquivos de vários blocos.
- O registro é cru (não comprimido); só o `checksums.txt` v4 dentro dele é LZ4.
- Codec: `encodePartFiles`/`decodePartFiles`, reaproveitando o código de entradas e zona de payload do manifest.
- O registro é imutável por nome de parte e não tem campo de epoch.
- **Decisão de 11/10 (opção 2):** `statistics*` entra em `partFileMustStayBlob` e vira blob como os dados de coluna, nunca inline. As alternativas eram 190 MB com statistics inline (rejeitada pelo dimensionamento do Keeper) e tudo como blob, com ~10 PUT a mais por parte (rejeitada pelo custo).

Mudar o formato depois do 3a exige reescrever todos os registros numa passada.

### Tamanho

| Fonte | Média | p50 | p99 | Máx | Total |
|---|---:|---:|---:|---:|---:|
| Laboratório (5.000 linhas, 4 colunas + projeção + skip index) | 4,6 KB | | | 4,8 KB | |
| Estimativa de produção: `checksums.txt` e inline reais dos 3.663 manifests do writer, com este codec e `statistics*` como blob | 9,6 KB | 8,7 KB | 32 KB | 265 KB | **35 MB**, uma cópia |

### Quem cria

Só a réplica que **produziu** a parte, no mesmo multi-op do Keeper que cria o znode da parte:
- insert (inclusive ATTACH de uma parte detached local);
- merge ou mutação local;
- clone de REPLACE;
- o backfill de boot, para partes anteriores aos registros (inclusive partes legadas recebidas por relink).

O registro é montado a partir do manifest publicado (`encodeKeeperPartRecord(part_path, checksummed_files)`), nunca reaplicando a regra de placement. Ele é recusado (`nullopt`, a parte vai pelo fetch comum) a menos que todo arquivo do `checksums.txt` fora do registro seja um Blob do manifest com `ref == {ch128, file_hash}` e `blob_size == file_size`, e o manifest não tenha outro Blob. Assim o registro é correto para partes de antes do roteamento de `statistics*`, clones por hardlink (mutações, REPLACE/ATTACH PARTITION FROM), partes de relink e de backfill. Pools cujo algoritmo de escrita não é `ch128` não geram registro.

A op do registro entra **depois** de `transaction.renameParts()`, que commita a transação de disco, no sink de insert, no REPLACE_RANGE e no REPLACE PARTITION. Merge, mutação, fetch e ATTACH_PART já renomeiam antes do commit. MOVE PARTITION TO TABLE commita a transação de disco depois do multi-op: essas partes ficam sem registro no commit (fetch comum) e o próximo boot faz o backfill.

### Invariantes

- **Uma cópia nunca cria o registro.** Até `1df4e77`: o commit de fetch confere (`Check(<zk>/cas_parts/<p>, -1)`) quando a parte foi anexada pelo registro ou o registro existe, e não acrescenta nada para uma parte legada buscada sem registro (`CasPartRecordCommit {Produced, Fetched}` passado até `checkPartChecksumsAndCommit`; o REPLACE_RANGE passa `Fetched` para partes buscadas e `Produced` para clones). Em `283ca096`, uma cópia sem registro passa a conferir que a réplica de origem ainda lista a parte (`copied_from_replica_path` explícito em todo commit). Assim uma cópia nunca ressuscita um registro que o GC do passo 2 removeu depois de apagar os objetos.
- **Imutável e nunca apagado no passo 1.** Não é reescrito, não sai com o znode de parte de nenhuma réplica nem quando a parte é mergeada. Os registros acumulam como a fila de delete do passo 2, que é o único removedor. `DROP TABLE` os apaga com a tabela.

## O que os readers fazem com ele

1. `Fetcher::fetchSelectedPart` → `attachPartFromKeeperRecord`, **só** para fetch para o conjunto ativo vindo de réplica da própria tabela: `!to_detached`, mesmo ZooKeeper, origem sob o `zookeeper_path` da própria tabela. Fetch para `detached/` e fetch de outra tabela ou de ZooKeeper auxiliar seguem o caminho de hoje.
2. `tryGet(<zookeeper_path>/cas_parts/<part>)`. Sem registro, o caminho de hoje (relink, bytes) roda sem mudança.
3. `holdPartFromCasRecord` decodifica, escolhe o disco CAS da tabela cujo pool o registro nomeia e deriva os blobs do `checksums.txt`. Registro malformado é logado e tratado como ausente. Parte que outro disco da policy já tem (TTL ou MOVE) não é segurada.
4. `holdKeeperPart` instala uma view em memória em `tmp-fetch_<part>`. Parte que o ref table do disco já tem nunca é sombreada.
5. Carga normal da parte tmp-fetch, `renameTempPartAndReplace` (re-key em memória) e commit.
6. O multi-op do commit confere que o registro ainda existe. É isso que vai serializar com a remoção do GC do passo 2.

A interface em `IContentAddressedExchange` tem quatro métodos: `encodeKeeperPartRecord`, `decodeKeeperPartRecord`, `holdKeeperPart`, `isHeldFromKeeper`. Partes seguradas pelo Keeper vivem em `CachedPartFolderAccess`; `getView`, `resolve` e `existsRef` as consultam primeiro; uma escrita avulsa numa delas é recusada (`NOT_IMPLEMENTED`). Probes de diretório `<part>/<file>` vão para a view da parte, o que tira o LIST por arquivo do `checkSize` no attach.

**Restart do reader:** o construtor do `StorageReplicatedMergeTree` lê, num multi-read, os registros de todas as partes listadas em `/replicas/<eu>/parts` e os segura antes de `loadDataParts` (`holdPartsFromCasRecords`). A tabela é dona do cliente Keeper; o disco nunca fala com o Keeper.

## Backfill no boot (migração 3a)

Na attach thread, logo depois de `createNewZooKeeperNodes` criar `cas_parts` (necessário para tabela criada por build antigo), toda parte ativa num disco CAS cujo registro um `exists` em lote diz ausente ganha o registro (`backfillCasPartRecords`): os mesmos bytes de um commit (`makeCasPartRecord`), a partir das views que o load já leu; creates assíncronos com `ZNODEEXISTS` tolerado; idempotente; 0 PUT e 0 LIST.

## Revisão `/enxuto` (comentário de 11/10 20:51Z no PR #2)

| Achado | Correção |
|---|---|
| A1 | O registro reaplicava a regra de placement em vez de ler o manifest; quebrava clones por hardlink, partes de relink e de backfill com `statistics*` inline. Agora é o conjunto de entradas Inline do manifest publicado, com as checagens acima. Saíram `isKeeperRecordFile`, `collectRecordFiles`, a releitura de bytes, o guard de `writeAlgo()` e `partFileIsInline`. Gtest novo `CASKeeperPart.RecordIsTheManifestInlineEntries` |
| A2 | Registro usado só em fetch para o conjunto ativo da própria tabela |
| M1 | Backfill movido para a attach thread, depois de `createNewZooKeeperNodes` (no construtor escrevia 0 registros no ATTACH de tabela existente) |
| M2 | Registro malformado vira "sem registro", com log, em vez de `CORRUPTED_DATA` |
| M3 | Parte que outro disco da policy tem não é segurada duas vezes |
| B1 | O commit usa o cliente Keeper com fault injection em todos os sítios. O retry por `ZNODEEXISTS` era falso positivo |
| S2 | Achado do agente do passo 2: commit de fetch criava registro ausente e podia ressuscitar um registro removido pelo GC. Corrigido em `1df4e77`: fetch só confere |
| C1–C12 | Limpezas: unions especulativos de `moving/`, máquina de evidência do teste, `sleep`, asserts de `numChildren`, helpers únicos (`casPartRecordsPath`, `casRecordDisks`, `sortedUniqueEntries`), comentários |

## Portão (builder novo, workspace `tec849b`, `c7g.8xlarge`, `RelWithDebInfo` sem LTO com `ENABLE_TESTS=ON`)

Rodada final em `1df4e77` (fonte às 20:19:01Z, `DONE` às 20:41:21Z), com `2b4d64b` e `5b80492` antes:

| Item | Resultado |
|---|---|
| Gtests CAS | **2.537 passaram, 0 falharam** (2 desabilitados). Novos: `CASFormatBattery.PartFiles`, `CASPartFilesFormat.*`, `CASKeeperPart.*`, `CASInlinePlacement.StatisticsStayBlob` |
| `test_cas_keeper_part_manifest` (novo) | **4/4**: attach wide e compact; restart do reader; backfill depois de apagar o pai `cas_parts`, com o relink do reader sem criar registro |
| `test_cas_gc_s3`, `test_cas_gcs_relink_liveness`, `test_cas_gc_sharded`, `test_cas_insert_fault_recovery`, `test_cas_lazy_load_recovery` | passaram (1/1 cada) |
| `test_cas_replicated_relink` | 18/19 em `1df4e77`, 19/19 em `5b80492`. A falha (`test_offer_without_pool_cookie_resolves_to_single_advertised_pool`, "promote: manifest body absent") é uma corrida GC × precommit do relink que também aparece na base: `294ea1e` limpo em 2 de 3 rodadas, este branch em 2 de 6. Relink e GC não foram tocados |
| Harness fault/RLS | `HARNESS_OK`, numa imagem feita com o binário de dev, não com o build ThinLTO de release |

Números do laboratório:
- Attach no reader: 24 de 24 partes (wide e compact), 0 `S3PutObject`, 0 `S3ListObjects`, 0 multipart/copy/delete; cerca de 3 ms por parte. O writer não registrou nenhum envio de parte.
- Restart do reader com N partes: os mesmos 5 LIST de um boot com 0 partes (2 do probe `_probe/` do mount, 3 do `_files/` da tabela, TEC-845); nenhum LIST de manifest, blob, stream ou parte.

Custo dos builders do passo 1 em 11/10: cerca de US$ 5 (3,7 h) na primeira rodada e US$ 2 (1,7 h) na final. Evidência na máquina do operador: `/tmp/cost-plan-600-20261010/tec-849/` (`ev2/` é a rodada final).

**O que este portão de `1df4e77` não cobre:** o build ThinLTO de release, a imagem no ECR e a combinação com o RC3. Isso foi coberto pelo portão da imagem de release de `283ca096` (seção "Imagem de release").

## Achados depois do portão de `1df4e77`, corrigidos em `283ca096`

1. **Bug P0 no REPLACE PARTITION** (achado pelo agente do passo 2 com `test_cas_reader_pin [replace]`, TEC-850, 11/10 22:00Z). `ALTER TABLE … REPLACE PARTITION … FROM …` no writer devolve `LOGICAL_ERROR: Unexpected name of log node: 1_4_4_0` em `tryWaitForReplicaToProcessLogEntry`, **depois** de o multi commitar. Causa: `replacePartitionFrom` lê o nó de log em `op_results.back()` (`StorageReplicatedMergeTree.cpp:10062`), mas o passo 1 acrescenta os creates de registro (`addCasPartRecordOps`, `:10019`) depois do `log/log-`. Correção mínima: guardar o índice do create de `log/log-` ou acrescentar os registros antes dele. Sem isso, o refresh REPLACE das 03–04Z falha no cliente com `alter_sync=1`, embora o dado tenha sido commitado. Em 11/10 22:04Z a decisão registrada foi corrigir no #2. `283ca096` lê a entrada de log pelo índice, não por `op_results.back()`, e acrescenta um teste de regressão de REPLACE PARTITION FROM em `test_cas_keeper_part_manifest`.
2. **Lacuna do relink** (mesma origem). O commit de uma cópia de parte sem registro (relink de parte legada, parte de MOVE PARTITION TO TABLE antes do backfill) não acrescenta check nenhum; um relink pode commitar uma parte cujo registro e blobs o GC do passo 2 já removeu. Proposta: com `how != Produced` e sem registro, `Check(<source_replica_path>/parts/<p>)`. Também decidido corrigir no #2; `283ca096` faz essa checagem.
3. **Pontos fora do passo 1:** DETACH de parte segurada pelo Keeper só re-keya em memória e some no restart; `metadata_version.txt` vai na cópia do produtor (não testado à parte).

Outras mudanças de `283ca096`, conforme a mensagem do commit:
- a op do registro passa a nascer em `getCommitPartOps`/`checkPartChecksumsAndAddCommitOps`, junto com o znode da parte;
- **produtores usam create-if-absent do Keeper (`CREATE_IF_NOT_EXISTS`, exigido, como o `RefreshTask` já exige)**, sem o `exists()` por insert. Os módulos de integração CAS replicados fixam essa feature flag. Até `1df4e77` o passo 1 usava só `create` e `check`, porque o harness sorteia feature flags e um create-if-not-exists dentro do multi derrubou a sessão numa rodada;
- no restart, partes que algum disco já tem não são procuradas no Keeper (o writer não lê registros);
- entradas Inline de uma parte segurada são hasheadas com o algoritmo de escrita do pool.

Keeper de produção 26.2.19 anuncia `CREATE_IF_NOT_EXISTS`, `CHECK_NOT_EXISTS`, `MULTI_READ` e `CHECK_STAT` (verificado em 11/10, TEC-850).

## Canário 3a no writer (preparado, não executado)

Scripts somente leitura em `/tmp/cost-plan-600-20261010/tec-849/canary/` (`snapshot.sh`, `reader-health.sh`, `ssm.sh`), para rodar antes e depois do restart do writer. Linha de base no writer às 21:46Z de 11/10, ainda no RC2:
- 3.848 partes CAS ativas em 489 tabelas, 0 registros (`missing_records` = partes ativas em todas as 12 tabelas mais movimentadas);
- Keeper: 108.965 znodes, 34,7 MB, 309 MiB de 2 GiB.

Aceite do 3a (TEC-851): 100 % das partes ativas com `<zk>/cas_parts/<p>`; PUT e LIST por classe iguais à linha de base (escrita dupla). Ponto sem volta: nenhum; rollback é a imagem anterior.

## Imagem de release

`build-image.sh 283ca09658a4c0e405106342362c0c3b864ed4b8` produziu `474668394551.dkr.ecr.us-east-2.amazonaws.com/kayro/clickhouse@sha256:250b6ed91ef1b83dedaa8e74efaf0222e344f342404827a9e03fdeef45dacb58`. Gate verde 12/10 00:46Z (início exato do build não registrado): gtests CAS 2.538 passaram / 0 falharam; harness fault/RLS na imagem de release passou; `test_cas_gc_s3` 2/2; `test_cas_keeper_part_manifest` 5/5 (inclui REPLACE PARTITION FROM); `test_cas_replicated_relink` 19/19; `test_cas_gcs_relink_liveness` 1/1; `test_cas_gc_sharded` 1/1; `test_cas_insert_fault_recovery` 1/1; `test_cas_lazy_load_recovery` 1/1.

3a (writer only) iniciado 12/10 ~01:10Z; readers permanecem em RC3.

Rollout: só no writer (`i-02e3809e0b416b25e`), com `configure-image.py` e a guarda de INSERT. Rollback: RC3 `sha256:6469a0c1da2508ba2d01cd04c6870861f94df8f80a68e71f7f6751f1eac6acfa`.
