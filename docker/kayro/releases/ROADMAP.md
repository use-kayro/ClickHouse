# Roadmap: metadata do CAS fora do S3 (milestone 6)

Este arquivo diz o que vem depois do [RC3](RC3.md) e do [passo 1](step1-keeper-part-record.md), o que já foi decidido e o que é proibido. Ele resume decisões registradas no Linear em 11/10/2026; o Linear é a fonte, e uma decisão nova lá vale mais que este texto. Atualize este arquivo no mesmo PR que muda um passo.

Projeto no Linear: [ClickHouse: readers de verdade e metadata fora do S3](https://linear.app/usekayro/project/clickhouse-readers-de-verdade-e-metadata-fora-do-s3-9e6d43e93b5c), milestone 6.

## Por quê

Hoje cada nó grava a metadata de cada parte como objetos pequenos no S3 (manifest, `.meta` por arquivo, ref logs, checkpoints, snapshots, leases), e o GC redescobre o estado listando e lendo esses objetos: cerca de 149 chaves por parte e ~86 requisições por INSERT ou merge. Custo residual estimado depois das correções de 11/10: ~US$ 7,6/dia, mais US$ 2–3 por restart (modelo, a remedir). O alvo, no modelo dos papers (Aurora, Neon, Delta): metadata da parte no Keeper, objetos de dado imutáveis por hash, GC derivado do Keeper, LIST só num scrubber manual. Estimativa: ~US$ 1,5–2/dia.

**Decisão do dono (11/10):** ficar no S3 compartilhado (EBS local por nó foi rejeitado, TEC-838) e levar o fork a esse modelo, podendo divergir da Altinity. Base continua 26.6 enquanto o porte do CAS para 26.8 (Altinity #2540) não fecha.

## Sequência

```
passo 1 (código, PR #2)  →  3a  →  2  →  3b  →  3c  →  3d
```

Cada passo é uma imagem com seu próprio arquivo neste diretório, rollout por `configure-image.py` (reader-mdb → reader → writer, ou só o writer quando indicado) e rollback pela imagem anterior. A exceção é o 3c, que é ponto sem volta para RC2/RC3.

| Passo | Linear | O que é | Onde roda | Estado em 11/10 ~22:20Z |
|---|---|---|---|---|
| 1 | TEC-849 | O writer grava um registro por parte em `<zk>/cas_parts/<p>`; uma réplica sabe anexar pela parte do registro; backfill no boot | código; vira a imagem do 3a | PR #2 mergeado (`b71585e`), portão verde em `1df4e77`; o bug P0 do REPLACE e a lacuna do relink foram corrigidos em `283ca096`, cuja imagem de release (`sha256:250b6ed9…`) passou no portão em 12/10 00:46Z. [Detalhe](step1-keeper-part-record.md) |
| 3a | TEC-851 | Imagem do passo 1 **só no writer**: escrita dupla (S3 como hoje + registro) e backfill de toda parte ativa | writer | linha de base do canário tirada às 21:46Z (3.848 partes, 0 registros). 3a (writer only) iniciado 12/10 ~01:10Z; readers permanecem em RC3 |
| 2 | TEC-850 | GC derivado dos registros no Keeper, sem LIST no caminho do GC | writer primeiro, depois os dois readers um logo após o outro | PR #4 mergeado em 12/10 01:14Z (`59caa36`, código `023e6c4`); portão do código verde; imagem de release `sha256:9347ff1c…` (push 02:52Z). Nos três nós desde 12/10 03:09:48Z; aceito nos três às 03:40Z. [Detalhe](step2-keeper-gc.md) |
| 2b | TEC-850, TEC-849 | Passo 2 + PR #6 (código morto do GC antigo) + PR #8 (remoção de arquivos de parte segurada pelo Keeper), sem mudança de protocolo | qualquer ordem, um nó por vez | PRs #6 e #8 mergeados (`a6355a99`); imagem de release `sha256:1912bde0…`; nos três nós desde 12/10 06:07:16Z. [Detalhe](step2b-gc-dead-code.md). 3c (PR #7) em rework após /enxuto, ordem de deploy readers primeiro ou writer com `cas_gc_enabled=0` |
| 2c | TEC-850 | O GC espera o startup das tabelas em vez de comparar com `getAllTableNames` (pausa do GC no writer de 12/10 07:21Z) | qualquer ordem, um nó por vez | PR #12 mergeado (`703cb49f`, head `1550abe8`); imagem `sha256:6dcd83ff…` com portão verde; deploy previsto ~17:10Z de 12/10. [Detalhe](step2c-gc-loaded-tables.md) |
| 3b | TEC-851 | Readers anexam pelo registro em produção | readers | depois do passo 2 |
| 3c | TEC-851 | Nenhum nó grava metadata no S3 | todos | decisões tomadas; branch `kayro/keeper-no-s3-metadata` só local, sem commits |
| 3d | TEC-851 | Apagar o código morto | todos | depois do 3c |

### Por que o 3a vem antes do passo 2

O GC do passo 2 decide o que está vivo pelos registros. Sem registro para toda parte ativa, o refcount fica incompleto. Por isso, também: **uma rodada do passo 2 não apaga nada enquanto houver parte listada sem registro**, e registra isso no log.

### Por que readers só depois do passo 2

Uma parte anexada pelo registro não tem ref no ref table S3, e o GC de hoje (RC2/RC3) apaga os blobs dela. Reproduzido: `test_cas_reader_pin [drop]` contra o binário do passo 1, "GC deleted 12 blobs the reader's query still reads" (TEC-850, 11/10 22:00Z).

## Decisões já tomadas

### Passo 1 (TEC-849, PR #2)

- Conteúdo do registro: as entradas Inline do manifest publicado da parte (`checksums.txt` e os arquivos pequenos que só existem dentro do manifest); blobs derivados do `checksums.txt` (`ch128` = `file_hash`). Formato `cas_part_files` v1, congelado.
- `statistics*` vira blob (`partFileMustStayBlob`), decisão de 11/10 ([parede](https://linear.app/usekayro/issue/TEC-849#comment-83aff1f0), [decisão](https://linear.app/usekayro/issue/TEC-849#comment-258a9223)).
- Uma cópia por parte no nível da tabela, não filho de `/replicas/<r>/parts/<p>`.
- Só quem produz a parte cria o registro; commit de fetch só confere. Registro imutável, nunca apagado no passo 1.

### Passo 2 (TEC-850)

Mapa do código: `tec-850-mapa-codigo-20261011.md` (resumo no comentário de 11/10 15:24Z da TEC-850).

- **Vivo** = referenciado pelo registro de alguma parte que alguma réplica ainda lista no Keeper. O registro do passo 1 é a fila de delete persistente. Sem marca d'água nova, sem fila nova, sem geração.
- **Parede 1, decisão revisada: remoção do registro em duas fases**, adiada pelo `old_parts_lifetime` que já existe (11/10, depois que generalizar o adiamento do pin quebrou o DROP_RANGE). Sem mudança no ReplicatedMergeTree, sem lock novo, sem campo novo no registro:
  1. rodada N: registro cuja parte nenhuma réplica lista recebe um `set` com os mesmos bytes, com checagem de versão; o `mtime` é a marca de condenado;
  2. rodada N+k: se a parte segue não listada **e** `now - mtime >= old_parts_lifetime`, multi-op com CHECK de que nenhuma réplica lista a parte, CHECK de `is_active`/`host` do writer e REMOVE do registro; depois delete por ETag exato dos hashes que nenhum registro vivo referencia;
  3. parte que volta a ser listada no meio só faz o CHECK falhar, e o ciclo recomeça.
- **`old_parts_lifetime`: fica no padrão de 480 s (decisão do dono, 11/10).** O dono aceita o risco: uma query de reader mais longa que 480 s pode falhar com objeto ausente se coincidir com a remoção adiada do GC. Ela falha e pode ser repetida, sem perda de dado. A proposta anterior de subir o valor para cobrir a query mais longa dos readers (560 s em 11/10, comentário de 22:04Z na TEC-850) foi descartada.
- **Rejeitado:** mudar os caminhos de remoção forçada do ReplicatedMergeTree (DROP/REPLACE/DETACH). Reproduzido no 26.6.4 oficial: com o nó da parte dropada adiado, uma queda ressuscita a partição dropada e as réplicas divergem; `cloneReplica` também ressuscita. Lock no padrão zero-copy (~430 escritas/min no Keeper) também rejeitado.
- **Mantido até o 3c:** a condenação `.meta=Condemned` em duas fases e o heartbeat floor (o dedup do writer adota objeto existente sem re-PUT, então o delete por ETag sozinho não protege parte em construção). O LIST do heartbeat floor (um por rodada) fica, documentado.
- Commit de fetch só confere o registro (já no #2). Partes detached e tabelas que o writer não hospeda ficam documentadas como desprotegidas no passo 2.
- Saem do CAS, onde ficarem mortos: eleição do líder do GC, read-ahead, fold dos ref logs, orphan sweep, janitor.
- Teste central: `tests/integration/test_cas_reader_pin` (DROP e REPLACE PARTITION durante uma query de 45 s no reader; variante com kill -9 do writer).
- Keeper de produção 26.2.19 anuncia `CREATE_IF_NOT_EXISTS`, `CHECK_NOT_EXISTS`, `MULTI_READ` e `CHECK_STAT` (verificado em 11/10).

### Passo 3 (TEC-851)

Mapa do código: `tec-851-mapa-codigo-20261011.md` (resumo no comentário de 11/10 17:01Z da TEC-851).

- **Fencing:** nada novo. O efêmero `is_active` e a versão do znode `host` da réplica (criados juntos em `activateReplica`, já checados no insert com quorum) são a epoch do writer. Sem epoch no registro, sem rejeição por epoch nos readers. O lease em S3 deixa de existir.
- **Migração sem ferramenta nova:** o backfill de boot do passo 1 (0 PUT, 0 LIST, idempotente).
- **3c, só o writer grava blobs** (decisão de 11/10 21:49Z): o writer é a réplica com `replicated_can_become_leader=1` (readers têm 0 desde o kayro-infra #216). Nas outras réplicas: `always_fetch_merged_part` passa a valer também para `MUTATE_PART`; INSERT em tabela CAS é recusado com erro claro; o fetch por bytes nunca faz upload. Invariante do GC: lê o ETag, confere `count == 0` sob o lock do refcount em memória, delete exato; a build soma o hash sob o lock e faz upload quando o count era 0.
- **3c, o que não pode ter registro é recusado** com `NOT_IMPLEMENTED` no disco CAS: FREEZE, MergeTree não replicada, `moving/` entre discos, DETACH PARTITION/PART. Medido no writer em 11/10: 0 partes detached no CAS; 496 tabelas em `shared_s3`, todas replicadas; 0 DETACH, ATTACH PART, FREEZE ou MOVE PART em 30 dias.
- `_files` em memória, com `format_version.txt` refeito no attach. Custo de boot do refcount a medir (estimativa ~330 mil entradas, ~25 MB).
- **Depois do 3c**, nenhum GC S3 (RC2/RC3) pode rodar no pool. Uma imagem antiga só sobe com o GC desligado (`cas_gc_enabled`, setting que já existe), e o rollback normal passa a ser a imagem do 3b.
- **3d** apaga: ref log/ckpt/snap/catálogo (~13 mil linhas), server root/lease/epoch (~6 mil), manifest S3, `.meta`, HEAD, relink, roteamento por arquivo (`classifyDirectory`, TEC-845). Ficam: layout por hash, cache local, delete em lote, delete por ETag exato e o codec do registro. O patch do RC3 e o da TEC-845 morrem junto com o código que corrigem.
- **Fora do escopo:** partes empacotadas (upstream #108118; TEC-852): não existem na base e brigam com o layout por conteúdo.

## Proibido

- **Nunca apagar nem expirar chave de `shared_pool/`** sem consentimento explícito do dono. Nenhum passo apaga as chaves antigas do pool, nem o 3c nem o 3d.
- **Nenhuma flag nem "modo antigo/novo".** Cada passo troca o caminho; a imagem anterior é o rollback. Nada de alias, fallback ou decoder duplo para manter o jeito antigo ao lado do novo.
- **Readers só depois do passo 2.** A imagem do passo 1 vai só para o writer.
- Nunca apagar nem recriar a identidade CAS, o namespace S3 ou o volume EBS ao trocar de imagem.
- Nunca mais de um restart por nó para a mesma mudança: imagem e storage vão juntos (`--storage`).
- Nunca usar uma imagem sem `BUILD_COMPLETE` (as descartadas estão na tabela do [README](README.md)).
- Nada de número inventado (timeout, TTL, janela, limite): use setting que já existe ou valor medido e documentado.
- Nunca rodar `tofu destroy` do builder a partir de `stack/`; só de `tools/clickhouse-builder/`, no workspace do builder.

## Onde estão os documentos de apoio

Os mapas e medições ficam na máquina do operador, em `/tmp/cost-plan-600-20261010/`, e o essencial de cada um está no Linear:

| Arquivo | Conteúdo | No Linear |
|---|---|---|
| `cas-desenho-alvo-20261011.md` | desenho alvo, revisado com as decisões dos passos 2 e 3 | TEC-838, comentário de 11/10 14:24Z (versão inicial) |
| `cas-anatomia-custo-20261011.md` | custo residual por operação × nó; como uma parte vira objetos S3 | TEC-838 |
| `tec-850-mapa-codigo-20261011.md` | mapa do código para o passo 2 | TEC-850, 11/10 15:24Z |
| `tec-851-mapa-codigo-20261011.md` | mapa do código para o passo 3, sub-passos e perguntas | TEC-851, 11/10 17:01Z |
| `tec-849/` | evidência do passo 1, script de tamanho do registro, scripts do canário 3a | TEC-849 |
| `rc3/`, `rc3b/` | evidência dos builds do RC3 | TEC-842, TEC-843 |

`/tmp` some no reboot: o que importa precisa estar no Linear ou neste diretório.
