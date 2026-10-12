# Imagens do ClickHouse da Kayro

Este diretório registra cada imagem do servidor ClickHouse que a Kayro constrói a partir deste fork: de que commit ela sai, que digest ela tem, que testes passou, onde roda e desde quando. Um arquivo por imagem, mais este índice e o [ROADMAP](ROADMAP.md).

Quem chega sem contexto deve ler, nesta ordem:
1. a tabela abaixo, para saber o que roda em produção agora;
2. o arquivo da imagem que interessa;
3. o [ROADMAP](ROADMAP.md), para saber o que vem depois e o que é proibido.

Tudo aqui é evidência registrada (Linear, PRs, releases, logs do builder). O que não tem registro aparece como "não registrado". Horários em UTC.

## O que é uma imagem

Uma imagem é sempre:

```
commit deste fork  →  scripts/clickhouse/build-image.sh (kayro-infra, no builder descartável)  →  ECR kayro/clickhouse@sha256:<digest>
```

- **Registro:** `474668394551.dkr.ecr.us-east-2.amazonaws.com/kayro/clickhouse`. O repositório ECR é declarado no kayro-infra (#184).
- **Base:** `altinity/clickhouse-server:26.6.4.20001.altinityantalya@sha256:4403168d63f049700a979876a8075a8ae4d7ae6be7ea7e09f4fbc6a526e420fa` (`docker/kayro/Dockerfile`). A imagem troca só o binário `/usr/bin/clickhouse` e confere o SHA-256 dele no build.
- **Labels OCI** (`docker/kayro/build.sh`): `org.opencontainers.image.revision` = commit compilado; `org.opencontainers.image.version` = a tag com que a imagem foi construída; `ai.usekayro.clickhouse.binary-sha256` = SHA-256 do binário.
- **A string de versão do servidor continua a da upstream** (`26.6.4.20001.altinityantalya`). `SELECT version()` não distingue RC2 de RC3. Identifique a imagem pelo digest (`docker inspect --format '{{.Config.Image}}' kayro-clickhouse` no nó) ou pelas labels.
- **Use sempre o digest**, nunca a tag. A tag é móvel.

### Nomes

- RC1 e RC2 têm tag git e release no GitHub: `v26.6.4.20001-kayro-cas1-rc1` e `v26.6.4.20001-kayro-cas1-rc2`. A tag da imagem é `26.6.4.20001-kayro-cas1-rcN`.
- A partir do RC3, a imagem sai do `build-image.sh` do kayro-infra, que usa como tag o próprio commit: `kayro/clickhouse:<commit de 40 hex>`. O RC3 não tem tag git nem release no GitHub; o nome "RC3" existe só nos registros (Linear TEC-843/844, PR #1, kayro-infra #229).
- Uma imagem que ainda não foi construída tem digest "pendente". Uma imagem construída que falhou no portão continua no ECR, mas fica marcada como **descartada** na tabela e nunca vai para um nó.

## Como construir

O procedimento manual está em [`docker/kayro/README.md`](../README.md) (toolchain, swap, `build.sh`, `compare-features.py`, push). Na prática, use o builder do kayro-infra, que automatiza esse procedimento e acrescenta os testes:

- kayro-infra `tools/clickhouse-builder/README.md`: criar o builder, rodar, acompanhar, coletar evidência, destruir.
- kayro-infra `scripts/clickhouse/build-image.sh <commit de 40 hex>`, rodando no builder. Passos, cada um com uma linha `== <hora> <passo>` em `/opt/clickhouse-builder/results/<commit>/build.log`:
  1. toolchain (Clang/LLVM/LLD 21.1.8, Rust `nightly-2026-03-22` com `rust-src`) e 64 GiB de swap só no builder;
  2. `docker/kayro/build.sh <registro>/kayro/clickhouse:<commit>` (RelWithDebInfo, ThinLTO, Rust ligado);
  3. `docker/kayro/compare-features.py` contra a base fixada (funções, formatos, engines, table functions, settings);
  4. harness fault/RLS `tests/integration/test_cas_load_recovery`;
  5. push no ECR e digest em `results/<commit>/digest`;
  6. gtests do CAS num diretório de build separado (`ENABLE_TESTS=ON` desliga o ThinLTO);
  7. `tests/integration/test_cas_gc_s3`;
  8. `== … BUILD_COMPLETE <imagem>@sha256:<digest>`.
- **Um workspace do OpenTofu por build** (`tofu workspace new <nome>` em `tools/clickhouse-builder/`, kayro-infra #226). Cada builder cria uma VPC; us-east-1 tem cota de 5.
- Custo registrado: cerca de US$ 3–5,5 por build completo (c7g.8xlarge, 2,7–4,4 h em 11/10).
- Copie a evidência (`digest`, `parity.json`, `release-validation.jsonl`, `cas-gtests.xml`/`.log`, `integration.log`, `build.log`) para a issue do Linear **antes** de destruir o builder. Destrua só a partir de `tools/clickhouse-builder/`, nunca de `stack/`.

**O portão de uma imagem** é o `BUILD_COMPLETE` com todos os passos verdes. Imagem pushada sem `BUILD_COMPLETE` (um passo falhou depois do push) não é candidata.

## Como fazer o rollout

O rollout é do kayro-infra, não deste repositório. Referência: kayro-infra `platforms/aws/modules/clickhouse-shared/README.md`, seção "Server image rollout", e o roteiro executado na TEC-844.

- **Ferramenta:** `scripts/clickhouse/configure-image.py` (kayro-infra #185; `--storage` desde o #222). Roda como root no nó, via SSM.
- **Um nó por vez, na ordem:** reader-mdb (`i-0bc5ccb44d4141059`) → reader compartilhado (`i-02369cef982eb67d2`) → writer (`i-02e3809e0b416b25e`).
- **Um restart por nó.** Cada restart custa 160–520 mil LIST no S3 nos primeiros minutos e pode derrubar o lease de outros membros por SlowDown (TEC-837, TEC-845). Se o storage renderizado do papel também mudou, ele vai no mesmo restart com `--storage`.
- **Comandos por nó** (arquivos copiados em base64 para `/var/tmp/<pasta>`: `configure-image.py`, `configure-filesystem-cache.py` e o storage renderizado do papel):

  ```sh
  python3 configure-image.py --image $IMG --storage storage.xml          # só valida; mostra "Image: <atual> -> <nova>"
  docker pull $IMG                                                        # pré-pull, para o restart cair no minuto marcado
  python3 configure-image.py --image $IMG --storage storage.xml --apply  # backup da unit e do server.xml, restart, health
  ```

  Render do storage: `templatefile("platforms/aws/modules/clickhouse-shared/storage.xml.tftpl", {role="<reader-mdb|reader|writer>", bucket_name="kayro-prod-clickhouse-474668394551-us-east-2", region="us-east-2", cache_size_bytes=21474836480})`.
- **Writer:** só com `system.processes` sem INSERT no momento do restart (o roteiro da TEC-844 exige 3 leituras seguidas, 2 s de intervalo, e aborta depois de 10 min sem janela).
- **Aceite por evidência, não por relógio.** O próximo nó só começa depois do aceite do anterior. `/ping` não prova nada. Confira: digest servido; nenhum loader falho ou cancelado; `system.cas_mounts` vivo; réplicas sem readonly, sem atraso e com fila drenando; nenhuma classe nova em `system.errors`; lease do writer intacto nos minutos seguintes (`CASMountLeaseLost`); dois usuários RLS diferentes. Registre cada passo no Linear.
- **Rollback = a imagem anterior, pelo digest**, com o mesmo comando. Falha de startup já restaura a unit sozinha; falha funcional pede o rollback explícito. Nunca apague nem recrie a identidade CAS, o namespace S3 ou o volume EBS.
- **Depois dos três nós:** um PR no kayro-infra atualiza `clickhouse_shared_image` em `config/kayro-prod.tfvars.json` para o novo digest, para nós novos nascerem nele. O Terraform não reinicia nada; esse valor é intenção, não prova do que roda.

## Regra dos readers

Um reader só recebe uma imagem cujo GC entende os registros de parte no Keeper, ou seja, **depois do passo 2** (TEC-850).

Motivo: a partir do [passo 1](step1-keeper-part-record.md), um reader pode anexar uma parte pelo registro no Keeper, sem ref no ref table S3. O GC atual (RC2/RC3) não enxerga essa parte e apaga os blobs dela. O `test_cas_reader_pin` contra o binário do passo 1 reproduziu isso: "GC deleted 12 blobs the reader's query still reads" (TEC-850, 11/10 22:00Z). Até o passo 2, a imagem do passo 1 só pode ir para o writer.

## Imagens

Prefixo de todos os digests: `474668394551.dkr.ecr.us-east-2.amazonaws.com/kayro/clickhouse@sha256:`.

| Imagem | Commit do fork | Digest | Construída | Portão | Onde roda, desde quando | Notas |
|---|---|---|---|---|---|---|
| Upstream Altinity | `2073b1f8` (tag `v26.6.4.20001.altinityantalya`) | `altinity/clickhouse-server:26.6.4.20001.altinityantalya@sha256:4403168d63f049700a979876a8075a8ae4d7ae6be7ea7e09f4fbc6a526e420fa` | não se aplica | não se aplica | rodou nos três nós até 07/10 | imagem anterior ao RC2 |
| [RC1](RC1.md) | `c2075023` | `85a2931647d92154dda525cadba64d1e957c4b6b02f0ccffc0d89fa01c5e545c` | 06/10 ~22:40Z | fault passou; **paridade rejeitou** (sem Rust) | nunca | **não usar** |
| [RC2](RC2.md) | compilado `fdc4d38f`; tag `294ea1e` | `ecb7ee0add4cdf9887baaffa9b20599e966eea1c91e754482acb2b1eda534718` | 07/10 00:41Z | paridade, fault/RLS | writer 07/10 01:40Z → 11/10 22:33Z; reader-mdb 07/10 01:35Z → 11/10 20:45Z; compartilhado 07/10 01:24Z → 11/10 21:55Z; nenhum nó desde então | rollback do RC3 |
| RC3, build de `da8927d` | `da8927da` | `2cc431e769c1491da5b8f1ca905488c646c1649df859c83140a37ef7bb2a67ea` | 11/10 14:14Z | **falhou**: gtests não compilam | nunca | **descartada** ([RC3](RC3.md)) |
| RC3, build de `260d77d8` | `260d77d8` | `e5c35d6012cbe5059a6cce32075e7c83f927daac28a77d2b7a1d71d7b37062d7` | 11/10 17:33Z | **falhou**: gtests não compilam | nunca | **descartada** ([RC3](RC3.md)) |
| [RC3](RC3.md) | `d2e59f1c` (merge `9700e6e`, mesma árvore) | `6469a0c1da2508ba2d01cd04c6870861f94df8f80a68e71f7f6751f1eac6acfa` | 11/10 20:05Z | verde: gtests CAS 2527/2527, `test_cas_gc_s3` 2/2, paridade, fault/RLS | os três nós: reader-mdb desde 11/10 20:45Z, compartilhado desde 21:55Z, writer desde 22:33Z; rollout encerrado às 22:43Z | imagem atual |
| [Passo 1](step1-keeper-part-record.md) | `283ca096` (PR #2, merge `b71585e`) | `250b6ed91ef1b83dedaa8e74efaf0222e344f342404827a9e03fdeef45dacb58` | gate verde 12/10 00:46Z | verde: gtests CAS 2.538 passaram / 0 falharam; harness fault/RLS na imagem de release passou; `test_cas_gc_s3` 2/2; `test_cas_keeper_part_manifest` 5/5 (inclui REPLACE PARTITION FROM); `test_cas_replicated_relink` 19/19; `test_cas_gcs_relink_liveness` 1/1; `test_cas_gc_sharded` 1/1; `test_cas_insert_fault_recovery` 1/1; `test_cas_lazy_load_recovery` 1/1 | writer: 3a iniciado 12/10 ~01:10Z; readers permanecem em RC3 | **só writer até o passo 2 (PR #4); readers ficam no RC3 `6469a0c1`** |
| [Passo 2](step2-keeper-gc.md) | `023e6c4` (PR #4, merge `59caa36`, 12/10 01:14Z) | `9347ff1ccd9a2a3f7ff242055cf29254e4a950f82a339f3c1fefd31e142d8a3e` | 12/10, push 02:52Z | código: gtests CAS 2.082/2.082, `test_cas_reader_pin` 3/3 e demais módulos verdes em `023e6c4`; imagem de release: paridade idêntica ao passo 1, fault/RLS passou | deploy iniciado 12/10 ~03:00Z, writer → reader-mdb → reader | **writer primeiro, depois os dois readers um logo após o outro; reader só depois do writer** |

## Como registrar uma imagem nova

Crie `<nome>.md` neste diretório e uma linha na tabela, no mesmo PR que muda o branch base ou logo depois do `BUILD_COMPLETE`. Campos obrigatórios:

- commit do fork, PR e issue do Linear;
- o que muda para quem opera (comportamento, não só código);
- digest e hora do `BUILD_COMPLETE`, ou "pendente";
- resultado de cada passo do portão;
- builds descartados e por quê;
- config do kayro-infra que acompanha a imagem;
- rollout: nó, hora do apply, aceite ou rollback, com link para o comentário do Linear;
- mudanças de comportamento conhecidas e o que não foi testado.

Atualize a coluna "Onde roda" a cada passo do rollout.
