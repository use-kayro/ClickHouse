# RC2: recuperação do carregamento de tabelas CAS ("cas-load-recovery")

Primeira imagem do fork em produção. É o patch do [RC1](RC1.md), agora com Rust, otimização de release e paridade completa com a imagem oficial. É o **rollback do RC3**.

| Campo | Valor |
|---|---|
| Tag git | `v26.6.4.20001-kayro-cas1-rc2` → `294ea1eded8ac5096713a4a6a56710a5c2933d21` (commit de documentação dos resultados) |
| Commit compilado | `fdc4d38f496de8d256299d4f38a5aa18bf786457` |
| Receita de empacotamento | `acd6157` (usa o artefato split-debug da upstream; nenhum código nativo mudou no reempacotamento) |
| Base | Altinity `v26.6.4.20001.altinityantalya`, `2073b1f88dec885444fac97edb8179ad1753c1ad` |
| Imagem | `474668394551.dkr.ecr.us-east-2.amazonaws.com/kayro/clickhouse@sha256:ecb7ee0add4cdf9887baaffa9b20599e966eea1c91e754482acb2b1eda534718` |
| Tag da imagem | `26.6.4.20001-kayro-cas1-rc2` |
| SHA-256 do binário | `b2c832061413dc1a37d7c6f24a955018d7fc1b4687d6aac8e71fbb3aba5e83d0` |
| Plataforma | linux/arm64 |
| Release | https://github.com/use-kayro/ClickHouse/releases/tag/v26.6.4.20001-kayro-cas1-rc2 (criada 07/10 00:41:32Z, publicada 00:43:45Z, pré-release), com `manifest.json`, `parity-rc2.json` e `release-validation.jsonl` |
| Linear | TEC-736 (Done 07/10 01:51Z) |
| Upstream | Altinity/ClickHouse#2498, base `antalya-26.6`, aberto em 07/10 com os seis arquivos C++ idênticos ao patch implantado. Estado atual: não registrado |

O branch `kayro/26.6-cas-load-recovery` em `294ea1e` é a base de todo o trabalho seguinte: o RC3 (PR #1) e o passo 1 (PR #2) partiram dele.

## O que muda para quem opera

Quando o CAS perde o lease de mount (por exemplo, S3 respondendo 503/SlowDown) e depois remonta, as tabelas cujo carregamento assíncrono falhou nesse intervalo voltam sozinhas, sem restart do servidor. Antes, elas ficavam `FAILED`/`CANCELED` e as consultas davam `ASYNC_LOAD_WAIT_FAILED` até alguém reiniciar o nó.

Só a exceção tipada de estado transitório do mount é repetida, e só na construção assíncrona de tabelas. Fencing, recusa terminal (`SYSTEM CAS FORGET` → `INVALID_STATE`) e erros genéricos de escrita ou convergência continuam falhando como antes. O patch não muda formato de dado.

**O patch não elimina a causa do throttling do S3.** Ele trata a recuperação.

## Commits

Sobre `2073b1f8`:
- `61fe33b7` retry no carregamento assíncrono (`AsyncLoader.h`, `DatabaseOrdinary.{cpp,h}`, `CasRequests.{cpp,h}`, `CasRefLedger.cpp`), harness `tests/integration/test_cas_load_recovery` e receita `docker/kayro`;
- `c2075023` (RC1), `d511e4fc` publicação no ECR;
- `fdc4d38f` **commit compilado**: Rust de volta, `RelWithDebInfo`, ThinLTO, sysroot ARM64 da upstream;
- `667b4244`, `773c5b6a`, `4547a8f0` comparação de capacidades e testes RLS com a mesma query em usuários diferentes, com e sem cache;
- `467d09f6` swap de 64 GiB só no builder;
- `acd61576` empacotar o artefato split-debug da upstream;
- `294ea1ed` resultados finais no README.

## Como foi construída

Manualmente, no laboratório AWS da TEC-736 (c7g.8xlarge, us-east-1, kayro-infra branch `test/cas-self-heal-lab`), seguindo [`docker/kayro/README.md`](../README.md). O builder do kayro-infra (`build-image.sh`) ainda não existia. Build otimizado de cerca de 1 h 45 min, com um restart do link antes do swap.

## Portão (TEC-736, 07/10 00:35–00:40Z, `release-validation.jsonl`)

- **Paridade com a imagem oficial:** 1.842 funções, 114 formatos, 81 engines, 91 table functions, 1.713 settings, iguais. DeltaLake e Wasmtime presentes. Só os metadados de build diferem.
- **Recuperação:** writer (26 falhas injetadas) e reader (28) recuperaram o carregamento sem restart, com `CASMountLeaseLost`=1 e `CASRemountSucceeded`=1. UUID da tabela preservado, linhas novas replicadas depois de cada recuperação.
- **RLS:** dois usuários com a mesma query viram linhas disjuntas e não vazias, antes e depois da recuperação, no writer e no reader. Alterar a row policy foi recusado. Cache de sistema de arquivos ligado no reader, cache de resultado desligado.
- **Recusa terminal** preservada: `INVALID_STATE`.
- **Shutdown durante a falha:** 3,87 s, exit 0.

Esses testes são sintéticos e isolados. Não qualificam carga de produção nem garantem nada contra quedas de storage ou rede.

## Config do kayro-infra que acompanha

- #184: repositório ECR `kayro/clickhouse`.
- #185 (`4e74db8`): `clickhouse_shared_image` com o digest, permissões ECR e o updater `scripts/clickhouse/configure-image.py` (um nó, com rollback automático se o startup falhar).
- #186 (`f29ca05`): os três security groups do ClickHouse alcançam o endpoint privado do ECR (só 3 ingress TCP 443).

## Rollout (TEC-736, 07/10)

Autorizado pelo dono: reader compartilhado primeiro, depois reader-mdb, writer por último. Imagem anterior (rollback naquela data): `altinity/clickhouse-server:26.6.4.20001.altinityantalya@sha256:4403168d63f049700a979876a8075a8ae4d7ae6be7ea7e09f4fbc6a526e420fa`.

| Nó | Início no RC2 | Resultado registrado |
|---|---|---|
| Reader compartilhado `i-02369cef982eb67d2` | 07/10 01:24:12Z | ~10 min de carregamento assíncrono, com timeouts de consulta nessa janela. Depois: 894 loaders OK, 325 réplicas sem atraso/fila/readonly, CAS live. 8 consultas MCP e 2 skills com hashes iguais ao baseline; RLS preservado |
| Reader-mdb `i-0bc5ccb44d4141059` | 07/10 01:35:56Z | 198 loaders OK, 56 réplicas saudáveis, CAS live. RLS dos usuários 1000/1097 com as mesmas contagens e hashes; usuários/policies/profiles 1108/554/4 |
| Writer `i-02e3809e0b416b25e` | 07/10 01:40:20Z | S3 503 durante o startup causou `CASMountLeaseLost`=1; remount e carregamento completo (1043/1043 às 01:49:25Z) **sem outro restart**. Timeouts/`NETWORK_ERROR` durante o carregamento. Nenhum INSERT real na janela observada |

Rollout dos três nós confirmado em 07/10 01:50Z. A verificação somente leitura de 07/10 ~09:20Z registrou 11.536 INSERT e 11.210 `AsyncInsertFlush` no writer desde 03:00Z, sem exceção de INSERT no filtro.

O primeiro intento no reader compartilhado parou antes de qualquer restart: o security group do endpoint ECR só aceitava ECS. O #186 corrigiu isso.

## Onde roda

| Nó | Desde | Até |
|---|---|---|
| Writer | 07/10 01:40:20Z | 11/10 22:33:08Z (RC3) |
| Reader-mdb | 07/10 01:35:56Z | 11/10 20:45Z (RC3) |
| Reader compartilhado | 07/10 01:24:12Z | 11/10 21:55Z (RC3) |

Restarts de container **sem troca de imagem** em 11/10, todos para config do kayro-infra (TEC-836/837/845):
- reader-mdb: boots às 11:13:07Z, 11:26:12Z e 11:58:56Z (o último aplicou `cas_gc_read_concurrency=1`, #219);
- reader compartilhado: 13:15:14Z (`configure-reader-role.py`, #216/#218/#219/#221);
- writer: 13:26:25Z (`configure-filesystem-cache.py`, só o GC).

## Comportamento conhecido em produção no RC2

- **Read-ahead do GC desperdiçado** (TEC-835/838): o líder do GC fazia ~2,4 M GET 404 por dia em `cas/ns/stream/<life>/_log/<epoch>-<seq>.zst`, 58 % dos GETs do nó, cerca de US$ 2,9/dia mais GuardDuty. Mitigado por config em 11/10 (#218, #219) e corrigido no [RC3](RC3.md).
- **Tempestade de LIST no restart** (TEC-845): 165–520 mil LIST por boot, com SlowDown capaz de derrubar o lease de outros membros. Continua no RC3.
