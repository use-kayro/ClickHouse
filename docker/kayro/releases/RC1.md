# RC1: recuperação do carregamento de tabelas CAS (rejeitada)

**Não usar.** A comparação de funcionalidades rejeitou esta imagem. Ela nunca rodou em produção.

| Campo | Valor |
|---|---|
| Tag git | `v26.6.4.20001-kayro-cas1-rc1` → `c20750231669cfc5b19b0c6b3df2cb05007732ec` |
| Base | Altinity `v26.6.4.20001.altinityantalya`, `2073b1f88dec885444fac97edb8179ad1753c1ad` |
| Imagem | `474668394551.dkr.ecr.us-east-2.amazonaws.com/kayro/clickhouse@sha256:85a2931647d92154dda525cadba64d1e957c4b6b02f0ccffc0d89fa01c5e545c` |
| Tag da imagem | `26.6.4.20001-kayro-cas1-rc1` |
| SHA-256 do binário | `2dd90d083e85d19924312b1c7af7da6d8e77654f83a20d38f430398806805512` |
| Plataforma | linux/arm64 |
| Release | https://github.com/use-kayro/ClickHouse/releases/tag/v26.6.4.20001-kayro-cas1-rc1 (publicada 06/10 22:40:30Z, pré-release) |
| Linear | TEC-736 |
| Em produção | nunca |

## O que muda

Quando o CAS perde o lease de mount e depois se recupera, o servidor original remonta o storage, mas as tabelas cujo carregamento falhou ficam em `FAILED`/`CANCELED` até um restart. Com este patch, a construção assíncrona de tabelas repete apenas a exceção tipada de estado transitório do mount CAS. Fencing, falhas terminais e erros genéricos de escrita ou convergência mantêm o comportamento original.

Commits (sobre `2073b1f8`): `61fe33b7` (retry no carregamento assíncrono, harness `tests/integration/test_cas_load_recovery` e receita `docker/kayro`) e `c2075023` (ambiente limpo para o teste de controle).

## Como foi construída

Com as flags reduzidas do experimento da TEC-736: `Release`, ThinLTO, **Rust desligado**, testes, utilitários e exemplos desligados. O binário foi compilado no laboratório AWS da TEC-736 (kayro-infra branch `test/cas-self-heal-lab`, PR #182), e os seis arquivos C++ compilados foram conferidos contra este fork.

## Validação

- **Fault harness** (`tests/integration/test_cas_load_recovery`, AWS, 06/10): o build original continuou `FAILED` depois que o mount se recuperou; o RC1 recuperou writer, reader e writer de novo sem restart, preservou linhas e UUID da tabela e replicou linhas novas. Recusa terminal preservada. Shutdown durante a falha: 3,61 s, exit 0.
- **Smoke de publicação:** pull por digest, `SELECT version(), 1` correto, label de source `c2075023`.
- **Paridade (06/10 22:53Z): rejeitada.** Funções 1842/1842, formatos 114/114 e settings 1713/1713 iguais, mas faltavam 4 engines DeltaLake, 7 table functions `deltaLake` e `USE_WASMTIME`/`USE_DELTA_KERNEL_RS`, porque o Rust estava desligado.

O resultado do fault test continua válido; o que faltou foi paridade com a distribuição oficial. O RC2 é o mesmo patch com Rust e otimização de release.
