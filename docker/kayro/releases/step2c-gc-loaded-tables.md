# Passo 2c: o GC espera o startup das tabelas em vez de comparar contagens

Corrige uma pausa do GC do CAS no writer: depois do primeiro DROP de uma tabela que existia no boot, o GC passava a achar que alguma tabela não estava carregada e não apagava mais nada até o próximo restart. Nenhum dado se perdeu; só a recuperação de espaço parou. Sem mudança de protocolo, sem setting nova, sem formato novo.

| Campo | Valor |
|---|---|
| PR | https://github.com/use-kayro/ClickHouse/pull/12 (branch `kayro/gc-loaded-tables`), head `1550abe87523a982a589ea04890f006e45ef3c27`, depois da revisão `/enxuto`. Aberto quando isto foi escrito |
| Base | passo 2b (`a6355a99`) |
| Imagem | `TODO` |
| Linear | TEC-850 |
| Em produção | `TODO` |

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

Portão do builder: `TODO`.

## Upstream

O vazamento de `startup_table` está corrigido em ClickHouse/ClickHouse#106238 (commit `22658b49`, 29/06/2026), presente no master e no 26.7 em diante, não na base 26.6 deste fork. O cherry-pick é uma questão separada; esta correção vale com ou sem ele.

## Regra de deploy

Sem mudança de protocolo: qualquer ordem, um nó por vez. Só o writer roda rodadas que apagam, então é nele que a correção aparece.

**Rollback:** a imagem do passo 2b, `sha256:1912bde084b7a39c86af8498715628da23be3554d42da1341aeafcb0cd34ba03`. Voltar para ela traz o bug de volta (a pausa depois do primeiro DROP de tabela do boot), e nada mais.

## Imagem de release

Digest: `TODO`.

## Deploy

`TODO`.
