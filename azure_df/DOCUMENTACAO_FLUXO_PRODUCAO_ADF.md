# Documentacao Tecnica do Fluxo de Producao - Azure Data Factory (adf-okajima)

## 1) Objetivo e escopo

Este documento descreve o fluxo **atualmente aplicado em producao** no modelo de dados do repositorio `azure_df`, com base nos artefatos versionados do Azure Data Factory.

Fonte de verdade usada:
- Triggers em `azure_df/trigger/*.json` (estado e agendamento).
- Pipelines em `azure_df/pipeline/*.json` (orquestracao e regras).
- Datasets em `azure_df/dataset/*.json` (origem/destino e path de arquivos).
- Linked services em `azure_df/linkedService/*.json` (conexoes).
- Integration runtime em `azure_df/integrationRuntime/*.json`.

## 2) Arquitetura de dados

### 2.1 Origens

- **PostgreSQL Supabase** (`PostgreSupabase`):
  - host: `aws-0-sa-east-1.pooler.supabase.com`
  - usado para tabelas de dimensoes, fatos, metas e controle incremental.
- **PostgreSQL Supabase Portal** (`PostgreSupabase_Portal`):
  - mesmo host, credencial separada.
  - usado para tabelas do dominio de tarefas/campanhas.
- **Oracle on-prem** (`Oracle_OKAJIMA_WINT`):
  - server: `192.168.254.190/WINT`
  - acesso via IR self-hosted `integrationRuntimeOkajimaWINT`.

### 2.2 Destino

- **ADLS Gen2** (`DataLake_Oracle`):
  - URL: `https://datalakeokajimapbi.dfs.core.windows.net/`
  - container principal usado: `powerbi`
  - formato padrao de escrita: **Parquet + Snappy**.

## 3) Fluxo produtivo agendado (triggers em estado Started)

Timezone dos agendamentos: `E. South America Standard Time`.

| Trigger | Janela diaria | Pipelines acionados |
|---|---|---|
| `Trigger_fatos` | 00:45, 06:45 ate 23:45 | `pl_fatos`, `pl_fatopedidorebaixador`, `pl_fatopromocaodesconto`, `pl_dimensoes_incremental`, `pl_tabelas_preco_pbi`, `pl_fato_est`, `pl_fato_rota`, `pl_relatorio_verbas_oracle_adls` |
| `Trigger_dimensoes` | 09:45, 12:45, 15:45, 18:45, 21:45 | `pl_dimensoes`, `pl_dimensoes_oracle_adls3` |
| `Trigger_metas` | 09:45, 12:45, 15:45, 18:45, 21:45 | `pl_metas` |
| `Trigger_CE` | 11:45, 15:45, 18:45 | `pl_hitloreal`, `pl_positivakc_oracle_adls` |

Observacao operacional:
- Os pipelines listados acima sao o **fluxo oficial em producao** porque estao conectados a triggers `Started`.
- Existem pipelines auxiliares/historicos/teste fora desse fluxo (secao 8).

## 4) Detalhamento do dominio Fatos

### 4.1 Pipeline `pl_fatos` (Supabase -> ADLS)

Estrutura:
- `ForEach_Table` paralelo sobre `table_configurations`.
- Para cada tabela:
  - Se `loadType = full`: deleta pasta/arquivo atual e recopia tudo.
  - Se `loadType = incremental`: calcula meses (`anomes`) e chama pipeline filho.

Configuracao atual (`table_configurations`):
- `fatofaturamento_pbi` | `anomes_faturamento` | `monthsToProcess=2` | `incremental`
- `fatodevolucao_pbi` | `anomes_devolucao` | `monthsToProcess=2` | `incremental`
- `fatopedidovenda_pbi` | `anomes_venda` | `monthsToProcess=2` | `incremental`

Observacao:
- `map_venda_devolucao_pbi` foi removida do `pl_fatos` em 28/03/2026 apos descontinuacao no modelo semantico do Power BI.

Tabelas SQL utilizadas (origem):
- `public.fatofaturamento_pbi`
- `public.fatodevolucao_pbi`
- `public.fatopedidovenda_pbi`

Regra de meses no incremental:
- Lookup usa `generate_series` para montar lista mensal ate mes atual.
- Com `monthsToProcess=2`, processa **mes atual e mes anterior**.

### 4.2 Pipeline filho `pl_fatos_process_incremental_partitions`

Para cada `anomes` recebido:
- `Delete_Incremental_Partition`
- `Copy_Incremental_Partition`

Caracteristicas:
- Loop de meses sequencial (`isSequential = true`) para reduzir concorrencia no mesmo path.
- Query usa `SELECT` simples com filtro por coluna de `anomes`; timeout controlado por `queryTimeout`.
- Retry configurado na copia (`retry=3`, intervalo 180s).

Tabelas SQL utilizadas (origem):
- Recebe `tableName` do `pl_fatos` para as 3 tabelas fato atualmente ativas no fluxo.

Path de saida:
- Incremental: `fatos/{tabela}/{tabela}_{anomes}.parquet`
- Full: `fatos/{tabela}/{tabela}.parquet`

### 4.3 Pipeline `pl_fatopedidorebaixador` (Oracle -> ADLS)

Pipeline controlador com decisao por parametro `LoadType`:
- `full` -> chama `pl_fatopedidorebaixador_full`
- default `incremental` -> chama `pl_fatopedidorebaixador_incremental`

Incremental (`pl_fatopedidorebaixador_incremental`):
- Se `AnomesList` nao vier preenchida, monta automaticamente:
  - `yyyyMM` atual no fuso `E. South America Standard Time`
  - `yyyyMM` do mes anterior no fuso `E. South America Standard Time`
- Para cada mes, consulta Oracle filtrando `DATA_ENTRADA_PEDIDO` no intervalo do mes.

Full (`pl_fatopedidorebaixador_full`):
- Busca todos os meses historicos distintos via Lookup em Oracle.
- Processa mes a mes e grava um arquivo por mes.

Tabelas SQL utilizadas (origem Oracle):
- `OKAJIMA.OKJ_FATOPEDIDOREBAIXADOR_PBI`

Path de saida:
- `fatos/fatopedidorebaixador_pbi/okj_fatopedidorebaixador_pbi_{anomes}.parquet`

### 4.4 Pipeline `pl_fatopromocaodesconto` (Oracle -> ADLS)

Pipeline controlador com decisao por parametro `LoadType`:
- `full` -> chama `pl_fatopromocaodesconto_full_2026`
- default `incremental` -> chama `pl_fatopromocaodesconto_incremental`

Incremental (`pl_fatopromocaodesconto_incremental`):
- Se `AnomesList` nao vier preenchida, monta automaticamente:
  - `yyyyMM` atual no fuso `E. South America Standard Time`
  - `yyyyMM` do mes anterior no fuso `E. South America Standard Time`
- Para cada mes, consulta Oracle filtrando `DATAVENDA` no intervalo do mes.

Full 2026 (`pl_fatopromocaodesconto_full_2026`):
- Busca meses distintos de `ANOMES_VENDA` entre `StartAnomes=202601` e
  `EndAnomesExclusive=202701`.
- Processa mes a mes e grava um arquivo por mes.

Tabela/View SQL utilizada (origem Oracle):
- `OKAJIMA.OKJ_FATOPROMOCAODESCONTO_PBI`

Path de saida:
- `fatos/fatopromocaodesconto_pbi/okj_fatopromocaodesconto_pbi_{anomes}.parquet`

### 4.5 Pipeline `pl_tabelas_preco_pbi` (Oracle -> ADLS)

Pipeline oficial para publicacao das tabelas de preco no lake. Executa em
paralelo sobre `TableConfigurations` e copia cada objeto Oracle para Parquet
com schema dinamico.

Configuracao atual:
- `OKJ_DIMREGIAOPRECO_PBI` -> `dimensoes/dimregiaopreco_pbi/okj_dimregiaopreco_pbi.parquet`
- `OKJ_DIMPRECOCLIENTE_PBI` -> `dimensoes/dimprecocliente_pbi/okj_dimprecocliente_pbi.parquet`
- `OKJ_FATOTABELAPRECO_PBI` -> `fatos/fatotabelapreco_pbi/okj_fatotabelapreco_pbi.parquet`

Tabelas/views SQL utilizadas (origem Oracle):
- `OKAJIMA.OKJ_DIMREGIAOPRECO_PBI`
- `OKAJIMA.OKJ_DIMPRECOCLIENTE_PBI`
- `OKAJIMA.OKJ_FATOTABELAPRECO_PBI`

## 5) Detalhamento do dominio Dimensoes

### 5.1 Pipeline `pl_dimensoes` (Supabase -> ADLS, carga recorrente)

Executa em paralelo (`batchCount=6`) para lista padrao:
- `dimrca_pbi`
- `dimproduto_pbi`
- `dimcliente_pbi`
- `dimfornecedor_pbi`
- `dimcarregamento_pbi`
- `dimcobranca_pbi`
- `dimemitente_pbi`
- `dimplanopagamento_pbi`
- `dimfilial_pbi`
- `promos_bees_sv_rca`
- `dimmotivodevolucao_pbi`
- `fatoestoqueatual_pbi`
- `fatorotalog_pbi`

Regra especial:
- Para `dimcliente_pbi`, a query faz `LEFT JOIN` com `okj_base_expo` para adicionar `base_expo`.

Tabelas SQL utilizadas (origem Supabase):
- `public.dimrca_pbi`
- `public.dimproduto_pbi`
- `public.dimcliente_pbi`
- `public.dimfornecedor_pbi`
- `public.dimcarregamento_pbi`
- `public.dimcobranca_pbi`
- `public.dimemitente_pbi`
- `public.dimplanopagamento_pbi`
- `public.dimfilial_pbi`
- `public.promos_bees_sv_rca`
- `public.dimmotivodevolucao_pbi`
- `public.fatoestoqueatual_pbi`
- `public.fatorotalog_pbi`
- `public.okj_base_expo` (join auxiliar quando item = `dimcliente_pbi`)

Path de saida:
- `dimensoes/{tabela}/{tabela}.parquet`

### 5.2 Pipeline `pl_dimensoes_oracle_adls3` (Oracle -> ADLS)

Copia tabela(s) Oracle para ADLS (atual em producao):
- Configuracao por objeto em `TableConfigurations`
- Cada item define:
  - `SourceView`
  - `SinkFolder`
  - `SinkFileName`
  - `QueryOverride` opcional

Tabelas/Views SQL utilizadas (origem Oracle):
- `OKAJIMA.OKJ_DIMROTA_PBI`
- `OKAJIMA.OKJ_DIMROTACOBERTURA_PBI`

Path de saida:
- `dimensoes/dimrota_pbi/dimrota_pbi.parquet`
- `dimensoes/dimrotacobertura_pbi/dimrotacobertura_pbi.parquet`

### 5.3 Pipeline `pl_dimensoes_incremental` + filho

Pipeline `pl_dimensoes_incremental`:
- Processa configuracao incremental atual:
  - tabela `public.dimcabecalhopedidovenda_pbi`
  - coluna de particao: `TO_CHAR(datavenda, 'YYYYMM')`
  - `monthsToProcess=2`
- Calcula a janela mensal pela data local de Sao Paulo (`CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'`) e retorna apenas `anomes` existentes na tabela de origem. Isso evita gerar parquet vazio do mes seguinte antes da virada local ou quando ainda nao ha linhas no novo mes.
- Chama `pl_dimensoes_process_incremental_partitions`.

Pipeline filho `pl_dimensoes_process_incremental_partitions`:
- Para cada `anomes`:
  - deleta arquivo anterior
  - recopia particao mensal
- Escreve no dataset com `FileSystem` parametrizavel (default no pipeline: `powerbi`).

Tabelas SQL utilizadas (origem Supabase):
- `public.dimcabecalhopedidovenda_pbi`

Path de saida:
- `dimensoes/dimcabecalhopedidovenda_pbi/dimcabecalhopedidovenda_pbi_{anomes}.parquet`

## 6) Detalhamento do dominio Metas

### Pipeline `pl_metas` (Supabase -> ADLS)

Estrutura:
- `ForEach_tabelas_meta` com `batchCount=10`.
- Copia cada tabela da lista para `metas/{tabela}/{tabela}.parquet`.

Lista atual contem 35 tabelas de metas (ex.: `fatometacategoria_pbi`, `fatometacliente_pbi`, `fatometageral_pbi`, `metaclienteconexao_pbi`).

Tabelas SQL utilizadas (origem Supabase):
- `public.fatometacategoria_pbi`
- `public.fatometacliente_pbi`
- `public.fatometadepartamento_pbi`
- `public.fatometafornecedor_pbi`
- `public.fatometaproduto_pbi`
- `public.fatometarca_pbi`
- `public.fatometamarca_pbi`
- `public.fatometagrupoprod_pbi`
- `public.fatometasecao_pbi`
- `public.fatometafornecedorpex_pbi`
- `public.fatometagrupoprodpex_pbi`
- `public.fatometaprodutopex_pbi`
- `public.fatometaclientepex_pbi`
- `public.fatometacliprodpex_pbi`
- `public.fatometacligrupoprod_pbi`
- `public.fatometafornecedorextra_pbi`
- `public.fatometaprodutoextra_pbi`
- `public.fatometaclienteextra_pbi`
- `public.fatometagrupoprodextra_pbi`
- `public.fatometaclientecnx_pbi`
- `public.fatometasuperfechamento_pbi`
- `public.fatometacliprod_pbi`
- `public.fatometageral_pbi`
- `public.fatometageralfornec_pbi`
- `public.fatometasemanapasta_pbi`
- `public.fatometasemanaregiao_pbi`
- `public.fatometageralfornecddd_pbi`
- `public.fatometafeira_pbi`
- `public.fatometaprevsemanal_pbi`
- `public.fatometabaseexpo_pbi`
- `public.fatometaprodutox_pbi`
- `public.fatometarcaextra_pbi`
- `public.fatometaexpo_pbi`
- `public.metapassagemexpo_pbi`
- `public.metaclienteconexao_pbi`

## 7) Detalhamento do dominio CE / Oracle complementar

### 7.1 `pl_hitloreal`

- Fonte Oracle (default `HITLOREAL_RESULTADOS_VW`) com query customizada e `CAST`.
- Destino: `fatos/fatohitloreal_pbi/fatohitloreal_pbi.parquet`.

Tabela/View SQL utilizada (origem Oracle):
- `OKAJIMA.HITLOREAL_RESULTADOS_VW`

### 7.2 `pl_positivakc_oracle_adls`

- Tabelas default:
  - `OKJ_POSITIVAKC_RESULTADOS`
  - `OKJ_POSITIVAKC_INDICADORES`
- Destino dinamico:
  - `positivakc/positivakc_resultados/okj_positivakc_resultados.parquet`
  - `positivakc/positivakc_indicadores/okj_positivakc_indicadores.parquet`

Tabelas SQL utilizadas (origem Oracle):
- `OKAJIMA.OKJ_POSITIVAKC_RESULTADOS`
- `OKAJIMA.OKJ_POSITIVAKC_INDICADORES`

## 8) Pipelines existentes fora do fluxo agendado de producao

Pipelines presentes no repositorio, mas nao ligados aos triggers ativos listados na secao 3:
- Historico/full auxiliar:
  - `pl_fatos_carga_historica`
  - `pl_dimensoes_cabecalho_full`
- Fluxos de tarefas (staging/dataflow):
  - `pl_tarefas_stg_portal`
  - `pl_tarefas_stg_portal_dyn`
  - `pl_tarefas_df`
  - dataflows `DfTarefasBase` e `DfTarefasFatoDim`
- Outros auxiliares:
  - `pl_dimperiodo`
  - `pl_campanha_extra`
- Legado:
  - `pl_fatopedidorebaixador_v1`
  - `pl_fatotabelapreco_pbi` (substituido por `pl_tabelas_preco_pbi`)
- Teste:
  - `pl_dimensoes_oracle_adls` (pasta `dimensoes/teste`)
  - `pl_dimensoes_teste`

Tabelas SQL utilizadas nesses pipelines:
- `pl_fatos_carga_historica`: `public.{tableName}` (default `public.fatofaturamento_pbi`).
- `pl_dimensoes_cabecalho_full`: `{tableName}` (default `dimcabecalhopedidovenda_pbi`; em uso com `public.dimcabecalhopedidovenda_pbi`).
- `pl_tarefas_stg_portal`: `public.okj_tarefas`, `public.okj_tarefas_prod`, `public.okj_tarefas_rcas`, `public.okj_tarefas_cli`.
- `pl_tarefas_stg_portal_dyn` (default): `okj_tarefas`, `okj_tarefas_prod`, `okj_tarefas_rcas`, `okj_tarefas_cli`, `fv_cliente_rca`.
- `pl_tarefas_df` / `DfTarefasFatoDim`: nao executa SQL em banco; consome parquet no ADLS gerado a partir de `okj_tarefas*`, dimensoes (`dimcliente_pbi`, `dimrca_pbi`, `dimrotarca_pbi`) e fato `fatofaturamento_pbi`.
- `DfTarefasBase`: idem, transformacao em parquet de tarefas + dimensoes (`dimfornecedor_pbi`, `dimproduto_pbi`).
- `pl_dimperiodo`: `public.dimperiodo_pbi`.
- `pl_campanha_extra`: `public.app_campanha_metas_rca`.
- `pl_fatopedidorebaixador_v1`: `public.fatopedidorebaixador_pbi` (legado).
- `pl_fatotabelapreco_pbi`: `OKAJIMA.OKJ_FATOTABELAPRECO_PBI` (legado; substituido por `pl_tabelas_preco_pbi`).
- `pl_dimensoes_oracle_adls` (teste): `OKAJIMA.OKJ_DIMGRUPOCLIENTES_PBI`, `OKAJIMA.OKJ_DIMGRUPOPRODUTOS_PBI`, `OKAJIMA.OKA_ROTAS_VW`.
- `pl_dimensoes_teste`: default `public.dimcabecalhopedidovenda_pbi_v2` (e pode usar `public.okj_base_expo` se item for `dimcliente_pbi`).

## 9) Padroes tecnicos importantes

- Idempotencia por particao:
  - Cargas incrementais usam estrategia `Delete` + `Copy`.
- Paralelismo controlado:
  - tabelas em paralelo, particoes mensais em sequencial.
- Tolerancia a falhas:
  - politicas de retry configuradas em copias e deletes.
- Controle de timeout:
  - queries com `statement_timeout` em partes criticas.
- Convencao de armazenamento:
  - 1 arquivo parquet por tabela (full) ou por tabela+mes (incremental).

## 10) Checklist rapido de operacao

1. Validar diariamente se os 4 triggers seguem em `Started`.
2. Monitorar falhas por pipeline com foco em Oracle (rede/IR self-hosted) e em consultas longas.
3. Em reprocesso:
   - Para fatos/dimensoes incrementais, reexecutar pipeline informando meses alvo.
   - Para rebaixador, usar `LoadType=full` apenas em recuperacao historica planejada.
4. Garantir consistencia de paths esperados no filesystem `powerbi`.

## 11) Catalogo consolidado de tabelas/views SQL (para repositorio de queries)

### 11.1 Supabase / PostgreSQL

- `public.app_campanha_metas_rca`
- `public.dimcabecalhopedidovenda_pbi`
- `public.dimcabecalhopedidovenda_pbi_v2`
- `public.dimcarregamento_pbi`
- `public.dimcliente_pbi`
- `public.dimcobranca_pbi`
- `public.dimemitente_pbi`
- `public.dimfilial_pbi`
- `public.dimfornecedor_pbi`
- `public.dimmotivodevolucao_pbi`
- `public.dimplanopagamento_pbi`
- `public.dimperiodo_pbi`
- `public.dimproduto_pbi`
- `public.dimrca_pbi`
- `public.fatodevolucao_pbi`
- `public.fatoestoqueatual_pbi`
- `public.fatofaturamento_pbi`
- `public.fatometabaseexpo_pbi`
- `public.fatometacategoria_pbi`
- `public.fatometacliente_pbi`
- `public.fatometaclienteextra_pbi`
- `public.fatometaclientepex_pbi`
- `public.fatometaclientecnx_pbi`
- `public.fatometacliprod_pbi`
- `public.fatometacliprodpex_pbi`
- `public.fatometacligrupoprod_pbi`
- `public.fatometadepartamento_pbi`
- `public.fatometaexpo_pbi`
- `public.fatometafeira_pbi`
- `public.fatometafornecedor_pbi`
- `public.fatometafornecedorextra_pbi`
- `public.fatometafornecedorpex_pbi`
- `public.fatometageral_pbi`
- `public.fatometageralfornec_pbi`
- `public.fatometageralfornecddd_pbi`
- `public.fatometagrupoprod_pbi`
- `public.fatometagrupoprodextra_pbi`
- `public.fatometagrupoprodpex_pbi`
- `public.fatometamarca_pbi`
- `public.fatometaprevsemanal_pbi`
- `public.fatometaproduto_pbi`
- `public.fatometaprodutoextra_pbi`
- `public.fatometaprodutopex_pbi`
- `public.fatometaprodutox_pbi`
- `public.fatometarca_pbi`
- `public.fatometarcaextra_pbi`
- `public.fatometasecao_pbi`
- `public.fatometasemanapasta_pbi`
- `public.fatometasemanaregiao_pbi`
- `public.fatometasuperfechamento_pbi`
- `public.fatopedidorebaixador_pbi`
- `public.fatopedidovenda_pbi`
- `public.fatorotalog_pbi`
- `public.fv_cliente_rca`
- `public.metaclienteconexao_pbi`
- `public.metapassagemexpo_pbi`
- `public.okj_base_expo`
- `public.okj_tarefas`
- `public.okj_tarefas_cli`
- `public.okj_tarefas_prod`
- `public.okj_tarefas_rcas`
- `public.promos_bees_sv_rca`

### 11.2 Oracle

- `OKAJIMA.HITLOREAL_RESULTADOS_VW`
- `OKAJIMA.OKJ_DIMPRECOCLIENTE_PBI`
- `OKAJIMA.OKJ_DIMREGIAOPRECO_PBI`
- `OKAJIMA.OKJ_DIMROTA_PBI`
- `OKAJIMA.OKJ_DIMROTACOBERTURA_PBI`
- `OKAJIMA.OKJ_DIMGRUPOCLIENTES_PBI`
- `OKAJIMA.OKJ_DIMGRUPOPRODUTOS_PBI`
- `OKAJIMA.OKJ_FATOPROMOCAODESCONTO_PBI`
- `OKAJIMA.OKJ_FATOPEDIDOREBAIXADOR_PBI`
- `OKAJIMA.OKJ_FATOTABELAPRECO_PBI`
- `OKAJIMA.OKJ_POSITIVAKC_INDICADORES`
- `OKAJIMA.OKJ_POSITIVAKC_RESULTADOS`

---

Documento gerado a partir do snapshot versionado no repositorio local em **20/02/2026**.
