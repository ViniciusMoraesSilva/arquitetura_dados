# Guia Para Agents

Este diretorio contem o Glue Job unico para execucoes `sot` e `sor`. O job usa JSONs locais para definir fontes, SQL, colunas tecnicas e destino.

## Mapa Rapido

- `src/main.py`: orquestrador Glue/PySpark comum para SOT e SOR.
- `src/config/config_origem_dados.json`: contrato de execucoes, fontes e `sql_path`.
- `src/config/config_destino_dados.json`: destinos fisicos por modo/processo/ambiente.
- `src/sql/`: SQLs referenciados por `sql_path`.
- `src/utils/metadata_ingestao.py`: leitura e validacao do item `INGESTION_ID` usado no SOR.
- `src/utils/templates.py`: placeholders permitidos nos filtros/configs.
- `src/utils/gravar_sor_glue.py`: escrita unificada em Glue Catalog/Parquet ou CSV no S3.

## Contratos

Argumentos obrigatorios comuns:

- `JOB_NAME`
- `MODO_EXECUCAO`, com valores `sot` ou `sor`
- `AMBIENTE`
- `DBLOCAL`

Argumentos por modo:

- SOT: `PROCESS_ID`; `DATA_REF` e opcional, mas deve ser informado quando a config usa `{DATA_REF}`.
- SOR: `INGESTION_ID`.

Argumentos opcionais:

- `PIPELINE_CONTROL_TABLE`, com default `pipeline_control`.
- `TABLE_PREDICATES`, no formato `nome_fonte::predicate`.
- `USAR_PREDICATE_OVERRIDE_DDB`, para habilitar override na tabela `glue_predicate_overrides`.

## Regras

- Preserve `MODO_EXECUCAO=sot` e `MODO_EXECUCAO=sor` como modos explicitos.
- O SQL deve vir de `sql_path`; nao volte a hardcodar caminhos por modo.
- Fontes SOT podem ser `catalogo` ou `csv`; fontes SOR permanecem orientadas pela metadata.
- Overrides de filtro seguem a precedencia `TABLE_PREDICATES`, DynamoDB, config.
- O SOR adiciona `ingestion_id` por default; o SOT adiciona `process_id` por default.
- O nome fisico de saida deve vir de `config_destino_dados.json`.
- Atualize `CONTEXT.md` e `docs/` sempre que mudar argumentos Glue, fluxo, configs, SQL, destino ou comportamento de leitura/escrita.
