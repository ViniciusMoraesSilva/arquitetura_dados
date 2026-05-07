# Guia Para Agents

Este diretorio contem o Glue Job generico de Carga SOR do MVP1. O job recebe um `INGESTION_ID`, busca metadata no DynamoDB `pipeline_control`, usa JSONs locais para definir leitura e destino, executa SQL por `process_name` e `sor_table_name`, e grava a versao SOR particionada por `ingestion_id`.

## Mapa Rapido

- `src/main.py`: orquestrador Glue/PySpark da carga SOR.
- `src/config/config_origem_dados.json`: filtros e parametros de leitura por processo/tabela.
- `src/config/config_destino_dados.json`: destinos fisicos por processo/tabela/ambiente.
- `src/sql/<process_name>/<sor_table_name>.sql`: SQL que define o layout final da tabela SOR.
- `src/utils/metadata_ingestao.py`: leitura e validacao do item `INGESTION_ID`.
- `src/utils/templates.py`: placeholders permitidos nos filtros de origem.
- `src/utils/gravar_sor_glue.py`: escrita Parquet no S3 e atualizacao do Glue Catalog.

## Contratos

Argumentos obrigatorios:

- `JOB_NAME`
- `INGESTION_ID`
- `AMBIENTE`
- `DBLOCAL`

Argumento opcional:

- `PIPELINE_CONTROL_TABLE`, com default `pipeline_control`.
- `TABLE_PREDICATES`, no formato `source_table_name::predicate`.
- `USAR_PREDICATE_OVERRIDE_DDB`, para habilitar override na tabela `glue_predicate_overrides`.

O item DynamoDB esperado usa:

- `PK = INGESTION_ID#<INGESTION_ID>`
- `SK = METADATA`
- `status = PROCESSING_SOR`

## Regras

- Nao use `DATA_REF` ou `GLUE_SQL_LOCATION` neste job.
- O filtro padrao da origem deve vir de `config_origem_dados.json`.
- Overrides de filtro seguem a precedencia `TABLE_PREDICATES`, DynamoDB, config.
- O override DynamoDB usa `pk = PROCESS_TABLE#<process_name>#<sor_table_name>` e `sk = SOURCE#<source_table_name>`.
- Deduplicacao opcional usa `campos_duplicidade` no item da origem.
- O nome fisico de saida deve vir de `config_destino_dados.json`.
- O SQL controla o layout final, mas o job adiciona `ingestion_id` antes de gravar.
- A Step Functions/Lambda continua responsavel por chamar `MARK_LOADED_SOR` e atualizar `CURRENT_SOR`.
