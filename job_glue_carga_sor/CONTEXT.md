# Contexto do Glue Unico SOT/SOR

`job_glue_carga_sor` agora orquestra execucoes Glue padronizadas com `MODO_EXECUCAO=sot` ou `MODO_EXECUCAO=sor`.

## Fluxo Comum

1. Recebe argumentos Glue comuns: `JOB_NAME`, `MODO_EXECUCAO`, `AMBIENTE` e `DBLOCAL`.
2. Resolve o contexto da execucao conforme o modo.
3. Seleciona fontes, SQL, colunas tecnicas e destino nos JSONs locais.
4. Carrega uma ou mais fontes, registra temp views, executa SQL sequencial e grava o resultado.

## Modo SOR

- Recebe `INGESTION_ID`.
- Busca `INGESTION_ID#<id> / METADATA` no DynamoDB `pipeline_control`.
- Valida `status = PROCESSING_SOR`.
- Seleciona execucao por `process_name`, `source_database_name`, `source_table_name` e `sor_table_name`.
- Adiciona `ingestion_id` antes da gravacao.

## Modo SOT

- Recebe `PROCESS_ID`.
- Seleciona execucao por `process_id` no JSON.
- Pode carregar multiplas fontes de catalogo e/ou CSV.
- Adiciona `process_id` por default, ou as colunas definidas em `post_columns`.

## Predicate

A precedencia do predicate e:

1. `TABLE_PREDICATES`, usando chave da fonte;
2. DynamoDB `glue_predicate_overrides`, quando habilitado;
3. `filtro_origem` do JSON.

No SOR, o override DynamoDB usa `pk = PROCESS_TABLE#<process_name>#<sor_table_name>` e `sk = SOURCE#<source_table_name>`.

No SOT, o override DynamoDB usa `pk = PROCESS_ID#<process_id>` e `sk = JOB#<job_name>`, com atributo `predicates`.

## Placeholders

SOR aceita placeholders de metadata e partitions:

- `INGESTION_ID`
- `PROCESS_NAME`
- `SOURCE_DATABASE_NAME`
- `SOURCE_TABLE_NAME`
- `SOR_DATABASE_NAME`
- `SOR_TABLE_NAME`
- `PARTITION_<NOME_DA_PARTITION_EM_UPPERCASE>`

SOT aceita placeholders de processo/data:

- `PROCESS_ID`
- `DATA_REF`
- `DATA_ATUAL`
- `ANO_DATA_REF`
- `MES_DATA_REF`
- `ANO_MES_DATA_REF`
- `MES_ANTERIOR`
- `ANO_MES_ANTERIOR`
