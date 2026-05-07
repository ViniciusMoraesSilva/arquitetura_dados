# Contexto do Glue Carga SOR

`job_glue_carga_sor` implementa o item 4 do MVP1 SOR/SOT/SPEC. Ele e generico por `process_name` e por tabela SOR.

## Fluxo

1. Recebe `INGESTION_ID`.
2. Busca `INGESTION_ID#<id> / METADATA` no DynamoDB.
3. Valida `status = PROCESSING_SOR`.
4. Seleciona a origem em `config_origem_dados.json`.
5. Resolve o filtro usando metadata e `partitions`.
6. Aplica override de predicate por argumento ou DynamoDB quando configurado.
7. Le a origem no Glue Catalog.
8. Aplica deduplicacao opcional por `campos_duplicidade`.
9. Registra temp views `source_data` e `<source_table_name>`.
10. Executa `src/sql/<process_name>/<sor_table_name>.sql`.
11. Adiciona `ingestion_id`.
12. Grava no destino configurado em `config_destino_dados.json`.

## Predicate

A precedencia do predicate e:

1. `TABLE_PREDICATES`, usando chave `source_table_name`;
2. DynamoDB `glue_predicate_overrides`;
3. `filtro_origem` do JSON.

O override DynamoDB usa `pk = PROCESS_TABLE#<process_name>#<sor_table_name>` e `sk = SOURCE#<source_table_name>`.

## Placeholders de Origem

Filtros em JSON podem usar:

- `INGESTION_ID`
- `PROCESS_NAME`
- `SOURCE_DATABASE_NAME`
- `SOURCE_TABLE_NAME`
- `SOR_DATABASE_NAME`
- `SOR_TABLE_NAME`
- `PARTITION_<NOME_DA_PARTITION_EM_UPPERCASE>`

Exemplo: `partitions.data_referencia` vira `PARTITION_DATA_REFERENCIA`.
