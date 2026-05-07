# Runbook

## Validacao Antes da Execucao

- Confirme que existe item `INGESTION_ID#<id> / METADATA` na tabela `pipeline_control`.
- Confirme que o item esta com `status = PROCESSING_SOR`.
- Confirme que existe origem compativel em `src/config/config_origem_dados.json`.
- Confirme que existe destino compativel em `src/config/config_destino_dados.json`.
- Confirme que existe SQL em `src/sql/<process_name>/<sor_table_name>.sql`.
- Se usar `TABLE_PREDICATES`, confirme o formato `source_table_name::predicate`.
- Se usar `USAR_PREDICATE_OVERRIDE_DDB=true`, confirme o item em `glue_predicate_overrides`.

## Falhas Comuns

- Metadata ausente ou com status diferente de `PROCESSING_SOR`.
- Placeholder do filtro de origem nao existe na metadata.
- Override de predicate com chave diferente de `source_table_name`.
- Item DynamoDB de override sem `enabled=true` ou sem campo `predicate`.
- SQL inexistente para o par `process_name` e `sor_table_name`.
- Destino sem `particao_tabela_destino` contendo `ingestion_id`.
- `campos_duplicidade` com coluna inexistente na origem.
- Coluna esperada pelo SQL nao existe na origem.
