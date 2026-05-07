# Runbook

## Validacao Antes da Execucao

- Confirme `MODO_EXECUCAO=sot` ou `MODO_EXECUCAO=sor`.
- Confirme que existe execucao compativel em `src/config/config_origem_dados.json`.
- Confirme que existe destino compativel em `src/config/config_destino_dados.json`.
- Confirme que o arquivo apontado por `sql_path` existe.
- Se usar `TABLE_PREDICATES`, confirme o formato `nome_fonte::predicate`.
- Se usar `USAR_PREDICATE_OVERRIDE_DDB=true`, confirme o item em `glue_predicate_overrides`.

## SOR

- Informe `INGESTION_ID`.
- Confirme que existe item `INGESTION_ID#<id> / METADATA` na tabela `pipeline_control`.
- Confirme que o item esta com `status = PROCESSING_SOR`.
- Confirme que a particao de destino espera a coluna tecnica `ingestion_id`.

## SOT

- Informe `PROCESS_ID`.
- Informe `DATA_REF` quando os filtros SOT usarem esse placeholder; formatos aceitos: `YYYYMM` ou `YYYYMMDD`.
- Confirme que cada fonte possui `nome_view` compativel com o SQL.
- Confirme que a particao de destino existe no DataFrame final.

## Falhas Comuns

- `MODO_EXECUCAO` diferente de `sot` ou `sor`.
- Metadata SOR ausente ou com status diferente de `PROCESSING_SOR`.
- Placeholder do filtro de origem nao existe no contexto do modo.
- Override de predicate com chave diferente do nome da fonte.
- Item DynamoDB de override sem `enabled=true`.
- SQL inexistente no `sql_path`.
- Destino sem `particao_tabela_destino` valida.
- `campos_duplicidade` com coluna inexistente na origem.
- Coluna esperada pelo SQL nao existe na temp view registrada.
