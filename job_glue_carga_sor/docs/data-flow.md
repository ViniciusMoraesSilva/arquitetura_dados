# Fluxo de Dados

O job e orientado por `MODO_EXECUCAO` e configuracoes locais.

```text
Argumentos Glue
  -> MODO_EXECUCAO=sot|sor
  -> contexto da execucao
  -> config_origem_dados.json
  -> leitura Glue Catalog/CSV com predicate
  -> temp views
  -> SQL configurado em sql_path
  -> post_columns
  -> config_destino_dados.json
  -> S3 + Glue Catalog ou CSV
```

## SOR

```text
INGESTION_ID
  -> DynamoDB pipeline_control
  -> metadata da ingestao
  -> execucao SOR por process_name/source/sor_table
  -> SQL SOR
  -> adiciona ingestion_id
  -> S3 SOR + Glue Catalog
```

## SOT

```text
PROCESS_ID
  -> execucao SOT configurada
  -> uma ou mais fontes catalogo/CSV
  -> SQL SOT
  -> adiciona process_id por default
  -> destino configurado
```
