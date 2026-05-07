# Fluxo de Dados

O job e orientado por metadata de ingestao e configuracoes locais.

```text
INGESTION_ID
  -> DynamoDB pipeline_control
  -> metadata da ingestao
  -> config_origem_dados.json
  -> leitura Glue Catalog com predicate
  -> temp view source_table_name
  -> SQL por process_name/sor_table_name
  -> config_destino_dados.json
  -> S3 SOR + Glue Catalog
```

O SQL final define o layout da SOR. A coluna `ingestion_id` e adicionada pelo job para versionamento.
