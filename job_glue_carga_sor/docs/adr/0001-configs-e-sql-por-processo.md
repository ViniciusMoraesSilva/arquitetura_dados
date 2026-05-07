# ADR 0001: Configuracao e SQL Por Processo/Tabela SOR

## Status

Aceita.

## Decisao

O Glue Carga SOR sera generico. A metadata do DynamoDB define a ingestao atual, enquanto os arquivos JSON e SQL do job definem comportamento por `process_name` e `sor_table_name`.

## Consequencias

- O filtro da origem fica versionado em `config_origem_dados.json`.
- O layout final fica versionado em SQL.
- O nome fisico da tabela de saida fica em `config_destino_dados.json`.
- O mesmo job pode processar multiplos processos e tabelas SOR.
