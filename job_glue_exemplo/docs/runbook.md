# Runbook

## Quando Usar

Use este runbook para preparar mudanças, revisar falhas e validar comportamento do job Glue em `job_glue_exemplo`.

## Pré-requisitos

A execução completa depende de:

- AWS Glue;
- PySpark;
- boto3;
- permissões para Glue Catalog, S3 e opcionalmente DynamoDB;
- recursos externos declarados nos JSONs de configuração.

Hoje o diretório não possui `requirements.txt`, `pyproject.toml`, testes automatizados ou configuração formal de lint.

## Configurar Uma Execução

Confirme os argumentos obrigatórios do job:

- `JOB_NAME`
- `DATA_REF`
- `TIPO_PROCESSAMENTO`
- `GLUE_SQL_LOCATION`
- `AMBIENTE`
- `DBLOCAL`

Confirme se `DATA_REF` está em `YYYYMM` ou `YYYYMMDD`.

Revise as origens em:

```text
src/config/config_origem_dados.json
```

Revise o destino em:

```text
src/config/config_destino_dados.json
```

Revise a transformação em:

```text
src/sql/consultas.sql
```

## Validação Antes de Deploy

Faça uma revisão estática:

```bash
rg --files job_glue_exemplo
rg "DATA_REF|TABLE_PREDICATES|USAR_PREDICATE_OVERRIDE_DDB|tipo_saida_destino" job_glue_exemplo
git diff -- job_glue_exemplo
```

Verifique manualmente:

- Todas as temp views usadas no SQL são criadas pelas fontes ou por queries anteriores.
- O campo de partição configurado em `particao_tabela_destino` existe no DataFrame final.
- Os placeholders usados nas configs pertencem à lista suportada.
- O `AMBIENTE` informado existe no bloco `ambientes`.
- O modo `tipo_saida_destino` é `catalogo` ou `csv`.
- Paths S3 apontam para o ambiente correto.

## Falhas Comuns

Arquivo SQL não encontrado:

- Verifique `GLUE_SQL_LOCATION`.
- O código procura `{GLUE_SQL_LOCATION}/src/sql/consultas.sql`.

Placeholder desconhecido:

- Revise campos textuais em `config_origem_dados.json`.
- Use apenas placeholders registrados em `CONTEXT.md`.

Database ou tabela não encontrada:

- Verifique `database_origem` e `tabela_origem`.
- Se `AMBIENTE != "prd"` e `DBLOCAL == "true"`, o database usado será `dblocal`.

Predicate inválido:

- Revise `filtro_origem`, `TABLE_PREDICATES` e overrides no DynamoDB.
- Para `TABLE_PREDICATES`, use `tabela::predicate`; se houver mais de um, separe por `;`.

Erro em partição de destino:

- Confirme que `particao_tabela_destino` tem ao menos uma coluna.
- Confirme que todas as colunas configuradas existem no DataFrame final.

Erro no CSV final:

- O modo CSV espera encontrar exatamente um arquivo `part-*` na pasta temporária.
- O job usa `coalesce(1)`, mas falhas de escrita ou sobras externas podem quebrar a promoção.

## Cuidados Operacionais

- O modo `catalogo` remove partições de destino antes de regravá-las.
- O modo `csv` apaga o arquivo final antes de copiar o novo arquivo temporário.
- A tabela DynamoDB `glue_predicate_overrides` pode alterar filtros de leitura sem mudança no JSON.
- Mudanças em SQL podem alterar schema final e quebrar partições ou consumidores downstream.
- Mudanças em `config_destino_dados.json` podem direcionar escrita para outro bucket, database, tabela ou pasta.

## Checklist de Investigação

1. Identifique a etapa do log: carregamento de fontes, execução SQL ou gravação.
2. Confira argumentos recebidos pelo job.
3. Revise predicates efetivos nos logs.
4. Valide se as fontes criam as temp views esperadas.
5. Execute leitura estática do SQL em ordem, lembrando das views `query_temp_N`.
6. Confira schema final contra partições e destino.
7. Verifique permissões AWS e existência dos recursos externos.
