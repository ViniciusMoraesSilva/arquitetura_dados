# Contexto do Projeto

`job_glue_exemplo` é um exemplo de job AWS Glue orientado por configuração. Ele demonstra um fluxo de ETL/ELT em PySpark no qual origens e destino são declarados em arquivos JSON, a transformação principal fica em SQL e a orquestração fica em Python.

## Objetivo

Executar um job Glue que:

1. resolve argumentos de execução;
2. monta um contexto de datas a partir de `DATA_REF`;
3. lê fontes do Glue Catalog e/ou CSV no S3;
4. aplica predicates de leitura;
5. registra as fontes como temp views Spark;
6. executa `src/sql/consultas.sql` em sequência;
7. adiciona `chave_processamento_registro`;
8. grava o resultado em Glue Catalog/Parquet ou CSV no S3.

## Linguagem de Domínio

- Job Glue: unidade de execução AWS Glue inicializada por `JobGlue`.
- Origem: fonte declarada em `config_origem_dados.json`, podendo vir de `catalogo` ou `csv`.
- Fonte de catálogo: tabela lida via `glue_context.create_data_frame.from_catalog`.
- Fonte CSV: arquivo ou pasta no S3 lida via `create_dynamic_frame.from_options`.
- Temp view: view Spark criada por `createOrReplaceTempView` para uso nas queries SQL.
- Predicate: filtro aplicado na leitura da origem. Pode vir do JSON, argumento do job ou DynamoDB.
- `DATA_REF`: data de referência da execução, aceita como `YYYYMM` ou `YYYYMMDD`.
- Destino: saída final declarada em `config_destino_dados.json`, com modo `catalogo` ou `csv`.
- Ambiente: valor de `AMBIENTE`, usado para escolher o `s3_destino` entre `dev`, `hom` e `prd`.
- `DBLOCAL`: flag que, fora de `prd`, força leitura de catálogo no database `dblocal`.

## Argumentos

Obrigatórios:

- `JOB_NAME`: nome do job Glue.
- `DATA_REF`: data de referência da execução.
- `TIPO_PROCESSAMENTO`: tipo de processamento, como `producao` ou `simulacao`; hoje é resolvido como argumento, mas não direciona ramificações no código.
- `GLUE_SQL_LOCATION`: local base usado para montar o caminho de `src/sql/consultas.sql`.
- `AMBIENTE`: ambiente de execução, esperado em `dev`, `hom` ou `prd` conforme config atual.
- `DBLOCAL`: ativa database local em `dev` e `hom` quando igual a `true`.

Opcionais:

- `TABLE_PREDICATES`: lista de overrides no formato `tabela::predicate`, separada por `;`.
- `USAR_PREDICATE_OVERRIDE_DDB`: quando `true`, busca overrides na tabela DynamoDB `glue_predicate_overrides`.

## Invariantes

- O fluxo principal tem três etapas: carregar fontes, executar consultas e gravar resultado.
- O SQL final é o resultado da última consulta executada em `consultas.sql`.
- Cada consulta SQL intermediária é registrada como `query_temp_1`, `query_temp_2` e assim por diante.
- A coluna `chave_processamento_registro` é adicionada após a execução das consultas SQL.
- O modo `catalogo` valida partições antes de gravar e remove o conteúdo das partições que serão regravadas.
- O modo `csv` escreve em pasta temporária, promove um único arquivo `part-*` para o nome final configurado e limpa a pasta temporária.
- Placeholders desconhecidos nos campos textuais da origem geram erro.

## Limites Atuais

- Não há testes automatizados no diretório.
- Não há `requirements.txt`, `pyproject.toml` ou configuração formal de lint/test.
- A execução completa depende de AWS Glue, PySpark, boto3, permissões AWS e recursos externos como S3, Glue Catalog e opcionalmente DynamoDB.
