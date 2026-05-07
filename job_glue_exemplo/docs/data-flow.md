# Fluxo de Dados

## 1. Inicialização

Ao executar `src/main.py`, o job instancia `JobGlue`.

Durante a inicialização, o job resolve os argumentos obrigatórios:

- `JOB_NAME`
- `DATA_REF`
- `TIPO_PROCESSAMENTO`
- `GLUE_SQL_LOCATION`
- `AMBIENTE`
- `DBLOCAL`

Também tenta resolver os argumentos opcionais:

- `TABLE_PREDICATES`
- `USAR_PREDICATE_OVERRIDE_DDB`

Depois inicializa `SparkContext`, `GlueContext`, `SparkSession`, `Job` e client boto3 de Glue na região `us-east-2`.

## 2. Contexto de Datas

`DATA_REF` aceita dois formatos:

- `YYYYMM`
- `YYYYMMDD`

A partir dele, o job monta os placeholders permitidos para campos textuais das origens:

- `DATA_REF`
- `DATA_ATUAL`
- `ANO_DATA_REF`
- `MES_DATA_REF`
- `ANO_MES_DATA_REF`
- `MES_ANTERIOR`
- `ANO_MES_ANTERIOR`

Qualquer placeholder fora dessa lista gera erro.

## 3. Leitura das Origens

O job lê `src/config/config_origem_dados.json`, combina as listas de fontes dos blocos `catalogo` e `csv`, resolve placeholders e converte cada origem para o contrato interno de `carregar_catalogo()`.

Para origem `catalogo`:

- usa `database_origem` como database, exceto quando `AMBIENTE != "prd"` e `DBLOCAL == "true"`, caso em que usa `dblocal`;
- usa `tabela_origem` como nome da tabela e da temp view;
- lê via `glue_context.create_data_frame.from_catalog`;
- aplica `push_down_predicate` quando houver predicate final.

Para origem `csv`:

- usa `caminho_s3_origem`;
- usa `nome_view` como temp view;
- lê via `glue_context.create_dynamic_frame.from_options`;
- converte para DataFrame;
- aplica `.filter(predicate)` quando houver predicate final.

## 4. Resolução de Predicates

Para cada fonte, a precedência do predicate é:

1. `TABLE_PREDICATES`, quando informado no formato `tabela::predicate`;
2. DynamoDB, quando `USAR_PREDICATE_OVERRIDE_DDB=true` e existir item habilitado;
3. `filtro_origem` resolvido a partir do JSON.

O override via DynamoDB consulta a tabela `glue_predicate_overrides` com:

- `pk`: `DATA_REF`;
- `sk`: `JOB_NAME`.

O item precisa ter `enabled=true` e um mapa `predicates`.

## 5. SQL Sequencial

O caminho do SQL é montado como:

```text
{GLUE_SQL_LOCATION}/src/sql/consultas.sql
```

`executar_consultas_sql()` separa o arquivo por `;`, remove trechos vazios e executa cada consulta com `spark.sql()`.

Cada resultado intermediário é registrado como temp view:

- primeira consulta: `query_temp_1`;
- segunda consulta: `query_temp_2`;
- demais consultas seguem o mesmo padrão.

O DataFrame retornado é sempre o resultado da última consulta.

## 6. Enriquecimento Final

Depois do SQL, o job adiciona a coluna:

```text
chave_processamento_registro
```

O valor vem de `monotonically_increasing_id()`.

## 7. Escrita

O destino é escolhido por `tipo_saida_destino` em `src/config/config_destino_dados.json`.

No modo `catalogo`:

- consolida config específica do ambiente;
- valida `particao_tabela_destino`;
- monta o path `s3_destino/nome_tabela_destino/`;
- remove as partições que serão regravadas;
- escreve Parquet no S3;
- atualiza o Glue Catalog com `UPDATE_IN_DATABASE`.

No modo `csv`:

- monta a pasta final `s3_destino/nome_pasta_destino/`;
- escreve em uma pasta temporária `_tmp/`;
- força um único arquivo com `coalesce(1)`;
- localiza o único `part-*`;
- copia para `nome_arquivo_csv`;
- limpa a pasta temporária.
