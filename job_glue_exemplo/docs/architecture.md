# Arquitetura

## Visão Geral

`job_glue_exemplo` usa uma arquitetura simples de job Glue configurável:

- Python coordena o ciclo de vida do job.
- JSON declara fontes e destino.
- Spark SQL concentra a transformação de dados.
- AWS Glue/Spark executa leitura, processamento e escrita.
- S3, Glue Catalog e DynamoDB são integrações externas.

O ponto de entrada é `src/main.py`, que instancia `JobGlue` e chama `executar_fluxo_de_processamento()`.

## Componentes

`JobGlue` em `src/main.py` é responsável por:

- resolver argumentos obrigatórios e opcionais do Glue;
- inicializar `SparkContext`, `GlueContext`, `SparkSession`, `Job` e client Glue;
- carregar predicates opcionais do DynamoDB;
- montar fontes a partir de `config_origem_dados.json`;
- registrar temp views para as fontes;
- executar o SQL configurado;
- gravar o DataFrame final no destino.

`src/utils/config_origem_dados.py` lê e valida a configuração técnica de leitura das origens, hoje restrita a `group_files` e `group_size_bytes` no bloco `catalogo`.

`src/utils/config_destino_dados.py` lê e consolida a configuração do destino. O campo `tipo_saida_destino` escolhe entre `catalogo` e `csv`, e o bloco `ambientes` define o `s3_destino` por ambiente.

`src/utils/executar_consultas_sql.py` separa o arquivo SQL por `;`, executa cada consulta em ordem e registra temp views intermediárias com prefixo `query_temp_`.

`src/utils/gravar_catalogo_glue.py` grava o DataFrame final. Em modo `catalogo`, escreve Parquet no S3 e atualiza o Glue Catalog. Em modo `csv`, escreve temporariamente no S3, localiza o arquivo `part-*` e promove para o nome final.

`src/utils/obter_predicate_override_ddb.py` consulta a tabela DynamoDB `glue_predicate_overrides` com chave `pk=DATA_REF` e `sk=JOB_NAME`.

## Configuração

`src/config/config_origem_dados.json` possui dois blocos de origem:

- `catalogo`: parâmetros de leitura do Glue Catalog e lista `fontes`;
- `csv`: lista `fontes` para leitura de CSV no S3.

Uma fonte de catálogo usa `database_origem`, `tabela_origem`, `descricao_origem` e opcionalmente `filtro_origem`.

Uma fonte CSV usa `caminho_s3_origem`, `nome_view`, `descricao_origem`, opções de CSV e opcionalmente `filtro_origem`.

`src/config/config_destino_dados.json` define:

- `tipo_saida_destino`: `catalogo` ou `csv`;
- parâmetros específicos de `catalogo`;
- parâmetros específicos de `csv`;
- `ambientes` com `s3_destino` para `dev`, `hom` e `prd`.

## Integrações AWS

- Glue Catalog: leitura de tabelas e atualização do catálogo no destino `catalogo`.
- S3: leitura de CSV, escrita de Parquet, escrita temporária e promoção de CSV final.
- DynamoDB: override opcional de predicates na tabela `glue_predicate_overrides`.
- boto3: criação de clients/resources para Glue, S3 e DynamoDB.

## Pontos de Extensão

- Adicionar novas fontes editando `config_origem_dados.json`, mantendo os nomes de views compatíveis com o SQL.
- Alterar transformação editando `src/sql/consultas.sql`.
- Trocar destino alterando `tipo_saida_destino` em `config_destino_dados.json`.
- Adicionar placeholders exige mudança em `montar_contexto_templates_origem()` e atualização da documentação.
- Adicionar novo tipo de origem ou destino exige mudança coordenada em `src/main.py`, utilitários de config/gravação e docs.
