# Guia Para Agents

Este diretório contém um exemplo de job AWS Glue em Python/PySpark. O job lê fontes declaradas em JSON, registra views temporárias no Spark, executa consultas SQL sequenciais e grava o resultado em Glue Catalog/Parquet ou CSV no S3.

Leia este arquivo antes de alterar qualquer coisa em `job_glue_exemplo`.

## Mapa Rápido

- `src/main.py`: orquestrador principal do job Glue. Centraliza argumentos, contexto Spark/Glue, leitura de fontes, execução SQL e gravação.
- `src/config/config_origem_dados.json`: contrato declarativo das origens lidas do Glue Catalog e/ou CSV no S3.
- `src/config/config_destino_dados.json`: contrato declarativo do destino final, incluindo modo `catalogo` ou `csv`.
- `src/sql/consultas.sql`: consultas Spark SQL executadas em sequência. Cada consulta vira uma temp view intermediária `query_temp_N`.
- `src/utils/`: funções auxiliares para leitura de configs, execução SQL, gravação no Glue Catalog/S3 e override de predicates no DynamoDB.
- `CONTEXT.md`: vocabulário e invariantes de domínio.
- `docs/architecture.md`: arquitetura técnica e responsabilidades.
- `docs/data-flow.md`: fluxo de dados ponta a ponta.
- `docs/runbook.md`: operação, validação e investigação de falhas.
- `docs/adr/0001-config-driven-glue-job.md`: decisão arquitetural sobre configuração declarativa.

## Regras de Trabalho

- Trate `src/main.py` como o orquestrador do fluxo. Evite espalhar regras de execução em módulos auxiliares sem necessidade clara.
- Preserve os contratos dos JSONs em `src/config/`. Se renomear campos, adicionar obrigatoriedade ou mudar valores esperados, atualize docs e código juntos.
- Mantenha o SQL compatível com as temp views registradas pelas origens. Para fontes de catálogo, a view usa `tabela_origem`; para CSV, usa `nome_view`.
- Não assuma que a execução local completa funciona fora de um ambiente com AWS Glue, PySpark, boto3 e permissões AWS configuradas.
- Evite alterações destrutivas em paths S3, nomes de database/tabela, partições, predicates e modo de escrita. Esses valores afetam dados reais quando executados em ambiente AWS.
- Não introduza dependências novas sem registrar como instalar e validar, já que hoje não há `requirements.txt`, `pyproject.toml` ou configuração formal de tooling.
- Atualize `CONTEXT.md` e `docs/` sempre que mudar argumentos Glue, fluxo, configs, SQL, destino ou comportamento de leitura/escrita.

## Contratos Importantes

Argumentos obrigatórios resolvidos pelo Glue:

- `JOB_NAME`
- `DATA_REF`
- `TIPO_PROCESSAMENTO`
- `GLUE_SQL_LOCATION`
- `AMBIENTE`
- `DBLOCAL`

Argumentos opcionais:

- `TABLE_PREDICATES`: formato `tabela::predicate`, separado por `;` para múltiplas tabelas.
- `USAR_PREDICATE_OVERRIDE_DDB`: habilita consulta à tabela DynamoDB `glue_predicate_overrides` quando o valor é `true`.

Precedência do predicate de leitura:

1. override via `TABLE_PREDICATES`;
2. override via DynamoDB;
3. `filtro_origem` definido no JSON de origem.

Placeholders aceitos em campos textuais de origem:

- `DATA_REF`
- `DATA_ATUAL`
- `ANO_DATA_REF`
- `MES_DATA_REF`
- `ANO_MES_DATA_REF`
- `MES_ANTERIOR`
- `ANO_MES_ANTERIOR`

## Comandos Seguros

Use comandos de leitura para entender o projeto:

```bash
rg --files job_glue_exemplo
sed -n '1,260p' job_glue_exemplo/src/main.py
sed -n '1,220p' job_glue_exemplo/src/config/config_origem_dados.json
sed -n '1,220p' job_glue_exemplo/src/config/config_destino_dados.json
```

Para revisar documentação ou mudanças:

```bash
git diff -- job_glue_exemplo
rg "DATA_REF|TABLE_PREDICATES|tipo_saida_destino|query_temp_" job_glue_exemplo
```

Não rode comandos que escrevam em S3, Glue Catalog ou DynamoDB sem confirmação explícita do usuário.

## Checklist Antes de Alterar

- Entendeu qual fonte gera cada temp view usada em `src/sql/consultas.sql`.
- Conferiu se `DATA_REF` pode estar em formato `YYYYMM` ou `YYYYMMDD`.
- Verificou se a alteração impacta `catalogo`, `csv` ou ambos.
- Confirmou se novas colunas de partição existem no DataFrame final.
- Atualizou a documentação relevante junto com mudanças de comportamento.
- Considerou que não há testes automatizados nem tooling formal neste diretório.
