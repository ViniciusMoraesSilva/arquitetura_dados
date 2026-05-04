# arquitetura_dados

MVP1 de uma arquitetura AWS para pipeline de dados em camadas SOR, SOT e SPEC.

O repositório implementa a base executável do fluxo descrito nas issues do GitHub: ingestão genérica versionada por `ingestion_id`, processamento FULL de `seguros` versionado por `process_id`, travas imutáveis de entrada em `INPUT_LOCK`, jobs Glue no padrão copiado/adaptado do JobGlue e publicação final rastreável.

## Arquitetura

- **SOR**: camada de origem refinada. Cada carga gera um `ingestion_id` e grava dados particionados por esse identificador.
- **SOT**: camada de processamento FULL. Módulos de `seguros` recebem somente `PROCESS_ID`, resolvem `INPUT_LOCK` no DynamoDB e gravam saídas por `process_id`.
- **SPEC**: camada final publicada. A publicação também recebe somente `PROCESS_ID`, preservando rastreabilidade até o FULL que gerou a saída.
- **DynamoDB `pipeline_control`**: plano de controle com chaves genéricas `PK` e `SK`, guardando `FULL_CONFIG`, metadata de ingestão, `CURRENT_SOR`, `PROCESS`, `PROCESS_LIST`, `INPUT_LOCK` e status de etapas.
- **Step Function FULL seguros**: recebe externamente apenas `data_referencia`, injeta `process_name=seguros`, chama a Lambda de preparação e executa módulos `x`, `y`, `z`, consolidação SOT e publicação SPEC.

## Contratos

- `ingestion_id`: versão imutável de uma carga SOR, no formato `ing_<uuid-curto>`.
- `process_id`: execução FULL imutável, no formato `proc_<process_name>_<YYYYMM>_<uuid-curto>`.
- `CURRENT_SOR#<table_name>#<data_referencia> / CURRENT`: ponteiro para a última SOR carregada com sucesso para uma tabela e referência.
- `FULL_CONFIG#seguros / TABLE#<table_name>`: lista de tabelas SOR exigidas pelo FULL de seguros.
- `PROCESS#<process_id> / INPUT_LOCK#<table_name>`: snapshot da entrada do processo, contendo o `ingestion_id` travado.

Glue Jobs não recebem `TABLE_PREDICATES` nem usam `glue_predicate_overrides` no contrato MVP1. Todos rodam pelo `main.py` copiado/adaptado do JobGlue com `--JOB_MODE=sor|process`: SOR usa `--INGESTION_ID`; módulos FULL usam `--PROCESS_ID` mais argumentos técnicos como `--MODULE_CONFIG`, `--GLUE_SQL_LOCATION`, `--AMBIENTE` e `--CONTROL_TABLE_NAME`. O predicate de leitura SOR é derivado de `INPUT_LOCK` e usa apenas `ingestion_id`.

## Estrutura

- `src/shared/`: helpers de IDs, tempo e plano de controle DynamoDB.
- `src/lambdas/`: handlers de preparação/finalização de ingestão, preparação FULL e status de etapa.
- `src/glue/carga_sor_generica/`: helpers de contrato da carga SOR genérica para Glue Catalog e CSV/S3.
- `src/glue/jobglue_process_module/`: cópia/adaptação local de `jobglue/src`, com `main.py`, `utils/`, `config/` e `sql/` como padrão canônico de todos os Glue Jobs.
- `src/glue/modulos_seguros/`: configs e SQL placeholders dos módulos `x`, `y`, `z`, consolidação e publicação.
- `infra/`: Terraform do MVP1.

O repositório externo de JobGlue não deve ser modificado. Este repo mantém uma cópia/adaptação local de `src/` e testes de contrato adaptados; Terraform do repo externo e arquivos locais/sensíveis não fazem parte da cópia.

## Terraform

O ambiente inicial é `dev` e a região padrão é `us-east-2`.

```bash
cd infra
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -var-file=envs/dev.tfvars.example
```

O Terraform cria:

- Bucket S3 por ambiente com prefixos `sor/`, `sot/`, `spec/`, `scripts/` e `tmp/`.
- DynamoDB `pipeline_control_<env>` com `PK` e `SK`.
- Seeds `FULL_CONFIG#seguros / TABLE#contratos`, `clientes` e `parcelas`.
- Glue databases SOR, SOT e SPEC.
- IAM mínimo para Lambda, Glue e Step Functions.
- Upload inicial da cópia JobGlue para `scripts/jobglue/` e das configs dos módulos para `scripts/modulos_seguros/`.
- Glue Jobs da carga SOR genérica, módulos `x/y/z`, consolidação e publicação, todos apontando para `scripts/jobglue/main.py`.
- Step Function FULL de seguros.

## Payloads

Preparar ingestão via Glue Catalog:

```json
{
  "database_name": "raw",
  "table_name": "contratos",
  "data_referencia": "202604",
  "source_type": "glue_catalog",
  "partition_values": {
    "ano_mes": "202604"
  }
}
```

Preparar ingestão CSV/S3:

```json
{
  "table_name": "clientes",
  "data_referencia": "202604",
  "source_type": "csv_s3",
  "s3_path": "s3://landing/clientes/202604/clientes.csv",
  "csv_options": {
    "header": true,
    "sep": ";"
  }
}
```

Executar FULL seguros:

```json
{
  "data_referencia": "202604"
}
```

## Testes

```bash
pytest -q
```

A suite cobre os contratos principais: geração de IDs, preparação/finalização de ingestão, CSV/S3 sem acoplamento a produto, preparo FULL com locks imutáveis, `JOB_MODE=sor|process`, resolução `PROCESS_ID -> INPUT_LOCK -> ingestion_id` e payload da Step Function para módulos.

## Evoluindo placeholders

Quando os nomes reais de seguros forem definidos:

1. Troque `contratos`, `clientes` e `parcelas` em `infra/envs/dev.tfvars.example` ou na variável `full_config_tables`.
2. Atualize as configs em `src/glue/modulos_seguros/*/config.json`.
3. Ajuste os SQLs em `src/glue/modulos_seguros/*/sql.sql`.
4. Rode `pytest -q` e depois os comandos Terraform de validação.

Mantenha o contrato externo dos módulos estável: `--PROCESS_ID` entra como identificador de execução, argumentos técnicos só localizam config/código, `INPUT_LOCK` resolve as versões SOR, e a saída permanece rastreável por `process_id`.
