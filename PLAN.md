# MVP1 AWS Pipeline SOR/SOT/SPEC

## Summary
Criar neste repositório uma arquitetura AWS completa para MVP1, com pastas separadas por recurso, sem alterar `/Users/macbookpro/Documents/git/jobglue`. O `jobglue/src` será copiado/adaptado localmente como padrão canônico dos Glue Jobs: `main.py`, `utils/`, `config/` e `sql/` serão preservados, excluindo Terraform externo, caches e arquivos locais. SOR roda com `JOB_MODE=sor` e `INGESTION_ID`; FULL roda com `JOB_MODE=process`, `PROCESS_ID`, `INPUT_LOCK`, leitura SOR por `ingestion_id` e escrita SOT/SPEC por `process_id`.

Defaults decididos:
- Região AWS: `us-east-2`.
- Ambiente inicial: `dev`, parametrizado por `environment`.
- `data_referencia`: formato `YYYYMM`, exemplo `202604`.
- S3: um bucket por ambiente com prefixos `sor/`, `sot/`, `spec/`, `scripts/`, `tmp/`.
- Orquestração FULL: uma Step Function chama Lambda de preparo e Glue Jobs sequenciais diretamente.
- Tabelas/módulos de seguros: placeholders configuráveis, inicialmente `contratos`, `clientes`, `parcelas` e módulos x/y/z.

## JOB Glue: Pontos Que Precisam Ser Adaptados Localmente
- [src/main.py](/Users/macbookpro/Documents/git/jobglue/src/main.py:59): hoje exige `DATA_REF`, `TIPO_PROCESSAMENTO`, `GLUE_SQL_LOCATION`, `AMBIENTE`, `DBLOCAL`; a versão local deve aceitar `PROCESS_ID` como argumento principal dos módulos.
- [src/main.py](/Users/macbookpro/Documents/git/jobglue/src/main.py:151): hoje lê predicates de `glue_predicate_overrides` por `DATA_REF + JOB_NAME`; a versão local deve consultar `pipeline_control` em `PROCESS#<process_id> / INPUT_LOCK#<table>`.
- [src/utils/obter_predicate_override_ddb.py](/Users/macbookpro/Documents/git/jobglue/src/utils/obter_predicate_override_ddb.py:20): hardcoded em `glue_predicate_overrides`; substituir localmente por helper de `pipeline_control`.
- [src/main.py](/Users/macbookpro/Documents/git/jobglue/src/main.py:352): há `df.count()` em leitura, contrário ao plano MVP1 para evitar custo; remover na cópia local.
- [src/utils/gravar_catalogo_glue.py](/Users/macbookpro/Documents/git/jobglue/src/utils/gravar_catalogo_glue.py:174): gravação atual faz purge por partição de destino; na cópia local, garantir particionamento por `process_id` para SOT/SPEC e evitar sobrescrever processos anteriores.
- [terraform/main.tf](/Users/macbookpro/Documents/git/jobglue/terraform/main.tf:5): Terraform atual só cria `glue_predicate_overrides`; neste repo o Terraform deve criar a infra completa do MVP1.

## Key Changes
- Criar estrutura:
  - `infra/` com módulos Terraform por recurso: `dynamodb`, `s3`, `iam`, `lambda`, `glue`, `stepfunctions`, `catalog`.
  - `src/shared/` com `dynamodb_control.py`, `ids.py`, `time_utils.py`.
  - `src/lambdas/` com preparo/finalização de ingestão, preparo FULL genérico e atualização de status.
  - `src/glue/carga_sor_generica/` para carga SOR.
  - `src/glue/jobglue_process_module/` como cópia adaptada do JOB Glue.
  - `src/glue/modulos_seguros/` com configs/SQL separados por módulo placeholder.
- Terraform provisiona:
  - DynamoDB `pipeline_control` com PK/SK e itens `FULL_CONFIG#seguros`.
  - Bucket S3 por ambiente.
  - Glue databases para SOR/SOT/SPEC.
  - Glue Jobs para carga SOR e módulos FULL.
  - Lambdas Python com env vars.
  - IAM roles/policies mínimos para Lambda, Glue e Step Functions.
  - Step Function `sfn-orq-full-seguros-<env>`.
- Contratos principais:
  - Ingestão recebe `database_name`, `table_name`, `data_referencia=YYYYMM`, `source_type`, `partition_values` ou config CSV.
  - Glue SOR recebe `--INGESTION_ID`.
  - FULL externo recebe `{ "data_referencia": "202604" }`.
  - Módulos recebem somente `--PROCESS_ID`.
  - Leitura SOR dos módulos usa predicate `ingestion_id='<id>'`, derivado do `INPUT_LOCK`.

## Implementation Plan
1. Criar fundação Python compartilhada:
   - Cliente DynamoDB usando `CONTROL_TABLE_NAME`.
   - Funções para `INGESTION_ID`, `CURRENT_SOR`, `FULL_CONFIG`, `PROCESS`, `INPUT_LOCK`.
   - IDs com prefixos `ing_` e `proc_<process_name>_<data_referencia>_<uuid-curto>`.
2. Implementar Lambdas:
   - `preparar_ingestao`: cria metadata `PROCESSING` e predicate de origem.
   - `finalizar_carga_sor_sucesso`: marca `SOR_LOADED`, grava histórico e atualiza `CURRENT_SOR`.
   - `finalizar_carga_sor_falha`: marca `FAILED` sem mexer em `CURRENT_SOR`.
   - `preparar_full_generica`: cria `PROCESS`, valida todas as entradas, cria locks idempotentes e retorna `process_id`.
   - `atualizar_step_status`: atualiza status de processo/job para uso pela Step Function.
3. Implementar Glue:
   - `carga_sor_generica` lê Catalog ou CSV, adiciona `ingestion_id` e `dt_ingestao_sor`, grava em `sor/<table>/ingestion_id=<id>/`.
   - `jobglue_process_module` reaproveita o padrão SQL/config do JOB Glue, mas resolve fontes SOR via `PROCESS_ID`.
   - Configs de módulos ficam em pastas separadas, com SQL e destino próprios.
4. Implementar Terraform:
   - `infra/envs/dev.tfvars.example`.
   - Módulos reutilizáveis e root module com variáveis `environment`, `aws_region`, `project_name`, `full_config_tables`.
   - Upload de scripts Glue para `s3://<bucket>/scripts/...`.
   - State backend local por padrão; backend remoto deixado documentado, não obrigatório no MVP1.
5. Documentar execução:
   - README com ordem: `terraform init`, `plan`, `apply`, payloads de ingestão e FULL, e como trocar placeholders reais.

## Test Plan
- Unit tests Python para:
  - criação de IDs e normalização `YYYYMM`.
  - helpers DynamoDB com mocks.
  - preparo de ingestão e finalização SOR.
  - preparo FULL com todas as tabelas presentes, tabela ausente e lock duplicado.
  - resolvedor do JOB Glue local: `PROCESS_ID -> INPUT_LOCK -> ingestion_id predicate`.
- Terraform:
  - `terraform fmt -check`.
  - `terraform validate`.
  - `terraform plan -var-file=envs/dev.tfvars.example` sem aplicar.
- Integração local/mocks:
  - simular DynamoDB items de `CURRENT_SOR`.
  - validar payload da Step Function passando só `process_id` para Glue Jobs.
  - validar que módulos não usam `data_referencia` no predicate de leitura SOR.

## Assumptions
- Não modificar `/Users/macbookpro/Documents/git/jobglue`; apenas referenciar e adaptar cópia local.
- MVP1 pode usar placeholders para domínio de seguros até nomes reais serem definidos.
- `TABLE_PREDICATES` e `glue_predicate_overrides` ficam fora do fluxo principal do MVP1; a cópia local usa `JOB_MODE=sor|process`.
- O primeiro ambiente provisionado é `dev` em `us-east-2`.
- A publicação SPEC no MVP1 grava saída particionada por `process_id`, sem política avançada de versionamento além do próprio controle no DynamoDB.
