# Plano para usar no Codex — MVP1 Pipeline SOR/SOT/SPEC

## Correção de rumo — padrão JobGlue copiado

Todos os Glue Jobs do MVP1 devem seguir o padrão copiado/adaptado de `/Users/macbookpro/Documents/git/jobglue/src`. A cópia local fica neste repositório, sem alterar o repo externo e sem copiar a pasta `terraform/`, `.git/`, `.env`, caches ou artefatos locais.

O pacote operacional copiado/adaptado deve preservar:

```text
main.py
utils/
config/
sql/
```

O `main.py` canônico usa `JOB_MODE`:

```text
JOB_MODE=sor      -> usa INGESTION_ID e metadata do pipeline_control
JOB_MODE=process  -> usa PROCESS_ID e resolve SOR via INPUT_LOCK
```

`PROCESS_ID` e `INGESTION_ID` são os identificadores de execução. Argumentos técnicos como `JOB_NAME`, `MODULE_CONFIG`, `GLUE_SQL_LOCATION`, `AMBIENTE` e `CONTROL_TABLE_NAME` são permitidos apenas para localizar código/configuração/ambiente. `TABLE_PREDICATES`, `glue_predicate_overrides` e leitura SOR por `data_referencia` ficam fora do contrato principal do MVP1.

## Contexto do MVP1

Estamos construindo o MVP1 de um pipeline de dados na AWS com as camadas:

```text
SOR  = camada de recepção/versionamento por ingestion_id
SOT  = camada intermediária/processamento por process_id
SPEC = camada final publicada
```

O MVP1 não terá site. A execução será por Step Functions, Lambda, Glue Jobs, DynamoDB, S3 e Glue Catalog.

A ideia principal:

```text
1. Ingestão genérica carrega tabelas da origem para SOR.
2. Cada carga gera um ingestion_id.
3. DynamoDB mantém CURRENT_SOR por tabela e data_referencia.
4. O FULL cria um process_id.
5. O FULL busca no DynamoDB quais tabelas precisa travar via FULL_CONFIG.
6. Para cada tabela, copia o CURRENT_SOR para INPUT_LOCK.
7. Os módulos recebem só process_id.
8. Os módulos leem SOR usando somente ingestion_id.
9. Os módulos gravam SOT usando process_id.
10. Consolidação e publicação usam process_id.
```

---

## Regras importantes

### Ingestão

A ingestão **não conhece processo/produto**.

Payload de entrada da ingestão:

```json
{
  "database_name": "db_contratos",
  "table_name": "contratos",
  "data_referencia": "2026-04",
  "partition_values": {
    "data_referencia": "2026-04"
  }
}
```

A ingestão deve criar no DynamoDB:

```text
INGESTION_ID#<ingestion_id> / METADATA
INGESTION_HISTORY#<table_name>#<data_referencia> / INGESTION#<ingestion_id>
CURRENT_SOR#<table_name>#<data_referencia> / CURRENT
```

### FULL

A Step Functions FULL será específica por processo/produto.

Exemplo:

```text
sfn-orq-full-seguros
```

Payload externo:

```json
{
  "data_referencia": "2026-04"
}
```

A própria Step Functions injeta `process_name = "seguros"` ao chamar a Lambda genérica de preparação.

Payload para a Lambda genérica:

```json
{
  "process_name": "seguros",
  "data_referencia": "2026-04"
}
```

A Lambda deve consultar:

```text
FULL_CONFIG#seguros
```

E criar os locks:

```text
PROCESS#<process_id> / INPUT_LOCK#<table_name>
```

### Leitura da SOR pelos módulos

O módulo recebe:

```json
{
  "process_id": "proc_seguros_202604_001"
}
```

O módulo consulta DynamoDB para obter:

```text
PROCESS#proc_seguros_202604_001 / INPUT_LOCK#contratos
```

E lê a SOR usando **somente**:

```text
ingestion_id='<ingestion_id>'
```

Não usar `data_referencia` no predicate dos módulos ao ler SOR.

---

## Estrutura desejada no repositório

Criar ou ajustar para algo próximo disso:

```text
infra/
  dynamodb.tf
  dynamodb_full_config.tf

src/
  lambdas/
    preparar_ingestao/
      app.py
    finalizar_carga_sor_sucesso/
      app.py
    finalizar_carga_sor_falha/
      app.py
    preparar_full_generica/
      app.py
    atualizar_step_status/
      app.py

  glue/
    carga_sor_generica/
      job.py
    modulo_x_seguros/
      job.py
    modulo_y_seguros/
      job.py
    modulo_z_seguros/
      job.py
    consolidacao_sot_seguros/
      job.py
    publicacao_spec_seguros/
      job.py

  shared/
    dynamodb_control.py
    ids.py
    time_utils.py
```

Se o repo já tiver outra estrutura, adaptar mantendo o mínimo de impacto.

---

# Fase 1 — Terraform DynamoDB e FULL_CONFIG

## Objetivo

Criar a tabela `pipeline_control` e cadastrar as tabelas obrigatórias do FULL de seguros via Terraform.

## Arquivo: `infra/dynamodb.tf`

```hcl
resource "aws_dynamodb_table" "pipeline_control" {
  name         = "pipeline_control"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "PK"
  range_key = "SK"

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "SK"
    type = "S"
  }

  tags = {
    Project = "pipeline-mvp1"
    Layer   = "control-plane"
  }
}
```

## Arquivo: `infra/dynamodb_full_config.tf`

```hcl
locals {
  full_configs = {
    seguros = [
      "contratos",
      "clientes",
      "parcelas"
    ]
  }

  full_config_items = flatten([
    for process_name, tables in local.full_configs : [
      for table_name in tables : {
        process_name = process_name
        table_name   = table_name
      }
    ]
  ])
}

resource "aws_dynamodb_table_item" "full_config" {
  for_each = {
    for item in local.full_config_items :
    "${item.process_name}-${item.table_name}" => item
  }

  table_name = aws_dynamodb_table.pipeline_control.name
  hash_key   = aws_dynamodb_table.pipeline_control.hash_key
  range_key  = aws_dynamodb_table.pipeline_control.range_key

  item = jsonencode({
    PK = {
      S = "FULL_CONFIG#${each.value.process_name}"
    }

    SK = {
      S = "TABLE#${each.value.table_name}"
    }

    process_name = {
      S = each.value.process_name
    }

    table_name = {
      S = each.value.table_name
    }
  })
}
```

## Resultado esperado no DynamoDB

```text
FULL_CONFIG#seguros / TABLE#contratos
FULL_CONFIG#seguros / TABLE#clientes
FULL_CONFIG#seguros / TABLE#parcelas
```

---

# Fase 2 — Biblioteca comum DynamoDB

## Objetivo

Criar funções reutilizáveis para consultar e atualizar o DynamoDB.

## Arquivo: `src/shared/dynamodb_control.py`

Implementar funções genéricas:

```python
get_item(pk: str, sk: str) -> dict | None
put_item(item: dict, condition_expression: str | None = None) -> None
update_item(pk: str, sk: str, update_expression: str, names: dict, values: dict) -> dict
query_by_pk(pk: str) -> list[dict]
```

Funções específicas:

```python
get_full_config(process_name: str) -> list[str]
get_current_sor(table_name: str, data_referencia: str) -> str
create_input_lock(process_id: str, table_name: str, data_referencia: str, ingestion_id: str) -> None
get_input_lock(process_id: str, table_name: str) -> dict
```

Regras:

```text
1. Usar boto3.
2. Nome da tabela vem da variável de ambiente CONTROL_TABLE_NAME.
3. Usar ConditionExpression ao criar INPUT_LOCK para evitar sobrescrita.
4. Se item obrigatório não existir, lançar exceção clara.
```

Exemplo de `get_full_config`:

```python
from boto3.dynamodb.conditions import Key

def get_full_config(process_name: str) -> list[str]:
    response = table.query(
        KeyConditionExpression=Key("PK").eq(f"FULL_CONFIG#{process_name}")
    )
    return [item["table_name"] for item in response.get("Items", [])]
```

Exemplo de `get_current_sor`:

```python
def get_current_sor(table_name: str, data_referencia: str) -> str:
    response = table.get_item(
        Key={
            "PK": f"CURRENT_SOR#{table_name}#{data_referencia}",
            "SK": "CURRENT"
        }
    )

    item = response.get("Item")
    if not item:
        raise ValueError(
            f"CURRENT_SOR não encontrado para table_name={table_name}, "
            f"data_referencia={data_referencia}"
        )

    return item["current_ingestion_id"]
```

---

# Fase 3 — Lambda preparar ingestão

## Objetivo

Criar a Lambda que recebe evento de tabela, gera `ingestion_id`, monta predicate simples e grava `INGESTION_ID`.

## Entrada

```json
{
  "database_name": "db_contratos",
  "table_name": "contratos",
  "data_referencia": "2026-04",
  "partition_values": {
    "data_referencia": "2026-04"
  }
}
```

## Regras

```text
1. Não receber process_name.
2. Gerar ingestion_id único.
3. Montar source_push_down_predicate a partir de partition_values.
4. Criar INGESTION_ID#<ingestion_id>/METADATA com status PROCESSING.
5. Retornar ingestion_id.
```

## Item criado

```json
{
  "PK": "INGESTION_ID#ing_100",
  "SK": "METADATA",
  "ingestion_id": "ing_100",
  "source_database_name": "db_contratos",
  "source_table_name": "contratos",
  "target_table_name": "contratos",
  "data_referencia": "2026-04",
  "source_push_down_predicate": "data_referencia='2026-04'",
  "status": "PROCESSING",
  "created_at": "2026-04-25T10:00:00Z"
}
```

---

# Fase 4 — Glue carga SOR

## Objetivo

Criar ou ajustar `glue-carga-sor-generica`.

## Entrada

Parâmetro:

```text
--INGESTION_ID ing_100
```

## Regras

```text
1. Consultar INGESTION_ID#<ingestion_id> / METADATA.
2. Ler source_database_name/source_table_name com source_push_down_predicate.
3. Adicionar ingestion_id e dt_ingestao_sor.
4. Gravar na SOR.
5. A SOR pode ser particionada por ingestion_id.
6. Não precisa fazer count.
```

## Observação

Não adicionar `row_count` no MVP1 para evitar custo de `df.count()`.

---

# Fase 5 — Lambdas finalizar carga SOR

## Sucesso

Criar ou ajustar `lambda-finalizar-carga-sor-sucesso`.

Entrada:

```json
{
  "ingestion_id": "ing_100"
}
```

A Lambda deve:

```text
1. Buscar INGESTION_ID#ing_100 / METADATA.
2. Atualizar status para SOR_LOADED.
3. Criar INGESTION_HISTORY#<table_name>#<data_referencia>/INGESTION#<ingestion_id>.
4. Atualizar CURRENT_SOR#<table_name>#<data_referencia>/CURRENT.
```

Exemplo `CURRENT_SOR`:

```json
{
  "PK": "CURRENT_SOR#contratos#2026-04",
  "SK": "CURRENT",
  "table_name": "contratos",
  "data_referencia": "2026-04",
  "current_ingestion_id": "ing_100",
  "status": "SOR_LOADED",
  "updated_at": "2026-04-25T10:10:00Z"
}
```

## Falha

Criar ou ajustar `lambda-finalizar-carga-sor-falha`.

A Lambda deve:

```text
1. Atualizar INGESTION_ID para FAILED.
2. Gravar error_message.
3. Não atualizar CURRENT_SOR.
```

---

# Fase 6 — Lambda preparar FULL genérica

## Objetivo

Criar `lambda-preparar-full-generica`.

## Entrada

```json
{
  "process_name": "seguros",
  "data_referencia": "2026-04"
}
```

## Regras

```text
1. Buscar FULL_CONFIG#seguros.
2. Para cada tabela, buscar CURRENT_SOR#<table_name>#<data_referencia>.
3. Se faltar alguma tabela, criar PROCESS com status FAILED_INPUT_MISSING.
4. Se tudo existir, criar process_id.
5. Criar PROCESS#<process_id>/HEADER.
6. Criar PROCESS_LIST#<process_name>#<data_referencia>/PROCESS#<process_id>.
7. Criar INPUT_LOCK para todas as tabelas.
8. Atualizar status para RUNNING.
9. Retornar process_id.
```

## Exemplo process_id

```text
proc_seguros_202604_001
```

Pode ser também com epoch/uuid:

```text
proc_seguros_202604_1777123456_a8f3
```

## Header

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "HEADER",
  "process_id": "proc_seguros_202604_001",
  "process_name": "seguros",
  "data_referencia": "2026-04",
  "execution_type": "FULL",
  "status": "LOCKING_INPUTS",
  "created_at": "2026-04-25T11:00:00Z"
}
```

## Process list

```json
{
  "PK": "PROCESS_LIST#seguros#2026-04",
  "SK": "PROCESS#proc_seguros_202604_001",
  "process_id": "proc_seguros_202604_001",
  "process_name": "seguros",
  "data_referencia": "2026-04",
  "execution_type": "FULL",
  "status": "LOCKING_INPUTS",
  "created_at": "2026-04-25T11:00:00Z"
}
```

## Input lock

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "INPUT_LOCK#contratos",
  "process_id": "proc_seguros_202604_001",
  "table_name": "contratos",
  "data_referencia": "2026-04",
  "ingestion_id": "ing_100",
  "locked_at": "2026-04-25T11:00:05Z"
}
```

---

# Fase 7 — Step Functions FULL seguros

## Objetivo

Criar/ajustar `sfn-orq-full-seguros`.

## Entrada externa

```json
{
  "data_referencia": "2026-04"
}
```

## Primeiro passo

Chamar `lambda-preparar-full-generica` com:

```json
{
  "process_name": "seguros",
  "data_referencia.$": "$.data_referencia"
}
```

## Depois

Passar para cada módulo:

```json
{
  "process_id.$": "$.process_id"
}
```

Fluxo:

```text
lambda-preparar-full-generica
   ↓
sfn-modulo-x-seguros
   ↓
sfn-modulo-y-seguros
   ↓
sfn-modulo-z-seguros
   ↓
sfn-consolidacao-sot-seguros
   ↓
sfn-publicacao-spec-seguros
```

---

# Fase 8 — Glue módulos

## Objetivo

Ajustar os módulos para receber apenas `process_id`.

Exemplo:

```text
--PROCESS_ID proc_seguros_202604_001
```

## Regra

Cada job sabe internamente:

```text
quais tabelas lê
qual database lê
onde grava na SOT
qual regra executa
```

O job consulta o DynamoDB somente para descobrir `ingestion_id`.

## Exemplo módulo X

O job sabe que precisa de:

```text
contratos
clientes
```

Busca:

```text
PROCESS#proc_seguros_202604_001 / INPUT_LOCK#contratos
PROCESS#proc_seguros_202604_001 / INPUT_LOCK#clientes
```

Lê a SOR usando somente:

```text
ingestion_id='ing_100'
```

Não usar `data_referencia` no predicate de leitura da SOR.

---

# Fase 9 — SOT e SPEC

## SOT

Os módulos gravam com:

```text
process_id
data_referencia
dt_processamento
```

O principal filtro depois é:

```text
process_id='<process_id>'
```

## Consolidação

A consolidação recebe:

```json
{
  "process_id": "proc_seguros_202604_001"
}
```

Lê SOT filtrando por `process_id`.

## Publicação SPEC

A publicação recebe:

```json
{
  "process_id": "proc_seguros_202604_001"
}
```

Lê SOT consolidada por `process_id` e grava SPEC.

---

# Fase 10 — Status de etapas

Criar função/Lambda para registrar status:

```text
PROCESS#<process_id> / STEP#MODULO_X
PROCESS#<process_id> / STEP#MODULO_Y
PROCESS#<process_id> / STEP#MODULO_Z
PROCESS#<process_id> / STEP#CONSOLIDATION
PROCESS#<process_id> / STEP#PUBLICATION
```

Exemplo:

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "STEP#MODULO_X",
  "process_id": "proc_seguros_202604_001",
  "step_name": "MODULO_X",
  "status": "RUNNING",
  "started_at": "2026-04-25T11:10:00Z"
}
```

Ao terminar:

```json
{
  "status": "SUCCESS",
  "finished_at": "2026-04-25T11:20:00Z"
}
```

---

# Critérios de aceite

O MVP1 estará pronto quando:

```text
1. Terraform cria pipeline_control.
2. Terraform cria FULL_CONFIG#seguros com contratos, clientes e parcelas.
3. Ingestão de contratos gera INGESTION_ID, INGESTION_HISTORY e CURRENT_SOR.
4. Ingestão de clientes gera CURRENT_SOR.
5. Ingestão de parcelas gera CURRENT_SOR.
6. sfn-orq-full-seguros recebe só data_referencia.
7. lambda-preparar-full-generica cria process_id e INPUT_LOCKS.
8. Módulos recebem só process_id.
9. Módulos leem SOR apenas por ingestion_id.
10. Módulos gravam SOT com process_id.
11. Consolidação lê SOT por process_id.
12. Publicação grava SPEC.
13. PROCESS termina com status PUBLISHED.
```

---

# Prompt pronto para o Codex

```text
Quero implementar o MVP1 do pipeline SOR/SOT/SPEC neste repositório.

Antes de codar, leia o repositório, identifique a estrutura atual, e me devolva um plano de alteração em etapas pequenas. Depois implemente.

Contexto do MVP1:

- SOR é a camada de ingestão versionada por ingestion_id.
- SOT é a camada intermediária processada por process_id.
- SPEC é a camada final publicada.
- A ingestão é genérica e não conhece processo/produto.
- O FULL é específico por processo, exemplo sfn-orq-full-seguros.
- A Step Functions FULL recebe externamente apenas data_referencia.
- A Step Functions injeta process_name=seguros ao chamar a lambda-preparar-full-generica.
- A lambda-preparar-full-generica recebe process_name e data_referencia, consulta FULL_CONFIG#<process_name> no DynamoDB, busca CURRENT_SOR de cada tabela, cria process_id e cria INPUT_LOCK de todas as tabelas antes dos módulos.
- Os módulos recebem só process_id.
- Os módulos leem SOR usando somente ingestion_id obtido do INPUT_LOCK.
- Os módulos gravam SOT usando process_id.
- Consolidação e publicação usam process_id.

Implementar:

1. Terraform:
   - Criar DynamoDB pipeline_control com PK e SK.
   - Criar FULL_CONFIG#seguros via aws_dynamodb_table_item.
   - FULL_CONFIG mínimo:
     seguros = ["contratos", "clientes", "parcelas"]

2. Shared Python:
   - Criar utilitário DynamoDB com funções:
     - query_by_pk
     - get_item
     - put_item
     - update_item
     - get_full_config
     - get_current_sor
     - create_input_lock
     - get_input_lock

3. Lambda preparar ingestão:
   - Entrada: database_name, table_name, data_referencia, partition_values.
   - Não receber process_name.
   - Gerar ingestion_id.
   - Montar source_push_down_predicate a partir de partition_values.
   - Criar INGESTION_ID#<ingestion_id>/METADATA com status PROCESSING.
   - Retornar ingestion_id.

4. Glue carga SOR:
   - Receber INGESTION_ID.
   - Buscar metadados no DynamoDB.
   - Ler Glue Catalog da origem com source_push_down_predicate.
   - Adicionar ingestion_id e dt_ingestao_sor.
   - Gravar SOR.
   - Não fazer count.

5. Lambda finalizar SOR sucesso:
   - Atualizar INGESTION_ID para SOR_LOADED.
   - Criar INGESTION_HISTORY#<table_name>#<data_referencia>/INGESTION#<ingestion_id>.
   - Atualizar CURRENT_SOR#<table_name>#<data_referencia>/CURRENT.

6. Lambda finalizar SOR falha:
   - Atualizar INGESTION_ID para FAILED.
   - Não atualizar CURRENT_SOR.

7. Lambda preparar FULL genérica:
   - Entrada: process_name, data_referencia.
   - Buscar FULL_CONFIG#<process_name>.
   - Buscar CURRENT_SOR de cada tabela.
   - Se faltar tabela, criar PROCESS com status FAILED_INPUT_MISSING.
   - Se tudo existir, criar process_id.
   - Criar PROCESS#<process_id>/HEADER.
   - Criar PROCESS_LIST#<process_name>#<data_referencia>/PROCESS#<process_id>.
   - Criar INPUT_LOCK para todas as tabelas.
   - Atualizar status para RUNNING.
   - Retornar process_id.

8. Módulos Glue:
   - Receber PROCESS_ID.
   - Buscar INPUT_LOCK das tabelas que o próprio job sabe que precisa.
   - Ler SOR usando somente ingestion_id='<id>'.
   - Gravar SOT com process_id.

9. Status:
   - Criar helper ou Lambda para registrar STEP#MODULO_X, STEP#MODULO_Y, STEP#MODULO_Z, STEP#CONSOLIDATION e STEP#PUBLICATION.

Restrições:
- Não criar MODULE_CONFIG no MVP1.
- Não criar PRODUCT_CONFIG no MVP1.
- Não criar PROCESS_CONFIG separado; usar somente FULL_CONFIG para lista de tabelas do FULL.
- Não colocar database/tabela de saída dos módulos no DynamoDB.
- Não usar data_referencia no predicate dos módulos ao ler SOR; usar somente ingestion_id.
- Não usar row_count no MVP1.
- Usar ConditionExpression ao criar INPUT_LOCK para evitar sobrescrita.
- Manter alterações pequenas e testáveis.
- Criar exemplos de eventos JSON para testar as Lambdas localmente, se o repo já tiver padrão de testes.
```

---

# AGENTS.md recomendado para o repo

```md
# AGENTS.md

## Objetivo do projeto

Este repositório implementa um pipeline AWS SOR/SOT/SPEC.

- SOR: camada de recepção versionada por ingestion_id.
- SOT: camada intermediária processada por process_id.
- SPEC: camada final publicada.

## Regras do MVP1

- Não criar site.
- Não criar MODULE_CONFIG.
- Não criar PRODUCT_CONFIG.
- Ingestão não conhece processo.
- FULL é específico por Step Functions do processo.
- Lambda preparar FULL é genérica.
- DynamoDB guarda FULL_CONFIG, CURRENT_SOR, INPUT_LOCK, PROCESS, STEP e histórico de ingestão.
- Glue dos módulos recebe process_id e busca ingestion_id no INPUT_LOCK.
- Módulos leem SOR apenas por ingestion_id.
- SOT e SPEC usam process_id.

## Convenções DynamoDB

Tabela: pipeline_control

Chaves:
- PK
- SK

Padrões:
- FULL_CONFIG#<process_name> / TABLE#<table_name>
- INGESTION_ID#<ingestion_id> / METADATA
- INGESTION_HISTORY#<table_name>#<data_referencia> / INGESTION#<ingestion_id>
- CURRENT_SOR#<table_name>#<data_referencia> / CURRENT
- PROCESS#<process_id> / HEADER
- PROCESS_LIST#<process_name>#<data_referencia> / PROCESS#<process_id>
- PROCESS#<process_id> / INPUT_LOCK#<table_name>
- PROCESS#<process_id> / STEP#<step_name>

## Restrições

- Não fazer count() nos Glue Jobs do MVP1.
- Não sobrescrever INPUT_LOCK existente.
- Não colocar regras de negócio dos módulos no DynamoDB.
- Não colocar SQL dos módulos no DynamoDB.
- Não adicionar data_referencia no predicate de leitura da SOR pelos módulos.
```
