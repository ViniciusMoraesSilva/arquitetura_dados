# Plano de Implementação — MVP1 Pipeline SOR / SOT / SPEC

## Contexto

Precisamos implementar um MVP1 para controlar ingestão de tabelas, versionamento na SOR, execução das regras de negócio e publicação final na SPEC.

A arquitetura usa:

- AWS Step Functions
- AWS Lambda
- AWS Glue Jobs
- Amazon DynamoDB
- Amazon S3
- EventBridge
- Terraform

A solução deve separar:

- `source_table_name`: nome da tabela na origem
- `sor_table_name`: nome interno padronizado dentro da SOR
- `ingestion_id`: versão da carga na SOR
- `process_id`: execução das regras de negócio
- `INPUT_LOCK`: trava de versões usadas no processo

---

# Objetivo do MVP1

Implementar o fluxo:

```text
Evento origem
  ↓
Step Functions Ingestão SOR
  ↓
Lambda Controlar Ingestão SOR
  ↓
Glue Job Carga SOR
  ↓
S3 SOR
  ↓
DynamoDB CURRENT_SOR
  ↓
Step Functions Regras de Negócio
  ↓
Lambda Preparar Regras
  ↓
INPUT_LOCK
  ↓
Glue Jobs Regras/Módulos
  ↓
S3 SOT
  ↓
Glue Consolidação
  ↓
RULES_PROCESSED
  ↓
Step Functions Publicação SPEC
  ↓
Glue Publicação SPEC
  ↓
S3 SPEC
```

---

# Convenções

## Nomes internos da SOR

O nome interno deve seguir o padrão:

```text
<process_name>_<entidade>
```

Exemplos:

```text
seguros_contratos
seguros_clientes
seguros_parcelas
```

A origem pode ter outro nome:

```text
db_origem.contrato
```

Mas dentro da SOR deve virar:

```text
sor_db.seguros_contratos
```

---

# Tabelas de controle no DynamoDB

Criar uma única tabela DynamoDB:

```text
pipeline_control
```

Com chave:

```text
PK = string
SK = string
```

A tabela deve armazenar os seguintes tipos de item:

```text
SOR_TABLE_CONFIG
PROCESS_RULES_CONFIG
INGESTION_ID
INGESTION_HISTORY
CURRENT_SOR
PROCESS
PROCESS_LIST
INPUT_LOCK
```

---

# 1. SOR_TABLE_CONFIG

## Objetivo

Mapear a tabela de origem para a tabela interna da SOR.

## Chave

```text
PK = SOR_TABLE_CONFIG#<process_name>
SK = SOURCE#<source_database_name>#<source_table_name>
```

## Exemplo

```json
{
  "PK": "SOR_TABLE_CONFIG#seguros",
  "SK": "SOURCE#db_origem#contrato",
  "process_name": "seguros",
  "source_database_name": "db_origem",
  "source_table_name": "contrato",
  "sor_database_name": "sor_db",
  "sor_table_name": "seguros_contratos",
  "enabled": true
}
```

## Critérios de aceite

- Deve ser possível consultar pelo processo + database origem + tabela origem.
- Se não existir configuração, a Lambda de ingestão deve falhar.
- Se `enabled = false`, a Lambda de ingestão deve falhar.

---

# 2. PROCESS_RULES_CONFIG

## Objetivo

Definir quais tabelas SOR são necessárias para executar as regras de negócio de um processo.

## Chave

```text
PK = PROCESS_RULES_CONFIG#<process_name>
SK = SOR_TABLE#<sor_table_name>
```

## Exemplo

```json
{
  "PK": "PROCESS_RULES_CONFIG#seguros",
  "SK": "SOR_TABLE#seguros_contratos",
  "process_name": "seguros",
  "sor_table_name": "seguros_contratos",
  "required": true,
  "enabled": true
}
```

## Critérios de aceite

- A Lambda de regras deve consultar todas as tabelas habilitadas do processo.
- Apenas itens `enabled = true` devem ser considerados.
- Itens `required = true` devem obrigatoriamente ter `CURRENT_SOR` ou `input_overrides`.

---

# 3. Lambda Controlar Ingestão SOR

## Nome sugerido

```text
lambda-controlar-ingestao-sor
```

## Objetivo

Centralizar o controle de ingestão.

Essa Lambda deve aceitar actions:

```text
REGISTER_INGESTION
MARK_LOADED_SOR
MARK_PROCESSING_FAILURE
```

Não usar mais:

```text
PENDING_PROCESSING
MARK_PROCESSING_SOR
```

O primeiro status da ingestão deve ser:

```text
PROCESSING_SOR
```

---

## 3.1 Action REGISTER_INGESTION

### Entrada

```json
{
  "action": "REGISTER_INGESTION",
  "payload": {
    "process_name": "seguros",
    "database_name": "db_origem",
    "table_name": "contrato",
    "partitions": {
      "data_referencia": "202604",
      "dia_base": "15"
    }
  }
}
```

### Regras

1. Consultar `SOR_TABLE_CONFIG#seguros / SOURCE#db_origem#contrato`.
2. Obter `source_database_name`, `source_table_name`, `sor_database_name` e `sor_table_name`.
3. Gerar `ingestion_id`.
4. Criar item `INGESTION_ID`.
5. Criar com status inicial `PROCESSING_SOR`.

### Item criado

```json
{
  "PK": "INGESTION_ID#ing_100",
  "SK": "METADATA",
  "ingestion_id": "ing_100",
  "process_name": "seguros",
  "source_database_name": "db_origem",
  "source_table_name": "contrato",
  "sor_database_name": "sor_db",
  "sor_table_name": "seguros_contratos",
  "partitions": {
    "data_referencia": "202604",
    "dia_base": "15"
  },
  "status": "PROCESSING_SOR",
  "created_at": "timestamp"
}
```

### Retorno

```json
{
  "ingestion_id": "ing_100",
  "process_name": "seguros",
  "sor_table_name": "seguros_contratos"
}
```

---

## 3.2 Action MARK_LOADED_SOR

### Entrada

```json
{
  "action": "MARK_LOADED_SOR",
  "payload": {
    "ingestion_id": "ing_100"
  }
}
```

### Regras

1. Buscar `INGESTION_ID#ing_100 / METADATA`.
2. Atualizar status para `LOADED_SOR`.
3. Criar histórico.
4. Atualizar `CURRENT_SOR`.

### Atualizar INGESTION_ID

```json
{
  "status": "LOADED_SOR",
  "loaded_at": "timestamp"
}
```

### Criar histórico

```text
PK = INGESTION_HISTORY#seguros#seguros_contratos
SK = INGESTION#ing_100
```

### Atualizar CURRENT_SOR

```text
PK = CURRENT_SOR#seguros#seguros_contratos
SK = CURRENT
```

Exemplo:

```json
{
  "PK": "CURRENT_SOR#seguros#seguros_contratos",
  "SK": "CURRENT",
  "process_name": "seguros",
  "source_database_name": "db_origem",
  "source_table_name": "contrato",
  "sor_database_name": "sor_db",
  "sor_table_name": "seguros_contratos",
  "current_ingestion_id": "ing_100",
  "partitions": {
    "data_referencia": "202604",
    "dia_base": "15"
  },
  "status": "LOADED_SOR",
  "updated_at": "timestamp"
}
```

---

## 3.3 Action MARK_PROCESSING_FAILURE

### Entrada

```json
{
  "action": "MARK_PROCESSING_FAILURE",
  "payload": {
    "ingestion_id": "ing_100",
    "error_message": "Erro ao processar origem"
  }
}
```

### Regras

1. Atualizar `INGESTION_ID` com `PROCESSING_FAILURE`.
2. Não atualizar `CURRENT_SOR`.
3. Não criar versão atual.

---

# 4. Glue Job Carga SOR

## Nome sugerido

```text
glue-carga-sor
```

## Entrada

```json
{
  "ingestion_id": "ing_100"
}
```

## Regras

1. Buscar `INGESTION_ID#ing_100 / METADATA`.
2. Ler metadados da origem, tabela SOR e `partitions`.
3. Interpretar `partitions`.
4. Montar predicate internamente.
5. Ler origem.
6. Gravar na SOR com `ingestion_id`.

## Importante

A Lambda não monta predicate.

O Glue SOR é responsável por:

- Predicate
- Regra de leitura
- Regra de gravação
- Tabela/prefixo destino da SOR

## Exemplo de destino

```text
s3://lake/sor/seguros_contratos/ingestion_id=ing_100/
```

---

# 5. Step Functions Ingestão SOR

## Nome sugerido

```text
sfn-orq-ingestao-sor-seguros
```

## Fluxo

```text
1. Recebe evento
2. Chama lambda-controlar-ingestao-sor com REGISTER_INGESTION
3. Recebe ingestion_id
4. Executa Glue Carga SOR
5. Se sucesso:
   - chama lambda-controlar-ingestao-sor com MARK_LOADED_SOR
6. Se falha:
   - chama lambda-controlar-ingestao-sor com MARK_PROCESSING_FAILURE
   - falha a execução
```

---

# 6. Lambda Preparar Regras de Negócio

## Nome sugerido

```text
lambda-preparar-regras-negocio-generica
```

## Entrada

```json
{
  "process_name": "seguros",
  "data_referencia": "202604",
  "input_overrides": {
    "seguros_contratos": "ing_090"
  }
}
```

`input_overrides` é opcional.

## Regras

1. Consultar `PROCESS_RULES_CONFIG#seguros`.
2. Obter as tabelas SOR habilitadas.
3. Para cada `sor_table_name`:
   - Se existir override, validar `INGESTION_ID`.
   - Se não existir override, buscar `CURRENT_SOR`.
4. Criar `process_id`.
5. Criar `PROCESS HEADER`.
6. Criar `PROCESS_LIST`.
7. Criar `INPUT_LOCK` para cada tabela.
8. Atualizar status para `RUNNING`.
9. Retornar `process_id`.

---

# 7. Validação do input_overrides

## Exemplo

```json
{
  "input_overrides": {
    "seguros_contratos": "ing_090"
  }
}
```

## Regras

Para cada override:

1. Buscar `INGESTION_ID#ing_090`.
2. Validar se existe.
3. Validar se `status = LOADED_SOR`.
4. Validar se o `sor_table_name` do ingestion é igual ao override informado.

Se qualquer validação falhar:

```text
status = FAILED_INVALID_OVERRIDE
```

E a Step Functions de regras deve falhar.

---

# 8. CURRENT_SOR como fallback

Se não houver override para uma tabela:

```text
CURRENT_SOR#seguros#seguros_contratos / CURRENT
```

A Lambda usa:

```text
current_ingestion_id
```

Se não existir `CURRENT_SOR` para tabela obrigatória:

```text
status = FAILED_INPUT_MISSING
```

E a Step Functions de regras deve falhar.

---

# 9. PROCESS HEADER

## Chave

```text
PK = PROCESS#proc_seguros_202604_001
SK = HEADER
```

## Exemplo

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "HEADER",
  "process_id": "proc_seguros_202604_001",
  "process_name": "seguros",
  "data_referencia": "202604",
  "execution_type": "BUSINESS_RULES",
  "status": "LOCKING_INPUTS",
  "created_at": "timestamp"
}
```

Depois dos locks:

```json
{
  "status": "RUNNING"
}
```

---

# 10. PROCESS_LIST

## Chave

```text
PK = PROCESS_LIST#seguros#202604
SK = PROCESS#proc_seguros_202604_001
```

## Objetivo

Permitir listar execuções por processo e data de referência.

---

# 11. INPUT_LOCK

## Chave

```text
PK = PROCESS#proc_seguros_202604_001
SK = INPUT_LOCK#seguros_contratos
```

## Exemplo com override

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "INPUT_LOCK#seguros_contratos",
  "process_id": "proc_seguros_202604_001",
  "process_name": "seguros",
  "sor_table_name": "seguros_contratos",
  "data_referencia": "202604",
  "ingestion_id": "ing_090",
  "lock_source": "OVERRIDE",
  "locked_at": "timestamp"
}
```

## Exemplo com current

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "INPUT_LOCK#seguros_clientes",
  "process_id": "proc_seguros_202604_001",
  "process_name": "seguros",
  "sor_table_name": "seguros_clientes",
  "data_referencia": "202604",
  "ingestion_id": "ing_200",
  "lock_source": "CURRENT_SOR",
  "locked_at": "timestamp"
}
```

---

# 12. Step Functions Regras de Negócio

## Nome sugerido

```text
sfn-orq-regras-negocio-seguros
```

## Fluxo

```text
1. Recebe data_referencia e input_overrides opcional
2. Injeta process_name = seguros
3. Chama lambda-preparar-regras-negocio-generica
4. Recebe process_id
5. Executa Glue Regra/Módulo X
6. Executa Glue Regra/Módulo Y
7. Executa Glue Regra/Módulo Z
8. Executa Glue Consolidação SOT
9. Atualiza PROCESS HEADER para RULES_PROCESSED
10. Atualiza PROCESS_LIST para RULES_PROCESSED
```

## Importante

A publicação SPEC não fica dentro dessa Step Functions.

---

# 13. Glue Jobs de Regras/Módulos

## Entrada

```json
{
  "process_id": "proc_seguros_202604_001"
}
```

## Regras

1. Buscar `INPUT_LOCK` das tabelas necessárias.
2. Ler SOR somente por `ingestion_id`.
3. Processar regras.
4. Gravar SOT com `process_id`.

## Importante

Os módulos não escolhem versão.

Eles apenas respeitam o `INPUT_LOCK`.

---

# 14. Glue Consolidação SOT

## Entrada

```json
{
  "process_id": "proc_seguros_202604_001"
}
```

## Regras

1. Ler saídas dos módulos na SOT filtrando por `process_id`.
2. Consolidar.
3. Gravar SOT consolidada com `process_id`.

Após sucesso, a Step Functions de regras atualiza:

```text
status = RULES_PROCESSED
```

---

# 15. Step Functions Publicação SPEC

## Nome sugerido

```text
sfn-orq-publicacao-spec-seguros
```

## Entrada

```json
{
  "process_id": "proc_seguros_202604_001"
}
```

## Fluxo

```text
1. Buscar PROCESS HEADER
2. Validar se status = RULES_PROCESSED
3. Se sim:
   - executar Glue Publicação SPEC
   - atualizar PROCESS HEADER para PUBLISHED
   - atualizar PROCESS_LIST para PUBLISHED
4. Se não:
   - falhar execução
   - não atualizar PROCESS HEADER
   - não atualizar PROCESS_LIST
```

---

# 16. Glue Publicação SPEC

## Entrada

```json
{
  "process_id": "proc_seguros_202604_001"
}
```

## Regras

1. Ler SOT consolidada por `process_id`.
2. Obter `data_referencia` do `PROCESS HEADER`.
3. Gravar SPEC com:
   - `process_id`
   - `data_referencia`
   - visão oficial do processo

---

# 17. Status esperados

## Ingestão

```text
PROCESSING_SOR
LOADED_SOR
PROCESSING_FAILURE
```

## Processo

```text
LOCKING_INPUTS
RUNNING
RULES_PROCESSED
PUBLISHED
FAILED_INPUT_MISSING
FAILED_INVALID_OVERRIDE
PROCESSING_FAILURE
```

---

# 18. Critérios gerais de aceite

## Ingestão

- Evento não precisa enviar `sor_table_name`.
- Lambda resolve `sor_table_name` via `SOR_TABLE_CONFIG`.
- Primeiro status deve ser `PROCESSING_SOR`.
- Glue SOR grava dados com `ingestion_id`.
- `CURRENT_SOR` deve ser atualizado usando `sor_table_name`.

## Regras de negócio

- `data_referencia` é obrigatória.
- `input_overrides` é opcional.
- `PROCESS_RULES_CONFIG` usa `SOR_TABLE#<sor_table_name>`.
- Lambda cria `process_id`.
- Lambda cria `INPUT_LOCK`.
- Módulos leem SOR somente por `ingestion_id`.

## Publicação SPEC

- Só publica se `status = RULES_PROCESSED`.
- Se não estiver pronto, Step Functions deve falhar.
- Não deve alterar status para `PUBLICATION_BLOCKED`.
- Não deve atualizar DynamoDB em caso de processo não pronto.

---

# 19. Ordem de implementação recomendada

## Fase 1 — DynamoDB e Configs

1. Criar tabela `pipeline_control`.
2. Criar seeds Terraform para:
   - `SOR_TABLE_CONFIG`
   - `PROCESS_RULES_CONFIG`
3. Criar exemplos para seguros:
   - `seguros_contratos`
   - `seguros_clientes`
   - `seguros_parcelas`

## Fase 2 — Lambda de Ingestão

1. Implementar `REGISTER_INGESTION`.
2. Implementar `MARK_LOADED_SOR`.
3. Implementar `MARK_PROCESSING_FAILURE`.
4. Criar testes unitários.

## Fase 3 — Glue Carga SOR

1. Ler `ingestion_id`.
2. Buscar metadata no DynamoDB.
3. Ler origem.
4. Gravar SOR com `ingestion_id`.

## Fase 4 — Step Functions Ingestão

1. Orquestrar Lambda + Glue.
2. Tratar sucesso.
3. Tratar falha.

## Fase 5 — Lambda Preparar Regras

1. Ler `PROCESS_RULES_CONFIG`.
2. Resolver overrides.
3. Resolver current.
4. Criar `process_id`.
5. Criar `INPUT_LOCK`.

## Fase 6 — Glue Jobs de Regras e Consolidação

1. Receber `process_id`.
2. Buscar `INPUT_LOCK`.
3. Ler SOR por `ingestion_id`.
4. Gravar SOT por `process_id`.
5. Consolidar SOT.

## Fase 7 — Step Functions Regras

1. Orquestrar preparação.
2. Executar módulos.
3. Executar consolidação.
4. Atualizar para `RULES_PROCESSED`.

## Fase 8 — Publicação SPEC

1. Criar Step Functions Publicação SPEC.
2. Validar `RULES_PROCESSED`.
3. Executar Glue Publicação SPEC.
4. Atualizar para `PUBLISHED`.

---

# 20. Não implementar no MVP1

Não implementar agora:

- Tela operacional
- Execução modular manual por usuário
- Cadastro dinâmico via front-end
- Controle detalhado de step no DynamoDB
- Row count obrigatório
- Regras de negócio no DynamoDB
- `PUBLICATION_BLOCKED`
