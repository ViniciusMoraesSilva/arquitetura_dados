# Proposta de Arquitetura  
## Pipeline SOR → SOT → SPEC com controle por `ingestion_id` e `process_id`

**Objetivo:**  
Criar um processo controlado, auditável e reprocessável para ingestão de tabelas, execução de regras de negócio e publicação final na SPEC.

---

# 1. Problema que queremos resolver

Hoje precisamos processar bases vindas de origens diferentes.

Exemplo:

- Tabela de contratos
- Tabela de clientes
- Tabela de parcelas
- Outras tabelas necessárias para o processo

Essas tabelas podem ser usadas por mais de um processo/produto, como:

- Seguros
- Cartões
- Imobiliário

O desafio é controlar:

- Qual versão da tabela foi carregada
- Qual versão foi usada em uma execução
- Como reprocessar sem perder rastreabilidade
- Como publicar a visão final com segurança

---

# 2. Ideia central da solução

A arquitetura separa dois conceitos:

| Conceito | Para que serve |
|---|---|
| `ingestion_id` | Controla a versão carregada na SOR |
| `process_id` | Controla a execução das regras de negócio |

Regra principal:

> A SOR é versionada por `ingestion_id`.  
> A SOT e a SPEC são controladas por `process_id`.

---

# 3. Camadas do processo

```text
Origem
  ↓
SOR
  ↓
SOT
  ↓
SPEC
```

## SOR

Camada de entrada/versionamento.

Guarda os dados recebidos da origem com `ingestion_id`.

## SOT

Camada intermediária.

Guarda o resultado das regras/módulos com `process_id`.

## SPEC

Camada final oficial.

Guarda a visão publicada para consumo.

---

# 4. Visão resumida da arquitetura

```text
EventBridge
  ↓
AWS Step Functions - Ingestão SOR
  ↓
Lambda Controlar Ingestão SOR
  ↓
AWS Glue Job Carga SOR
  ↓
S3 / SOR
  ↓
DynamoDB - CURRENT_SOR
```

Depois:

```text
AWS Step Functions - Regras de Negócio
  ↓
Lambda Preparar Regras
  ↓
INPUT_LOCK no DynamoDB
  ↓
Glue Jobs de Regras
  ↓
S3 / SOT
  ↓
Glue Consolidação
  ↓
SOT Consolidada
```

E por fim:

```text
AWS Step Functions - Publicação SPEC
  ↓
Glue Publicação SPEC
  ↓
S3 / SPEC
```

---

# 5. Componentes principais

| Componente | Responsabilidade |
|---|---|
| EventBridge | Receber evento de chegada de tabela |
| Step Functions Ingestão SOR | Orquestrar a carga da tabela para SOR |
| Lambda Controlar Ingestão SOR | Criar e atualizar controle da ingestão |
| Glue Job SOR | Ler origem e gravar SOR |
| DynamoDB | Guardar controle operacional |
| Step Functions Regras de Negócio | Orquestrar regras/módulos e consolidação |
| Lambda Preparar Regras | Criar `process_id` e travar versões |
| Glue Jobs de Regras | Processar regras de negócio |
| Step Functions Publicação SPEC | Orquestrar publicação final |
| Glue Publicação SPEC | Gravar visão oficial na SPEC |

---

# 6. Evento de ingestão

A origem informa que uma tabela está disponível.

Exemplo:

```json
{
  "process_name": "seguros",
  "database_name": "db_origem",
  "table_name": "contratos",
  "partitions": {
    "data_referencia": "202604",
    "dia_base": "15"
  }
}
```

Pontos importantes:

- O evento informa processo, database, tabela e partições.
- A Lambda não monta predicate.
- A Lambda não define tabela destino.
- O Glue SOR decide como ler e onde gravar.

---

# 7. Registro da ingestão

A Step Functions chama a Lambda:

```text
action = REGISTER_INGESTION
```

A Lambda cria um `ingestion_id`.

Exemplo:

```text
ingestion_id = ing_100
```

E grava no DynamoDB:

```json
{
  "PK": "INGESTION_ID#ing_100",
  "SK": "METADATA",
  "ingestion_id": "ing_100",
  "process_name": "seguros",
  "source_database_name": "db_origem",
  "source_table_name": "contratos",
  "partitions": {
    "data_referencia": "202604",
    "dia_base": "15"
  },
  "status": "PROCESSING_SOR"
}
```

---

# 8. Status da ingestão

Para a ingestão SOR teremos:

| Status | Significado |
|---|---|
| `PROCESSING_SOR` | A carga para SOR está em processamento |
| `LOADED_SOR` | A carga foi concluída com sucesso |
| `PROCESSING_FAILURE` | A carga falhou |

Importante:

> O primeiro status já será `PROCESSING_SOR`.

Não teremos mais `PENDING_PROCESSING`.

---

# 9. Glue Job SOR

O Glue Job SOR recebe apenas:

```json
{
  "ingestion_id": "ing_100"
}
```

Ele busca no DynamoDB:

```text
INGESTION_ID#ing_100 / METADATA
```

E descobre:

```text
process_name = seguros
database_name = db_origem
table_name = contratos
partitions = { data_referencia: 202604, dia_base: 15 }
```

O Glue decide internamente:

- Como montar o predicate
- Como ler a origem
- Qual regra técnica aplicar
- Onde gravar na SOR

---

# 10. Gravação na SOR

O Glue grava os dados na SOR com o `ingestion_id`.

Exemplo conceitual:

```text
s3://lake/sor/contratos/ingestion_id=ing_100/
```

A leitura futura dessa versão será feita por:

```text
ingestion_id = ing_100
```

---

# 11. Finalização da ingestão

Após o Glue SOR terminar, a Step Functions chama novamente a Lambda:

```text
action = MARK_LOADED_SOR
```

A Lambda atualiza:

```text
INGESTION_ID#ing_100
status = LOADED_SOR
```

Também cria histórico:

```text
INGESTION_HISTORY#seguros#contratos / INGESTION#ing_100
```

E atualiza o ponteiro atual:

```text
CURRENT_SOR#seguros#contratos / CURRENT
```

---

# 12. O que é CURRENT_SOR?

O `CURRENT_SOR` aponta para a última versão válida da tabela para um processo.

Exemplo:

```json
{
  "PK": "CURRENT_SOR#seguros#contratos",
  "SK": "CURRENT",
  "process_name": "seguros",
  "source_table_name": "contratos",
  "current_ingestion_id": "ing_100",
  "partitions": {
    "data_referencia": "202604",
    "dia_base": "15"
  },
  "status": "LOADED_SOR"
}
```

Esse item responde:

> Qual é a última versão válida de contratos para seguros?

Resposta:

```text
ing_100
```

---

# 13. Configuração das regras de negócio

No DynamoDB teremos:

```text
PROCESS_RULES_CONFIG#seguros / TABLE#contratos
PROCESS_RULES_CONFIG#seguros / TABLE#clientes
PROCESS_RULES_CONFIG#seguros / TABLE#parcelas
```

Isso responde:

> Quais tabelas precisam ser travadas para executar as regras de negócio de seguros?

Exemplo:

```json
{
  "PK": "PROCESS_RULES_CONFIG#seguros",
  "SK": "TABLE#contratos",
  "process_name": "seguros",
  "source_table_name": "contratos"
}
```

---

# 14. Disparo das regras de negócio

O operador dispara:

```json
{
  "data_referencia": "202604"
}
```

Ou, se quiser escolher uma versão específica:

```json
{
  "data_referencia": "202604",
  "input_overrides": {
    "contratos": "ing_090"
  }
}
```

A `data_referencia` é obrigatória no disparo das regras.

Ela identifica a referência da execução do `process_id`.

---

# 15. O que é input_overrides?

O `input_overrides` permite escolher manualmente a versão de uma tabela.

Exemplo:

```json
{
  "input_overrides": {
    "contratos": "ing_090"
  }
}
```

Nesse caso:

- `contratos` usará `ing_090`
- As demais tabelas usarão `CURRENT_SOR`

Exemplo:

```text
contratos → ing_090 via OVERRIDE
clientes  → ing_200 via CURRENT_SOR
parcelas  → ing_300 via CURRENT_SOR
```

---

# 16. Preparação das regras de negócio

A Step Functions de regras chama:

```text
lambda-preparar-regras-negocio-generica
```

Ela recebe:

```json
{
  "process_name": "seguros",
  "data_referencia": "202604",
  "input_overrides": {
    "contratos": "ing_090"
  }
}
```

A Lambda faz:

1. Consulta `PROCESS_RULES_CONFIG#seguros`
2. Descobre as tabelas obrigatórias
3. Resolve o `ingestion_id` de cada tabela
4. Cria o `process_id`
5. Cria os `INPUT_LOCKS`

---

# 17. Criação do process_id

Exemplo:

```text
process_id = proc_seguros_202604_001
```

Header no DynamoDB:

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "HEADER",
  "process_id": "proc_seguros_202604_001",
  "process_name": "seguros",
  "data_referencia": "202604",
  "execution_type": "BUSINESS_RULES",
  "status": "LOCKING_INPUTS"
}
```

Depois de criar os locks, o status vira:

```text
RUNNING
```

---

# 18. O que é INPUT_LOCK?

O `INPUT_LOCK` congela a versão usada por aquele `process_id`.

Exemplo:

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "INPUT_LOCK#contratos",
  "process_id": "proc_seguros_202604_001",
  "process_name": "seguros",
  "source_table_name": "contratos",
  "data_referencia": "202604",
  "ingestion_id": "ing_090",
  "lock_source": "OVERRIDE"
}
```

Outro exemplo:

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "INPUT_LOCK#clientes",
  "process_id": "proc_seguros_202604_001",
  "process_name": "seguros",
  "source_table_name": "clientes",
  "data_referencia": "202604",
  "ingestion_id": "ing_200",
  "lock_source": "CURRENT_SOR"
}
```

---

# 19. Regra mais importante

Depois que o `INPUT_LOCK` foi criado:

> Nenhum módulo escolhe versão de tabela.

Os módulos recebem apenas:

```json
{
  "process_id": "proc_seguros_202604_001"
}
```

E consultam o DynamoDB para descobrir os `ingestion_id` travados.

---

# 20. Execução dos módulos

Exemplo: Regra/Módulo X.

O Glue Job recebe:

```text
process_id = proc_seguros_202604_001
```

Busca no DynamoDB:

```text
PROCESS#proc_seguros_202604_001 / INPUT_LOCK#contratos
PROCESS#proc_seguros_202604_001 / INPUT_LOCK#clientes
```

Recebe:

```text
contratos = ing_090
clientes  = ing_200
```

Lê a SOR usando somente:

```text
ingestion_id = ing_090
ingestion_id = ing_200
```

---

# 21. Gravação na SOT

Os módulos gravam na SOT com `process_id`.

Exemplo:

```text
s3://lake/sot/seguros/modulo_x/process_id=proc_seguros_202604_001/
```

A partir daqui, a execução é controlada por `process_id`.

Regra:

```text
SOR → lê por ingestion_id
SOT → lê por process_id
```

---

# 22. Consolidação

A consolidação fica dentro da:

```text
AWS Step Functions Regras de Negócio
```

Ela chama:

```text
AWS Glue Job Consolidação SOT
```

O Glue lê as saídas dos módulos:

```text
process_id = proc_seguros_202604_001
```

E grava a SOT consolidada.

Após sucesso:

```text
PROCESS status = RULES_PROCESSED
```

---

# 23. Publicação SPEC

A publicação SPEC fica em outra Step Functions:

```text
AWS Step Functions Publicação SPEC
```

Ela recebe:

```json
{
  "process_id": "proc_seguros_202604_001"
}
```

Antes de publicar, valida o status do processo.

---

# 24. Regra para publicar SPEC

A publicação só pode acontecer se:

```text
PROCESS status = RULES_PROCESSED
```

Se estiver correto:

```text
AWS Step Functions Publicação SPEC
  ↓
AWS Glue Job Publicação SPEC
  ↓
S3 / SPEC
```

Após sucesso:

```text
PROCESS status = PUBLISHED
```

---

# 25. Se o processo não estiver pronto?

Se a publicação for disparada e o processo **não estiver** em:

```text
RULES_PROCESSED
```

A Step Functions de Publicação SPEC deve:

```text
falhar a execução
não atualizar PROCESS HEADER
não atualizar PROCESS_LIST
```

Isso evita mascarar o estado real do processo.

Exemplo:

- Se estiver `RUNNING`, continua `RUNNING`
- Se estiver `FAILED_INPUT_MISSING`, continua `FAILED_INPUT_MISSING`
- Se estiver `FAILED_INVALID_OVERRIDE`, continua `FAILED_INVALID_OVERRIDE`

---

# 26. Estados principais do processo

## Ingestão

```text
PROCESSING_SOR
LOADED_SOR
PROCESSING_FAILURE
```

## Regras de negócio

```text
LOCKING_INPUTS
RUNNING
RULES_PROCESSED
FAILED_INPUT_MISSING
FAILED_INVALID_OVERRIDE
PROCESSING_FAILURE
```

## Publicação

```text
PUBLISHED
```

---

# 27. Exemplo completo de ponta a ponta

## 1. Chegou contratos

```json
{
  "process_name": "seguros",
  "database_name": "db_origem",
  "table_name": "contratos",
  "partitions": {
    "data_referencia": "202604",
    "dia_base": "15"
  }
}
```

Gera:

```text
ingestion_id = ing_100
CURRENT_SOR#seguros#contratos = ing_100
```

---

# 28. Exemplo completo de ponta a ponta

## 2. Chegou clientes

```json
{
  "process_name": "seguros",
  "database_name": "db_origem",
  "table_name": "clientes",
  "partitions": {
    "data_referencia": "202604",
    "dia_base": "15"
  }
}
```

Gera:

```text
ingestion_id = ing_200
CURRENT_SOR#seguros#clientes = ing_200
```

---

# 29. Exemplo completo de ponta a ponta

## 3. Disparo das regras

```json
{
  "data_referencia": "202604"
}
```

A Lambda cria:

```text
process_id = proc_seguros_202604_001
```

E trava:

```text
contratos = ing_100
clientes  = ing_200
parcelas  = ing_300
```

---

# 30. Exemplo com override

Se contratos precisa usar uma versão antiga:

```json
{
  "data_referencia": "202604",
  "input_overrides": {
    "contratos": "ing_090"
  }
}
```

O lock fica:

```text
contratos = ing_090 via OVERRIDE
clientes  = ing_200 via CURRENT_SOR
parcelas  = ing_300 via CURRENT_SOR
```

---

# 31. Benefícios da solução

## Rastreabilidade

Sabemos exatamente:

- Qual tabela chegou
- Qual `ingestion_id` foi gerado
- Qual versão está atual
- Qual versão foi usada no `process_id`
- Qual processo publicou a SPEC

## Reprocessamento

Podemos rodar novamente usando:

- Versões atuais
- Ou versões específicas via `input_overrides`

## Segurança operacional

A publicação só acontece após:

```text
RULES_PROCESSED
```

---

# 32. Benefícios técnicos

- Lambda simples na ingestão
- Glue SOR concentra regra de leitura e gravação
- DynamoDB guarda apenas controle operacional
- Step Functions orquestra o fluxo
- SOR versionada por `ingestion_id`
- SOT/SPEC controladas por `process_id`
- Publicação SPEC isolada em Step Functions própria

---

# 33. O que fica fora do MVP1

Neste MVP1, não teremos:

- Tela operacional
- Execução modular manual por usuário
- Cadastro dinâmico via front-end
- Controle detalhado de cada step no DynamoDB
- Row count obrigatório
- Regras de negócio dentro do DynamoDB

Essas evoluções podem entrar em um MVP2.

---

# 34. Decisão importante

O DynamoDB não será usado como data warehouse.

Ele será usado como **control plane**.

Ou seja:

```text
DynamoDB = controle operacional
S3       = dados
Glue     = processamento
SFN      = orquestração
```

---

# 35. Resumo final

A arquitetura final fica baseada em:

```text
ingestion_id → controla versão na SOR

process_id → controla execução das regras

INPUT_LOCK → congela quais ingestion_id foram usados

RULES_PROCESSED → libera publicação

PUBLISHED → indica SPEC oficial publicada
```

---

# 36. Frase principal

> A ingestão versiona os dados.  
> As regras de negócio congelam as versões em um `process_id`.  
> A publicação só acontece quando a consolidação estiver concluída.
