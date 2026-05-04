# Proposta de Arquitetura  
## Pipeline SOR → SOT → SPEC com controle por `ingestion_id`, `process_id` e tabela SOR interna

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

- Qual tabela chegou da origem
- Qual nome essa tabela terá dentro da nossa SOR
- Qual versão da tabela foi carregada
- Qual versão foi usada em uma execução
- Como reprocessar sem perder rastreabilidade
- Como publicar a visão final com segurança

---

# 2. Ideia central da solução

A arquitetura separa três conceitos:

| Conceito | Para que serve |
|---|---|
| `source_table_name` | Nome da tabela na origem |
| `sor_table_name` | Nome padronizado da tabela dentro da SOR |
| `ingestion_id` | Controla a versão carregada na SOR |
| `process_id` | Controla a execução das regras de negócio |

Regra principal:

> A origem pode ter qualquer nome.  
> Dentro do nosso sistema, controlamos tudo pelo nome padronizado da SOR.

---

# 3. Padrão de nomes da SOR

A recomendação é usar:

```text
<process_name>_<entidade>
```

Exemplos:

```text
seguros_contratos
seguros_clientes
seguros_parcelas

cartoes_contratos
cartoes_clientes
cartoes_transacoes
```

Assim, a tabela interna deixa explícito o domínio/processo ao qual pertence.

---

# 4. Por que não usar diretamente o nome da origem?

Porque o nome da origem pode mudar ou não seguir o nosso padrão.

Exemplo de origem:

```text
db_origem.contrato
db_legado.tb_ctr_001
db_mainframe.arq_ctr
```

Dentro do nosso lake, queremos algo estável:

```text
sor_db.seguros_contratos
```

Isso desacopla o sistema interno da nomenclatura externa.

---

# 5. Novo cadastro: SOR_TABLE_CONFIG

Criamos um cadastro no DynamoDB para mapear origem → tabela SOR interna.

Exemplo:

```text
PK = SOR_TABLE_CONFIG#seguros
SK = SOURCE#db_origem#contrato
```

Item:

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

---

# 6. Evento de ingestão

A origem informa que uma tabela está disponível.

Exemplo:

```json
{
  "process_name": "seguros",
  "database_name": "db_origem",
  "table_name": "contrato",
  "partitions": {
    "data_referencia": "202604",
    "dia_base": "15"
  }
}
```

Pontos importantes:

- O evento informa processo, database, tabela e partições.
- O evento não precisa conhecer `sor_table_name`.
- A Lambda resolve o `sor_table_name` consultando o `SOR_TABLE_CONFIG`.
- O Glue SOR decide como ler e onde gravar.

---

# 7. Registro da ingestão

A Step Functions chama a Lambda:

```text
action = REGISTER_INGESTION
```

A Lambda faz:

1. Consulta `SOR_TABLE_CONFIG#seguros / SOURCE#db_origem#contrato`
2. Descobre `sor_table_name = seguros_contratos`
3. Gera `ingestion_id`
4. Grava o controle da ingestão

Exemplo:

```text
ingestion_id = ing_100
```

---

# 8. Item INGESTION_ID

A Lambda grava no DynamoDB:

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

  "status": "PROCESSING_SOR"
}
```

---

# 9. Status da ingestão

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

# 10. Glue Job SOR

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
source_database_name = db_origem
source_table_name = contrato
sor_database_name = sor_db
sor_table_name = seguros_contratos
partitions = { data_referencia: 202604, dia_base: 15 }
```

---

# 11. Responsabilidade do Glue SOR

O Glue decide internamente:

- Como montar o predicate
- Como ler a origem
- Qual regra técnica aplicar
- Onde gravar na SOR
- Como materializar `sor_table_name`

Exemplo conceitual:

```text
s3://lake/sor/seguros_contratos/ingestion_id=ing_100/
```

---

# 12. Finalização da ingestão

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
INGESTION_HISTORY#seguros#seguros_contratos / INGESTION#ing_100
```

E atualiza o ponteiro atual:

```text
CURRENT_SOR#seguros#seguros_contratos / CURRENT
```

---

# 13. O que é CURRENT_SOR?

O `CURRENT_SOR` aponta para a última versão válida da tabela SOR interna para um processo.

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
  "status": "LOADED_SOR"
}
```

Esse item responde:

> Qual é a última versão válida de `seguros_contratos` para seguros?

Resposta:

```text
ing_100
```

---

# 14. Configuração das regras de negócio

No DynamoDB teremos:

```text
PROCESS_RULES_CONFIG#seguros / SOR_TABLE#seguros_contratos
PROCESS_RULES_CONFIG#seguros / SOR_TABLE#seguros_clientes
PROCESS_RULES_CONFIG#seguros / SOR_TABLE#seguros_parcelas
```

Isso responde:

> Quais tabelas SOR precisam ser travadas para executar as regras de negócio de seguros?

Exemplo:

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

---

# 15. Disparo das regras de negócio

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
    "seguros_contratos": "ing_090"
  }
}
```

A `data_referencia` é obrigatória no disparo das regras.

Ela identifica a referência da execução do `process_id`.

---

# 16. O que é input_overrides?

O `input_overrides` permite escolher manualmente a versão de uma tabela SOR.

Exemplo:

```json
{
  "input_overrides": {
    "seguros_contratos": "ing_090"
  }
}
```

Nesse caso:

- `seguros_contratos` usará `ing_090`
- As demais tabelas usarão `CURRENT_SOR`

Exemplo:

```text
seguros_contratos → ing_090 via OVERRIDE
seguros_clientes  → ing_200 via CURRENT_SOR
seguros_parcelas  → ing_300 via CURRENT_SOR
```

---

# 17. Preparação das regras de negócio

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
    "seguros_contratos": "ing_090"
  }
}
```

A Lambda faz:

1. Consulta `PROCESS_RULES_CONFIG#seguros`
2. Descobre as tabelas SOR obrigatórias
3. Resolve o `ingestion_id` de cada tabela SOR
4. Cria o `process_id`
5. Cria os `INPUT_LOCKS`

---

# 18. Criação do process_id

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

# 19. O que é INPUT_LOCK?

O `INPUT_LOCK` congela a versão usada por aquele `process_id`.

Exemplo com override:

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "INPUT_LOCK#seguros_contratos",
  "process_id": "proc_seguros_202604_001",
  "process_name": "seguros",
  "sor_table_name": "seguros_contratos",
  "data_referencia": "202604",
  "ingestion_id": "ing_090",
  "lock_source": "OVERRIDE"
}
```

Exemplo com current:

```json
{
  "PK": "PROCESS#proc_seguros_202604_001",
  "SK": "INPUT_LOCK#seguros_clientes",
  "process_id": "proc_seguros_202604_001",
  "process_name": "seguros",
  "sor_table_name": "seguros_clientes",
  "data_referencia": "202604",
  "ingestion_id": "ing_200",
  "lock_source": "CURRENT_SOR"
}
```

---

# 20. Regra mais importante

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

# 21. Execução dos módulos

Exemplo: Regra/Módulo X.

O Glue Job recebe:

```text
process_id = proc_seguros_202604_001
```

Busca no DynamoDB:

```text
PROCESS#proc_seguros_202604_001 / INPUT_LOCK#seguros_contratos
PROCESS#proc_seguros_202604_001 / INPUT_LOCK#seguros_clientes
```

Recebe:

```text
seguros_contratos = ing_090
seguros_clientes  = ing_200
```

Lê a SOR usando somente:

```text
ingestion_id = ing_090
ingestion_id = ing_200
```

---

# 22. Gravação na SOT

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

# 23. Consolidação

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

# 24. Publicação SPEC

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

# 25. Regra para publicar SPEC

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

# 26. Se o processo não estiver pronto?

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

# 27. Estados principais do processo

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

# 28. Exemplo completo de ponta a ponta

## 1. Chegou contratos

Evento recebido:

```json
{
  "process_name": "seguros",
  "database_name": "db_origem",
  "table_name": "contrato",
  "partitions": {
    "data_referencia": "202604",
    "dia_base": "15"
  }
}
```

Mapeamento resolvido:

```text
source_table_name = contrato
sor_table_name    = seguros_contratos
```

Gera:

```text
ingestion_id = ing_100
CURRENT_SOR#seguros#seguros_contratos = ing_100
```

---

# 29. Exemplo completo de ponta a ponta

## 2. Chegou clientes

Evento recebido:

```json
{
  "process_name": "seguros",
  "database_name": "db_origem",
  "table_name": "cliente",
  "partitions": {
    "data_referencia": "202604",
    "dia_base": "15"
  }
}
```

Mapeamento resolvido:

```text
source_table_name = cliente
sor_table_name    = seguros_clientes
```

Gera:

```text
ingestion_id = ing_200
CURRENT_SOR#seguros#seguros_clientes = ing_200
```

---

# 30. Exemplo completo de ponta a ponta

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
seguros_contratos = ing_100
seguros_clientes  = ing_200
seguros_parcelas  = ing_300
```

---

# 31. Exemplo com override

Se contratos precisa usar uma versão antiga:

```json
{
  "data_referencia": "202604",
  "input_overrides": {
    "seguros_contratos": "ing_090"
  }
}
```

O lock fica:

```text
seguros_contratos = ing_090 via OVERRIDE
seguros_clientes  = ing_200 via CURRENT_SOR
seguros_parcelas  = ing_300 via CURRENT_SOR
```

---

# 32. Benefícios da solução

## Rastreabilidade

Sabemos exatamente:

- Qual tabela chegou da origem
- Qual tabela SOR interna foi usada
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

# 33. Benefícios técnicos

- Desacoplamento entre origem e SOR
- Nome interno padronizado por processo
- Lambda simples na ingestão
- Glue SOR concentra regra de leitura e gravação
- DynamoDB guarda apenas controle operacional
- Step Functions orquestra o fluxo
- SOR versionada por `ingestion_id`
- SOT/SPEC controladas por `process_id`
- Publicação SPEC isolada em Step Functions própria

---

# 34. O que fica fora do MVP1

Neste MVP1, não teremos:

- Tela operacional
- Execução modular manual por usuário
- Cadastro dinâmico via front-end
- Controle detalhado de cada step no DynamoDB
- Row count obrigatório
- Regras de negócio dentro do DynamoDB

Essas evoluções podem entrar em um MVP2.

---

# 35. DynamoDB como control plane

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

# 36. Chaves principais no DynamoDB

```text
SOR_TABLE_CONFIG#seguros / SOURCE#db_origem#contrato

INGESTION_ID#ing_100 / METADATA

INGESTION_HISTORY#seguros#seguros_contratos / INGESTION#ing_100

CURRENT_SOR#seguros#seguros_contratos / CURRENT

PROCESS_RULES_CONFIG#seguros / SOR_TABLE#seguros_contratos

PROCESS#proc_seguros_202604_001 / HEADER

PROCESS#proc_seguros_202604_001 / INPUT_LOCK#seguros_contratos
```

---

# 37. Resumo final

A arquitetura final fica baseada em:

```text
source_table_name → nome da tabela na origem

sor_table_name → nome interno padronizado na SOR

ingestion_id → controla versão na SOR

process_id → controla execução das regras

INPUT_LOCK → congela quais ingestion_id foram usados

RULES_PROCESSED → libera publicação

PUBLISHED → indica SPEC oficial publicada
```

---

# 38. Frase principal

> A origem informa o que chegou.  
> A Lambda resolve o nome interno da SOR.  
> A ingestão versiona os dados.  
> As regras de negócio congelam as versões em um `process_id`.  
> A publicação só acontece quando a consolidação estiver concluída.
