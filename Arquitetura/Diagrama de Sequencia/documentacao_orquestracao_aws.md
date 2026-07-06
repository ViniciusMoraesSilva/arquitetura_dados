# Documentação do Processo de Orquestração AWS

## 1. Objetivo

Este documento descreve o fluxo de orquestração para ingestão SOR, execução de regras/SOT e publicação SPEC.

A solução permite:

- receber eventos de múltiplas origens;
- gerar `ingestion_id` sequencial por carga;
- manter histórico das versões recebidas;
- executar automaticamente quando todas as origens obrigatórias chegarem;
- permitir execução manual escolhendo versões específicas;
- gerar `process_id` sequencial;
- travar os inputs usados em cada execução;
- executar regras em um fluxo separado por produto;
- publicar SPEC automaticamente ou manualmente, conforme configuração;
- acompanhar tudo pelo site usando dados do DynamoDB.

---

## 2. Componentes principais

| Componente | Responsabilidade |
|---|---|
| EventBridge | Receber evento da origem e iniciar a Step Function SOR |
| Step Function SOR | Orquestrar a carga da origem na camada SOR |
| Lambda Controlar Ingestão | Criar/finalizar `ingestion_id`, atualizar histórico e `CURRENT_SOR` |
| Glue SOR | Carregar os dados da origem para S3 SOR |
| DynamoDB | Controlar estado, histórico, configurações, locks e execuções |
| Lambda Orquestradora | Verificar se já pode iniciar o processamento automático |
| Step Function Orquestradora de Processamento | Criar `process_id`, travar inputs, chamar fluxo de regras e decidir SPEC |
| Lambda Criar Processo | Criar `process_id` e `INPUT_LOCK` |
| Step Function Fluxo de Regras | Executar Glues de regras e consolidação do produto |
| Glue Regras | Executar regras de negócio lendo SOR pelo `INPUT_LOCK` |
| Glue Consolidação SOT | Consolidar o resultado final na camada SOT |
| Step Function Publicação SPEC | Publicar SPEC a partir da SOT |
| Glue SPEC | Gerar arquivos finais SPEC |
| Site/API | Visualização, execução manual e publicação manual da SPEC |

---

## 3. Exemplo de processo

Produto: `seguros`  
Competência: `202604`  
Origens obrigatórias:

```json
["cliente", "contrato", "parcela"]
```

Configuração do produto no DynamoDB:

```json
{
  "pk": "PROCESS_CONFIG#seguros",
  "produto": "seguros",
  "rules_execution_mode": "FIRST_ONLY",
  "spec_execution_mode": "AUTO",
  "required_origins": ["cliente", "contrato", "parcela"],
  "orchestrator_state_machine_arn": "arn:aws:states:sa-east-1:123:stateMachine:sfn-orquestradora-processamento",
  "rules_flow_state_machine_arn": "arn:aws:states:sa-east-1:123:stateMachine:sfn-fluxo-regras-seguros",
  "spec_state_machine_arn": "arn:aws:states:sa-east-1:123:stateMachine:sfn-spec-seguros"
}
```

---

## 4. Fluxo 1 — chegada do evento de origem

### 4.1 Evento recebido

Exemplo de evento vindo da origem:

```json
{
  "produto": "seguros",
  "origem": "cliente",
  "data_referencia": "202604",
  "s3_input_path": "s3://landing/seguros/202604/cliente/arquivo_001.parquet",
  "event_time": "2026-07-06T10:00:00-03:00"
}
```

### 4.2 EventBridge inicia Step Function SOR

O EventBridge envia para a Step Function SOR:

```json
{
  "produto": "seguros",
  "origem": "cliente",
  "data_referencia": "202604",
  "s3_input_path": "s3://landing/seguros/202604/cliente/arquivo_001.parquet"
}
```

---

## 5. Fluxo 2 — Step Function SOR

## 5.1 Registrar ingestão

A Step Function SOR chama a Lambda Controlar Ingestão.

A Lambda gera um `ingestion_id` sequencial, por exemplo:

```text
000001
```

Registro criado no DynamoDB:

```json
{
  "pk": "INGESTION#seguros#202604#cliente",
  "sk": "VERSION#000001",
  "produto": "seguros",
  "origem": "cliente",
  "data_referencia": "202604",
  "ingestion_id": "000001",
  "status": "PROCESSING_SOR",
  "started_at": "2026-07-06T10:00:05-03:00",
  "s3_input_path": "s3://landing/seguros/202604/cliente/arquivo_001.parquet"
}
```

### Status `PROCESSING_SOR`

Significa que a origem foi registrada e a carga SOR está em andamento.

---

## 5.2 Executar Glue SOR

A Step Function SOR chama o Glue SOR com parâmetros:

```json
{
  "produto": "seguros",
  "origem": "cliente",
  "data_referencia": "202604",
  "ingestion_id": "000001",
  "s3_input_path": "s3://landing/seguros/202604/cliente/arquivo_001.parquet",
  "s3_output_path": "s3://sor/seguros/202604/cliente/000001/"
}
```

O Glue grava os dados em:

```text
s3://sor/seguros/202604/cliente/000001/
```

---

## 5.3 Finalizar ingestão

Após o Glue SOR terminar com sucesso, a Lambda Controlar Ingestão atualiza o DynamoDB.

Registro de histórico:

```json
{
  "pk": "INGESTION#seguros#202604#cliente",
  "sk": "VERSION#000001",
  "status": "LOADED_SOR",
  "finished_at": "2026-07-06T10:12:00-03:00",
  "s3_output_path": "s3://sor/seguros/202604/cliente/000001/"
}
```

Registro `CURRENT_SOR` atualizado:

```json
{
  "pk": "CURRENT_SOR#seguros#202604",
  "sk": "ORIGIN#cliente",
  "current_ingestion_id": "000001",
  "status": "LOADED_SOR",
  "updated_at": "2026-07-06T10:12:00-03:00"
}
```

### Status `LOADED_SOR`

Significa que a carga da origem terminou com sucesso e a versão está disponível para uso.

---

## 6. Fluxo 3 — verificação automática

No final da Step Function SOR, a Lambda Orquestradora é chamada.

Ela consulta:

- configuração do produto;
- origens obrigatórias;
- origens já recebidas na competência;
- modo de execução.

Exemplo de origens recebidas até o momento:

```json
{
  "produto": "seguros",
  "data_referencia": "202604",
  "required_origins": ["cliente", "contrato", "parcela"],
  "received_origins": {
    "cliente": "000001",
    "contrato": "000004"
  }
}
```

Como ainda falta `parcela`, a Lambda não dispara o processamento.

Status da janela mensal:

```json
{
  "pk": "PROCESS_WINDOW#seguros#202604",
  "status": "WAITING_INPUTS",
  "missing_origins": ["parcela"]
}
```

### Status `WAITING_INPUTS`

Significa que ainda faltam uma ou mais origens obrigatórias para permitir execução automática.

---

## 7. Quando todas as origens chegam

Quando `cliente`, `contrato` e `parcela` estiverem carregados:

```json
{
  "received_origins": {
    "cliente": "000001",
    "contrato": "000004",
    "parcela": "000002"
  }
}
```

A Lambda Orquestradora verifica o modo:

```json
{
  "rules_execution_mode": "FIRST_ONLY"
}
```

Se for permitido executar, ela tenta travar a janela mensal com condição atômica no DynamoDB.

Exemplo de transição:

```text
WAITING_INPUTS -> LOCKED_FOR_EXECUTION
```

### Status `LOCKED_FOR_EXECUTION`

Significa que a janela foi travada para evitar execução duplicada. Mesmo que eventos cheguem em paralelo, apenas uma execução consegue avançar.

Depois disso, a Lambda inicia a Step Function Orquestradora de Processamento.

Payload:

```json
{
  "produto": "seguros",
  "data_referencia": "202604",
  "trigger_type": "AUTO",
  "input_overrides": null
}
```

---

## 8. Modos de execução das regras

| Modo | Significado |
|---|---|
| `LOCKED` | Nenhuma execução automática é permitida |
| `MANUAL` | O sistema não executa automaticamente; o usuário precisa clicar no site |
| `FIRST_ONLY` | Executa automaticamente apenas a primeira vez que todas as origens chegarem |
| `ALWAYS` | Executa automaticamente sempre que houver nova origem válida e todas as obrigatórias existirem |

---

## 9. Fluxo manual pelo site

O usuário pode selecionar versões específicas:

```json
{
  "produto": "seguros",
  "data_referencia": "202604",
  "input_overrides": {
    "cliente": "000002",
    "contrato": "000004",
    "parcela": "LATEST"
  }
}
```

O site chama a API, e a API inicia a Step Function Orquestradora de Processamento.

Payload:

```json
{
  "produto": "seguros",
  "data_referencia": "202604",
  "trigger_type": "MANUAL",
  "input_overrides": {
    "cliente": "000002",
    "contrato": "000004",
    "parcela": "LATEST"
  }
}
```

`LATEST` será resolvido pela Lambda Criar Processo usando o `CURRENT_SOR`.

---

## 10. Step Function Orquestradora de Processamento

Essa Step Function é a casca padrão reutilizável.

Ela contém:

1. Lambda Criar Processo;
2. consulta ao `PROCESS_CONFIG`;
3. chamada do Step Function Fluxo de Regras correto;
4. atualização de status;
5. decisão de publicação SPEC automática ou manual.

---

## 10.1 Lambda Criar Processo

A Lambda Criar Processo gera um `process_id` sequencial.

Exemplo:

```text
000008
```

Ela resolve as versões:

Entrada:

```json
{
  "cliente": "000002",
  "contrato": "000004",
  "parcela": "LATEST"
}
```

Após resolver `LATEST`:

```json
{
  "cliente": "000002",
  "contrato": "000004",
  "parcela": "000007"
}
```

Cria o registro `PROCESS_EXECUTION`:

```json
{
  "pk": "PROCESS_EXECUTION#seguros#202604#000008",
  "produto": "seguros",
  "data_referencia": "202604",
  "process_id": "000008",
  "trigger_type": "MANUAL",
  "status": "RUNNING",
  "input_lock": {
    "cliente": "000002",
    "contrato": "000004",
    "parcela": "000007"
  },
  "created_at": "2026-07-06T11:00:00-03:00"
}
```

### Status `RUNNING`

Significa que o processo foi criado, os inputs foram travados e as regras estão em execução.

---

## 11. INPUT_LOCK

O `INPUT_LOCK` é a fotografia das versões usadas em uma execução.

Exemplo:

```json
{
  "cliente": "000002",
  "contrato": "000004",
  "parcela": "000007"
}
```

Ele garante que, mesmo que uma nova versão de `cliente` chegue depois, o processo `000008` continuará usando exatamente as versões travadas.

---

## 12. Chamada do Step Function Fluxo de Regras

A Step Function Orquestradora consulta o `PROCESS_CONFIG`:

```json
{
  "rules_flow_state_machine_arn": "arn:aws:states:sa-east-1:123:stateMachine:sfn-fluxo-regras-seguros"
}
```

Depois chama o fluxo de regras com:

```json
{
  "produto": "seguros",
  "data_referencia": "202604",
  "process_id": "000008"
}
```

---

## 13. Step Function Fluxo de Regras

Essa Step Function é específica por produto.

Para `seguros`, pode conter:

1. Glue Regra Apólice;
2. Glue Regra Cliente;
3. Glue Regra Parcela;
4. Glue Consolidação SOT.

Cada Glue recebe:

```json
{
  "produto": "seguros",
  "data_referencia": "202604",
  "process_id": "000008"
}
```

O Glue consulta o DynamoDB:

```text
PROCESS_EXECUTION#seguros#202604#000008
```

E recupera:

```json
{
  "input_lock": {
    "cliente": "000002",
    "contrato": "000004",
    "parcela": "000007"
  }
}
```

Depois lê os caminhos SOR correspondentes:

```text
s3://sor/seguros/202604/cliente/000002/
s3://sor/seguros/202604/contrato/000004/
s3://sor/seguros/202604/parcela/000007/
```

E grava resultados na SOT:

```text
s3://sot/seguros/202604/process_id=000008/
```

---

## 14. Regras processadas

Quando o Step Function Fluxo de Regras termina com sucesso, a Step Function Orquestradora atualiza:

```json
{
  "status": "RULES_PROCESSED",
  "rules_finished_at": "2026-07-06T11:35:00-03:00"
}
```

### Status `RULES_PROCESSED`

Significa que as regras e a consolidação SOT terminaram com sucesso. A execução está pronta para publicação SPEC.

---

## 15. Decisão de publicação SPEC

A Step Function Orquestradora consulta o `PROCESS_CONFIG`:

```json
{
  "spec_execution_mode": "AUTO",
  "spec_state_machine_arn": "arn:aws:states:sa-east-1:123:stateMachine:sfn-spec-seguros"
}
```

Se `spec_execution_mode = AUTO`, ela chama a Step Function Publicação SPEC.

Payload:

```json
{
  "produto": "seguros",
  "data_referencia": "202604",
  "process_id": "000008",
  "trigger_type": "AUTO_SPEC"
}
```

Se `spec_execution_mode = MANUAL`, ela não chama a SPEC e atualiza:

```json
{
  "status": "WAITING_SPEC_PUBLICATION"
}
```

### Status `WAITING_SPEC_PUBLICATION`

Significa que a SOT está pronta, mas a publicação SPEC depende de ação manual do usuário.

---

## 16. Publicação manual da SPEC

Quando o usuário clica em `Publicar SPEC`, o site chama a API:

```json
{
  "produto": "seguros",
  "data_referencia": "202604",
  "process_id": "000008"
}
```

A API chama a Lambda Publicar SPEC Manual.

A Lambda:

1. consulta o `PROCESS_CONFIG#seguros`;
2. valida se o `process_id` está `RULES_PROCESSED` ou `WAITING_SPEC_PUBLICATION`;
3. pega o `spec_state_machine_arn` correto;
4. inicia a Step Function Publicação SPEC.

---

## 17. Step Function Publicação SPEC

A Step Function SPEC recebe:

```json
{
  "produto": "seguros",
  "data_referencia": "202604",
  "process_id": "000008",
  "trigger_type": "AUTO_SPEC"
}
```

Primeiro valida o processo no DynamoDB.

Depois atualiza:

```json
{
  "status": "PUBLISHING_SPEC",
  "spec_started_at": "2026-07-06T11:36:00-03:00"
}
```

### Status `PUBLISHING_SPEC`

Significa que a publicação da SPEC está em andamento.

---

## 18. Glue SPEC

O Glue SPEC recebe:

```json
{
  "produto": "seguros",
  "data_referencia": "202604",
  "process_id": "000008"
}
```

Ele lê a SOT:

```text
s3://sot/seguros/202604/process_id=000008/
```

E publica SPEC:

```text
s3://spec/seguros/202604/process_id=000008/
```

---

## 19. Processo publicado

Após o Glue SPEC terminar com sucesso:

```json
{
  "status": "PUBLISHED",
  "spec_finished_at": "2026-07-06T11:45:00-03:00",
  "spec_output_path": "s3://spec/seguros/202604/process_id=000008/"
}
```

### Status `PUBLISHED`

Significa que o processo foi publicado com sucesso na camada SPEC.

---

## 20. Status principais

| Status | Onde aparece | Significado |
|---|---|---|
| `PROCESSING_SOR` | Ingestão | A carga SOR está em andamento |
| `LOADED_SOR` | Ingestão | A origem foi carregada com sucesso na SOR |
| `WAITING_INPUTS` | Janela mensal | Ainda faltam origens obrigatórias |
| `LOCKED_FOR_EXECUTION` | Janela mensal | A janela foi travada para iniciar execução automática |
| `RUNNING` | Processo | Regras/SOT em execução |
| `RULES_PROCESSED` | Processo | Regras e SOT finalizaram com sucesso |
| `WAITING_SPEC_PUBLICATION` | Processo | SPEC aguardando publicação manual |
| `PUBLISHING_SPEC` | Processo | SPEC em publicação |
| `PUBLISHED` | Processo | SPEC publicada com sucesso |
| `FAILED_SOR` | Ingestão | Falha na carga SOR |
| `FAILED_RULES` | Processo | Falha nas regras/SOT |
| `FAILED_SPEC` | Processo | Falha na publicação SPEC |

---

## 21. Visão do site

O site consegue mostrar, por produto e competência:

- todas as origens obrigatórias;
- última versão disponível de cada origem;
- histórico de versões recebidas;
- horário de início e fim de cada ingestão;
- status da carga SOR;
- process_id criado;
- versões travadas no `INPUT_LOCK`;
- status das regras;
- status da SPEC;
- botão de execução manual;
- botão de publicação manual da SPEC quando aplicável.

Exemplo de visão mensal:

```text
Produto: seguros
Competência: 202604

Origem      Última versão    Status       Início      Fim
cliente     000003           LOADED_SOR   10:00       10:12
contrato    000004           LOADED_SOR   10:15       10:21
parcela     000007           LOADED_SOR   10:30       10:34

Process ID atual: 000008
Status: PUBLISHED
SPEC: publicada
```

---

## 22. Resumo final

O fluxo completo é:

```text
Evento da origem
 -> Step Function SOR
 -> Glue SOR
 -> DynamoDB registra histórico e CURRENT_SOR
 -> Lambda Orquestradora avalia execução automática
 -> Step Function Orquestradora de Processamento
 -> Lambda Criar Processo gera process_id e INPUT_LOCK
 -> Step Function Fluxo de Regras executa Glues do produto
 -> SOT consolidada
 -> Step Function Orquestradora decide SPEC AUTO/MANUAL
 -> Step Function Publicação SPEC
 -> Glue SPEC
 -> Status PUBLISHED
```

A arquitetura separa claramente:

- ingestão de dados;
- controle operacional;
- execução de regras;
- publicação final;
- interação manual pelo site.
