# ADR 0001: Job Glue Orientado Por Configuração

## Status

Aceita.

## Contexto

O job precisa executar um fluxo Glue/PySpark com fontes, filtros, SQL e destino que podem variar por ambiente e por execução. O código atual separa responsabilidades:

- Python orquestra o job;
- JSON declara origens e destino;
- SQL concentra a transformação;
- argumentos Glue carregam contexto de execução;
- DynamoDB pode fornecer override operacional de predicates.

## Decisão

Manter o job orientado por configuração declarativa:

- origens ficam em `src/config/config_origem_dados.json`;
- destino fica em `src/config/config_destino_dados.json`;
- transformação fica em `src/sql/consultas.sql`;
- `src/main.py` resolve contexto, placeholders, predicates e coordena as etapas;
- utilitários em `src/utils/` encapsulam leitura de config, execução SQL, gravação e DynamoDB.

## Consequências

Benefícios:

- novas fontes podem ser adicionadas sem alterar o orquestrador, quando já usam tipos suportados;
- troca entre destino `catalogo` e `csv` fica centralizada no JSON;
- SQL pode evoluir separado da lógica de leitura e escrita;
- overrides de predicate permitem ajustes operacionais por execução.

Custos:

- contratos dos JSONs precisam ser preservados com cuidado;
- erros de placeholder, SQL ou partição aparecem em tempo de execução;
- execução completa depende de ambiente AWS Glue e recursos externos;
- mudanças em SQL podem quebrar destino ou consumidores se o schema final mudar.

## Regras Para Evolução

- Adicionar novo tipo de origem exige atualizar mapeamento em `src/main.py` e documentação.
- Adicionar novo tipo de destino exige atualizar `gravar_catalogo_glue.py`, config e documentação.
- Adicionar placeholder exige atualizar `montar_contexto_templates_origem()` e docs.
- Alterar campos obrigatórios dos JSONs exige atualizar validação, exemplos e docs.
