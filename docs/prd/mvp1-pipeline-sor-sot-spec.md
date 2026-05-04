# PRD: MVP1 Pipeline AWS SOR/SOT/SPEC

Status: needs-triage
Labels: needs-triage
Date: 2026-04-30

## Problem Statement

O projeto precisa sair de um plano conceitual para uma arquitetura executável de pipeline de dados na AWS, com separação clara entre ingestão, processamento FULL por produto/processo e publicação final. Hoje o repositório contém principalmente o plano do MVP1 e não possui ainda a estrutura de código, infraestrutura, orquestração e contratos compartilhados necessários para subir a solução.

O usuário também possui um exemplo externo de JOB Glue que já resolve parte importante do problema: ler fontes declarativas, executar SQL sequencial e gravar resultados. Porém esse JOB Glue atual foi desenhado em torno de `DATA_REF`, `JOB_NAME`, `TABLE_PREDICATES` e uma tabela DynamoDB própria para overrides de predicate. Isso não atende diretamente ao contrato desejado do MVP1, em que os módulos recebem somente `PROCESS_ID`, consultam `INPUT_LOCK` no DynamoDB de controle e leem SOR exclusivamente por `ingestion_id`.

O MVP1 precisa criar uma base de implementação dentro deste repositório, com pastas separadas para cada recurso, Terraform para subir na AWS e uma cópia/adaptação local de `src/` e `test/` do JOB Glue sem alterar o repositório externo original e sem copiar o Terraform externo.

## Solution

Construir neste repositório uma arquitetura AWS completa para o MVP1 do pipeline SOR/SOT/SPEC. A solução terá ingestão genérica, controle transacional em DynamoDB, carga SOR, preparação FULL, módulos Glue por processo, consolidação SOT, publicação SPEC e Terraform para provisionar a infraestrutura em AWS.

A ingestão será independente de processo/produto. Cada carga criará um `ingestion_id`, registrará metadados no DynamoDB e atualizará o ponteiro `CURRENT_SOR` ao concluir com sucesso. O processamento FULL será específico por processo/produto, começando pelo processo `seguros`. A Step Function FULL receberá `data_referencia`, injetará `process_name`, chamará uma Lambda genérica para criar `process_id` e travar as entradas necessárias a partir de `FULL_CONFIG`.

Os módulos de processamento receberão somente `PROCESS_ID`. Eles consultarão `pipeline_control`, obterão os `INPUT_LOCK` das tabelas necessárias, resolverão os `ingestion_id` bloqueados para aquele processo e lerão a SOR usando apenas predicates por `ingestion_id`. As saídas SOT e SPEC serão gravadas por `process_id`, preservando rastreabilidade e evitando sobrescrita indevida entre execuções.

O JOB Glue externo será tratado como base operacional. Uma cópia/adaptação local de `src/` e testes será mantida no repositório para reaproveitar `main.py`, configuração declarativa, leitura de fontes, execução SQL sequencial e gravação, mas com o contrato do MVP1. O repositório externo de JOB Glue não será modificado.

## User Stories

1. As a data platform engineer, I want the MVP1 architecture to be created inside this repository, so that the implementation can evolve independently from the external JOB Glue example.
2. As a data platform engineer, I want Terraform to provision the MVP1 AWS resources, so that environments can be recreated consistently.
3. As a data platform engineer, I want Terraform to be parameterized by environment, so that `dev` can be created first without blocking future `hom` and `prd`.
4. As a data platform engineer, I want the default AWS region to be `us-east-2`, so that all resources are created in the intended region.
5. As a data platform engineer, I want one S3 bucket per environment with prefixes per layer, so that SOR, SOT, SPEC, scripts and temporary files are organized without excessive bucket sprawl.
6. As a data platform engineer, I want a DynamoDB control table named around the pipeline control plane, so that ingestion, processing and locking metadata have a shared source of truth.
7. As a data platform engineer, I want the DynamoDB table to use generic `PK` and `SK` keys, so that multiple control entities can coexist in the same table.
8. As a data platform engineer, I want `FULL_CONFIG` records in DynamoDB, so that each FULL process can declare which SOR tables must be locked before execution.
9. As a data platform engineer, I want the initial `seguros` FULL config to include placeholder tables, so that the MVP can be implemented before final domain table names are locked.
10. As a data ingestion operator, I want ingestion to receive source metadata and partition values, so that loads can be prepared without knowing any downstream product.
11. As a data ingestion operator, I want ingestion to support Glue Catalog sources, so that existing cataloged datasets can be loaded into SOR.
12. As a data ingestion operator, I want ingestion to support CSV/S3 sources, so that file-based sources can also be onboarded in MVP1.
13. As a data ingestion operator, I want each ingestion to generate an `ingestion_id`, so that every SOR version can be referenced immutably.
14. As a data ingestion operator, I want ingestion metadata to be stored before the Glue load starts, so that failures can be traced even when the load does not complete.
15. As a data ingestion operator, I want successful SOR loads to update `CURRENT_SOR`, so that FULL processing can discover the latest valid version for a table and reference date.
16. As a data ingestion operator, I want failed SOR loads not to update `CURRENT_SOR`, so that incomplete data cannot become the current version.
17. As a data ingestion operator, I want ingestion history to be retained, so that previous versions can be audited after `CURRENT_SOR` moves forward.
18. As a Glue job developer, I want the generic SOR job to receive only `INGESTION_ID`, so that source read details come from the control plane rather than duplicated orchestration payloads.
19. As a Glue job developer, I want the generic SOR job to add `ingestion_id` to all rows, so that downstream modules can read exact SOR versions.
20. As a Glue job developer, I want SOR to be partitioned by `ingestion_id`, so that modules can efficiently read the locked version.
21. As a pipeline orchestrator, I want a FULL Step Function for `seguros`, so that the product-specific flow has a clear executable entrypoint.
22. As a pipeline orchestrator, I want the external FULL payload to contain only `data_referencia`, so that callers do not need to understand internal process wiring.
23. As a pipeline orchestrator, I want the Step Function to inject `process_name`, so that the preparation Lambda can remain generic across products.
24. As a pipeline orchestrator, I want the preparation Lambda to create a `process_id`, so that every FULL execution has a stable trace identifier.
25. As a pipeline orchestrator, I want the preparation Lambda to validate all required `CURRENT_SOR` entries before modules run, so that missing inputs fail early.
26. As a pipeline orchestrator, I want input locks to be created once per process and table, so that modules process a consistent snapshot even if new ingestions arrive later.
27. As a pipeline orchestrator, I want lock creation to avoid overwriting existing locks, so that process reproducibility is protected.
28. As a pipeline orchestrator, I want the process header and process list to be persisted, so that executions can be discovered by process and reference date.
29. As a Glue module developer, I want modules to receive only `PROCESS_ID`, so that module contracts stay small and stable.
30. As a Glue module developer, I want modules to resolve their SOR inputs from DynamoDB locks, so that orchestration payloads do not carry predicates or ingestion details.
31. As a Glue module developer, I want modules to read SOR only using `ingestion_id`, so that locked snapshots are respected and `data_referencia` is not accidentally used as the version selector.
32. As a Glue module developer, I want a local adaptation of the JOB Glue structure, so that SQL-driven module development stays familiar.
33. As a Glue module developer, I want each module to have separate configuration and SQL, so that modules can evolve independently.
34. As a Glue module developer, I want the adapted JOB Glue to remove expensive read counts, so that the MVP avoids unnecessary Spark actions.
35. As a Glue module developer, I want SOT outputs to be partitioned by `process_id`, so that process runs are traceable and not overwritten by later executions.
36. As a publication developer, I want SPEC publication to use `process_id`, so that final published data can be traced back to the FULL execution.
37. As a publication developer, I want consolidation and publication to be part of the same FULL Step Function, so that the end-to-end run is observable in one orchestration.
38. As a platform maintainer, I want Lambda functions to share common DynamoDB helper code, so that key construction and error handling are consistent.
39. As a platform maintainer, I want ID generation to be centralized, so that ingestion and process identifiers follow predictable conventions.
40. As a platform maintainer, I want time formatting to be centralized, so that timestamps are consistent across metadata records.
41. As a platform maintainer, I want AWS IAM policies to be provisioned with Terraform, so that Glue, Lambda and Step Functions have explicit permissions.
42. As a platform maintainer, I want Glue databases for SOR, SOT and SPEC to be provisioned, so that catalog organization reflects pipeline layers.
43. As a platform maintainer, I want scripts to be uploaded to S3 by the infrastructure flow, so that Glue Jobs can run deployed artifacts.
44. As a platform maintainer, I want placeholders for real seguros modules, so that the MVP can be wired before final business transformations are supplied.
45. As a platform maintainer, I want the original JOB Glue repository to remain unchanged, so that this architecture does not break the existing example or any work depending on it.
46. As a tester, I want DynamoDB helpers to be testable in isolation, so that key construction and missing-item behavior can be verified without AWS.
47. As a tester, I want Lambda handlers to be testable with mocked dependencies, so that ingestion and FULL preparation behavior can be verified cheaply.
48. As a tester, I want the adapted JOB Glue lock resolution to be testable, so that `PROCESS_ID` correctly resolves to `ingestion_id` predicates.
49. As a tester, I want Terraform validation to be part of acceptance, so that syntax and provider wiring issues are caught before apply.
50. As a future implementer, I want a README with execution examples, so that I can run Terraform and invoke ingestion/FULL flows without rediscovering contracts.

## Implementation Decisions

- The implementation will create an AWS MVP1 pipeline using Lambda, Glue Jobs, Step Functions, DynamoDB, S3 and Glue Catalog.
- The repository will be organized around separated resource folders for infrastructure, Lambdas, Glue jobs and shared Python modules.
- Terraform will provision the full MVP1 infrastructure rather than only the DynamoDB control table.
- The first environment will be `dev`, but Terraform will be parameterized with an `environment` variable for future environments.
- The default AWS region will be `us-east-2`.
- S3 will use one bucket per environment, with layer and artifact prefixes for SOR, SOT, SPEC, scripts and temporary data.
- The control plane will use one DynamoDB table with `PK` and `SK` keys.
- The control table will store ingestion metadata, ingestion history, current SOR pointers, full process configuration, process headers, process listings and input locks.
- The canonical reference date format will be `YYYYMM`.
- Ingestion will remain generic and will not receive or store product-specific process intent.
- A generic SOR Glue job will support both Glue Catalog and CSV/S3 input sources using the copied/adapted JobGlue `main.py` with `JOB_MODE=sor`.
- Successful SOR finalization will update `CURRENT_SOR`; failed finalization will not.
- FULL orchestration will be product-specific at the Step Function layer and generic at the preparation Lambda layer.
- The `seguros` FULL Step Function will call Glue Jobs directly in sequence rather than using nested Step Functions.
- The preparation Lambda will create `process_id`, validate required SOR inputs and create immutable input locks.
- Modules will receive `PROCESS_ID` as the only execution identifier; technical arguments may locate config, SQL, environment and control table.
- Modules will derive SOR read predicates from locked `ingestion_id` values.
- Modules will not use `data_referencia` as a SOR read predicate.
- The external JOB Glue project will not be modified.
- A local copied/adapted JOB Glue module will be created in this repository from the external `src/` and tests, excluding external Terraform and local artifacts.
- The local copied/adapted JOB Glue module will preserve `main.py`, utilities, config-driven reads, SQL sequencing and destination writing.
- The local copied/adapted JOB Glue module will use `JOB_MODE=sor|process`, replacing `DATA_REF` and predicate override behavior as version selectors with `INGESTION_ID` or `PROCESS_ID -> INPUT_LOCK`.
- The local copied/adapted JOB Glue module will avoid unnecessary Spark `count` actions during MVP1 reads.
- SOT and SPEC writes will include `process_id` partitioning or equivalent traceability.
- `TABLE_PREDICATES` and `glue_predicate_overrides` will not be part of the primary MVP1 processing contract.
- The first seguros tables and modules will be placeholders: `contratos`, `clientes`, `parcelas`, modules x/y/z, consolidation and publication.
- Backend remote state for Terraform is not required for MVP1; local state is acceptable initially, with remote backend documentation allowed.

## Testing Decisions

- Tests should verify external behavior and contracts rather than internal implementation details.
- DynamoDB control helpers should be tested as a deep module because they encapsulate key construction, common reads/writes and required-item error handling behind a stable interface.
- ID and time helpers should be tested as deep modules because they provide small stable interfaces used across Lambdas and Glue jobs.
- Lambda handlers should be tested with mocked control-plane dependencies to verify payload handling, metadata writes, status transitions and failure behavior.
- FULL preparation tests should cover all inputs present, missing current SOR, duplicate locks and successful process creation.
- The generic SOR Glue job should be tested at the argument/configuration boundary with mocked Glue/Spark dependencies where practical.
- The adapted JOB Glue process module should be tested for `PROCESS_ID` to `INPUT_LOCK` resolution and for producing `ingestion_id` predicates.
- Tests should assert that modules do not use `data_referencia` to read SOR.
- Terraform should be checked with formatting and validation commands.
- Terraform planning against the dev example variables should be part of acceptance, without requiring apply in automated local tests.
- Existing JOB Glue tests in the external project are copied as prior art and adapted into MVP1 contract tests for argument parsing, declarative source mapping, input-lock predicate resolution and destination configuration.
- The MVP1 test suite should prefer unit tests and mocked integration tests until real AWS account execution is available.

## Out of Scope

- Modifying the external JOB Glue repository.
- Building a site, UI or self-service portal.
- Implementing real business transformations for seguros beyond placeholder module structure.
- Creating a production-grade multi-account deployment strategy.
- Enforcing remote Terraform state as a hard requirement.
- Implementing advanced observability dashboards.
- Implementing row counts for SOR MVP1 loads.
- Supporting arbitrary incremental processing beyond the FULL flow described here.
- Replacing the control-plane DynamoDB design with a different state store.
- Supporting regions other than `us-east-2` as the initial default.
- Finalizing real table names if the domain model is still using placeholders.

## Further Notes

The most important architectural boundary is between ingestion versioning and process execution. Ingestion creates immutable SOR versions identified by `ingestion_id`; FULL execution creates a process snapshot identified by `process_id`; modules consume only the process snapshot.

The external JOB Glue project is the base pattern for SQL-driven Glue jobs, but its current DynamoDB override contract does not match the MVP1. The local adaptation should keep the copied `main.py`, declarative configs, SQL sequencing and destination writing; replace argument and predicate resolution with `JOB_MODE`, `INGESTION_ID` and the control-plane lock model.

The initial implementation should favor deep modules around DynamoDB control access, ID generation and process input resolution. Those modules carry the core domain rules and should be easy to test without AWS.
