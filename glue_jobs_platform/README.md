# Glue Jobs Platform

Plataforma nova para padronizar jobs AWS Glue sem alterar os jobs atuais.

## Estrutura

- `src/glue_jobs_lib/`: biblioteca empacotavel como `.whl`.
- `jobs/job_sot/`: job fino SOT.
- `jobs/job_spec/`: job fino SPEC.
- `jobs/job_sor/`: job fino SOR.
- `tests/`: testes unitarios da biblioteca e contratos dos jobs finos.

## Contratos

- SOT exige `PROCESS_ID`, `DATA_REF` e `INGESTION_ID`.
- SPEC exige `PROCESS_ID` e `DATA_REF`; resolve `INGESTION_ID` no DynamoDB quando a config usa esse placeholder.
- SOR exige `INGESTION_ID` e resolve metadata no DynamoDB.

## Build

```bash
python -m build glue_jobs_platform
```
