# Glue Jobs Procedural

Versao procedural da plataforma Glue, criada para manter a essencia do `job_glue_exemplo`: `main.py` explicito, etapas claras e libs por funcoes.

## Estrutura

- `src/glue_jobs_lib/common.py`: funcoes comuns de config, templates, leitura, SQL e gravacao.
- `src/glue_jobs_lib/sot.py`: funcoes especificas SOT.
- `src/glue_jobs_lib/spec.py`: funcoes especificas SPEC.
- `src/glue_jobs_lib/sor.py`: funcoes especificas SOR.
- `jobs/job_sot`, `jobs/job_spec`, `jobs/job_sor`: jobs finos com `main.py` procedural.

## Build

```bash
python -m pip wheel ./glue_jobs_procedural -w /tmp/glue_jobs_procedural_dist --no-deps
```
