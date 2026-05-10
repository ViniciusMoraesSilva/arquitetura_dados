from __future__ import annotations

from pathlib import Path


def test_job_sot_uses_sot_resolver():
    content = Path("glue_jobs_platform/jobs/job_sot/main.py").read_text(encoding="utf-8")

    assert "SotPlanResolver" in content
    assert "runner.run(resolver)" in content


def test_job_spec_uses_spec_resolver():
    content = Path("glue_jobs_platform/jobs/job_spec/main.py").read_text(encoding="utf-8")

    assert "SpecPlanResolver" in content
    assert "runner.run(resolver)" in content


def test_job_sor_uses_sor_resolver():
    content = Path("glue_jobs_platform/jobs/job_sor/main.py").read_text(encoding="utf-8")

    assert "SorPlanResolver" in content
    assert "runner.run(resolver)" in content
