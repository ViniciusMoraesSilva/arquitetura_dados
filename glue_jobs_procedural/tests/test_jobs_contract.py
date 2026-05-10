from __future__ import annotations

from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def test_job_sot_tem_fluxo_procedural_e_usa_lib_sot():
    content = (PACKAGE_ROOT / "jobs/job_sot/main.py").read_text(encoding="utf-8")

    assert "class " not in content
    assert "sot." in content
    assert "def executar_fluxo_de_processamento" in content


def test_job_spec_tem_fluxo_procedural_e_usa_lib_spec():
    content = (PACKAGE_ROOT / "jobs/job_spec/main.py").read_text(encoding="utf-8")

    assert "class " not in content
    assert "spec." in content
    assert "def executar_fluxo_de_processamento" in content


def test_job_sor_tem_fluxo_procedural_e_usa_lib_sor():
    content = (PACKAGE_ROOT / "jobs/job_sor/main.py").read_text(encoding="utf-8")

    assert "class " not in content
    assert "sor." in content
    assert "def executar_fluxo_de_processamento" in content
