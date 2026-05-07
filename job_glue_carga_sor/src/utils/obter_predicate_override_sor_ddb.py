"""Utilitario para leitura de predicate override SOR no DynamoDB."""

from __future__ import annotations

from typing import Any


def montar_chave_predicate_override_sor(metadata: dict[str, Any]) -> dict[str, str]:
    """Monta a chave do override de predicate SOR."""
    return {
        "pk": f"PROCESS_TABLE#{metadata['process_name']}#{metadata['sor_table_name']}",
        "sk": f"SOURCE#{metadata['source_table_name']}",
    }


def obter_predicate_override_sor_ddb(
    dynamodb_resource: Any,
    metadata: dict[str, Any],
) -> str | None:
    """
    Busca o predicate override SOR para process_name/sor_table_name/source_table_name.

    Returns:
        Predicate quando existir item habilitado, senao ``None``.
    """
    tabela = dynamodb_resource.Table("glue_predicate_overrides")
    resposta = tabela.get_item(Key=montar_chave_predicate_override_sor(metadata))
    item = resposta.get("Item")

    if not item or not item.get("enabled", False):
        return None

    predicate = item.get("predicate")
    if not predicate:
        return None

    return str(predicate)


def obter_predicates_override_sot_ddb(
    dynamodb_resource: Any,
    job_name: str,
    process_id: str,
) -> dict[str, str]:
    """
    Busca predicates override SOT para uma execucao por process_id.

    O item esperado usa ``pk = PROCESS_ID#<process_id>`` e
    ``sk = JOB#<job_name>``, com atributo ``predicates`` no formato
    ``nome_fonte -> predicate``.
    """
    tabela = dynamodb_resource.Table("glue_predicate_overrides")
    resposta = tabela.get_item(
        Key={
            "pk": f"PROCESS_ID#{process_id}",
            "sk": f"JOB#{job_name}",
        }
    )
    item = resposta.get("Item")

    if not item or not item.get("enabled", False):
        return {}

    predicates = item.get("predicates")
    if not isinstance(predicates, dict):
        return {}

    return {
        str(nome_fonte): str(predicate)
        for nome_fonte, predicate in predicates.items()
        if nome_fonte and predicate
    }
