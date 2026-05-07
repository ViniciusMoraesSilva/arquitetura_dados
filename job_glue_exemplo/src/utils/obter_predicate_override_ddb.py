"""Utilitário para leitura de predicate override no DynamoDB."""


def obter_predicate_override_ddb(
    dynamodb_resource, job_name: str, data_ref: str
) -> dict[str, str]:
    """
    Busca o mapa de predicates override para uma execução do job.

    Args:
        dynamodb_resource: Recurso boto3 do DynamoDB.
        job_name: Nome do job em execução.
        data_ref: Data de referência da execução.

    Returns:
        Dicionário ``tabela -> predicate`` quando existir item habilitado no
        DynamoDB. Retorna dicionário vazio quando o item não existir, estiver
        desabilitado ou não contiver predicates válidos.
    """
    tabela = dynamodb_resource.Table("glue_predicate_overrides")
    resposta = tabela.get_item(
        Key={
            "pk": data_ref,
            "sk": job_name,
        }
    )
    item = resposta.get("Item")

    if not item or not item.get("enabled", False):
        return {}

    predicates = item.get("predicates")
    if not isinstance(predicates, dict):
        return {}

    return {
        str(nome_tabela): str(predicate)
        for nome_tabela, predicate in predicates.items()
        if nome_tabela and predicate
    }
