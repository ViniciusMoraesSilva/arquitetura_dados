from __future__ import annotations

from copy import deepcopy
import os


class MissingItemError(KeyError):
    pass


class DuplicateItemError(RuntimeError):
    pass


def key(pk: str, sk: str) -> dict[str, str]:
    return {"PK": pk, "SK": sk}


class InMemoryControlPlane:
    def __init__(self):
        self._items: dict[tuple[str, str], dict] = {}

    def put_item(self, pk: str, sk: str, item: dict, *, condition_not_exists: bool = False) -> dict:
        item_key = (pk, sk)
        if condition_not_exists and item_key in self._items:
            raise DuplicateItemError(f"item already exists: {pk} / {sk}")
        stored = {"PK": pk, "SK": sk, **deepcopy(item)}
        self._items[item_key] = stored
        return deepcopy(stored)

    def get_item(self, pk: str, sk: str) -> dict | None:
        item = self._items.get((pk, sk))
        return deepcopy(item) if item else None

    def require_item(self, pk: str, sk: str) -> dict:
        item = self.get_item(pk, sk)
        if not item:
            raise MissingItemError(f"missing item: {pk} / {sk}")
        return item

    def query_pk(self, pk: str, sk_prefix: str | None = None) -> list[dict]:
        items = []
        for (item_pk, item_sk), item in self._items.items():
            if item_pk != pk:
                continue
            if sk_prefix is not None and not item_sk.startswith(sk_prefix):
                continue
            items.append(deepcopy(item))
        return sorted(items, key=lambda item: item["SK"])

    def create_ingestion_metadata(self, ingestion_id: str, metadata: dict) -> dict:
        return self.put_item(
            f"INGESTION_ID#{ingestion_id}",
            "METADATA",
            metadata,
            condition_not_exists=True,
        )

    def get_ingestion_metadata(self, ingestion_id: str) -> dict:
        return self.require_item(f"INGESTION_ID#{ingestion_id}", "METADATA")

    def update_ingestion_metadata(self, ingestion_id: str, updates: dict) -> dict:
        current = self.get_ingestion_metadata(ingestion_id)
        current.update(deepcopy(updates))
        return self.put_item(f"INGESTION_ID#{ingestion_id}", "METADATA", current)

    def put_ingestion_history(self, table_name: str, data_referencia: str, ingestion_id: str, item: dict) -> dict:
        return self.put_item(
            f"INGESTION_HISTORY#{table_name}#{data_referencia}",
            f"INGESTION#{ingestion_id}",
            item,
        )

    def update_current_sor(self, table_name: str, data_referencia: str, item: dict) -> dict:
        return self.put_item(f"CURRENT_SOR#{table_name}#{data_referencia}", "CURRENT", item)

    def get_current_sor(self, table_name: str, data_referencia: str) -> dict | None:
        return self.get_item(f"CURRENT_SOR#{table_name}#{data_referencia}", "CURRENT")

    def put_full_config_table(self, process_name: str, table_name: str, item: dict | None = None) -> dict:
        payload = {"process_name": process_name, "table_name": table_name}
        if item:
            payload.update(item)
        return self.put_item(f"FULL_CONFIG#{process_name}", f"TABLE#{table_name}", payload)

    def list_full_config_tables(self, process_name: str) -> list[str]:
        return [item["table_name"] for item in self.query_pk(f"FULL_CONFIG#{process_name}", "TABLE#")]

    def put_process_header(self, process_id: str, item: dict) -> dict:
        return self.put_item(f"PROCESS#{process_id}", "HEADER", item)

    def get_process_header(self, process_id: str) -> dict:
        item = self.get_item(f"PROCESS#{process_id}", "HEADER")
        if item:
            return item
        return {"process_id": process_id}

    def update_process_header(self, process_id: str, updates: dict) -> dict:
        current = self.get_process_header(process_id)
        current.update(deepcopy(updates))
        return self.put_process_header(process_id, current)

    def put_process_list(self, process_name: str, data_referencia: str, process_id: str, item: dict) -> dict:
        payload = {"process_name": process_name, "data_referencia": data_referencia, "process_id": process_id}
        payload.update(item)
        return self.put_item(
            f"PROCESS_LIST#{process_name}#{data_referencia}",
            f"PROCESS#{process_id}",
            payload,
        )

    def get_process_list(self, process_name: str, data_referencia: str) -> list[dict]:
        return self.query_pk(f"PROCESS_LIST#{process_name}#{data_referencia}", "PROCESS#")

    def create_input_lock(self, process_id: str, table_name: str, item: dict) -> dict:
        payload = {"process_id": process_id, "table_name": table_name}
        payload.update(deepcopy(item))
        return self.put_item(
            f"PROCESS#{process_id}",
            f"INPUT_LOCK#{table_name}",
            payload,
            condition_not_exists=True,
        )

    def get_input_lock(self, process_id: str, table_name: str) -> dict | None:
        return self.get_item(f"PROCESS#{process_id}", f"INPUT_LOCK#{table_name}")

    def require_input_lock(self, process_id: str, table_name: str) -> dict:
        return self.require_item(f"PROCESS#{process_id}", f"INPUT_LOCK#{table_name}")

    def list_input_locks(self, process_id: str) -> list[dict]:
        return self.query_pk(f"PROCESS#{process_id}", "INPUT_LOCK#")

    def put_step_status(self, process_id: str, step_name: str, item: dict) -> dict:
        payload = {"process_id": process_id, "step_name": step_name}
        payload.update(deepcopy(item))
        return self.put_item(f"PROCESS#{process_id}", f"STEP#{step_name}", payload)

    def get_step_status(self, process_id: str, step_name: str) -> dict | None:
        return self.get_item(f"PROCESS#{process_id}", f"STEP#{step_name}")


class DynamoDBControlPlane:
    def __init__(self, table_name: str, dynamodb_resource=None):
        if dynamodb_resource is None:
            import boto3

            dynamodb_resource = boto3.resource("dynamodb")
        self.table = dynamodb_resource.Table(table_name)

    def put_item(self, pk: str, sk: str, item: dict, *, condition_not_exists: bool = False) -> dict:
        stored = {"PK": pk, "SK": sk, **deepcopy(item)}
        kwargs = {"Item": stored}
        if condition_not_exists:
            kwargs["ConditionExpression"] = "attribute_not_exists(PK) AND attribute_not_exists(SK)"
        try:
            self.table.put_item(**kwargs)
        except Exception as exc:
            error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if error_code == "ConditionalCheckFailedException":
                raise DuplicateItemError(f"item already exists: {pk} / {sk}") from exc
            raise
        return deepcopy(stored)

    def get_item(self, pk: str, sk: str) -> dict | None:
        response = self.table.get_item(Key={"PK": pk, "SK": sk})
        return response.get("Item")

    def require_item(self, pk: str, sk: str) -> dict:
        item = self.get_item(pk, sk)
        if not item:
            raise MissingItemError(f"missing item: {pk} / {sk}")
        return item

    def query_pk(self, pk: str, sk_prefix: str | None = None) -> list[dict]:
        from boto3.dynamodb.conditions import Key

        expression = Key("PK").eq(pk)
        if sk_prefix is not None:
            expression = expression & Key("SK").begins_with(sk_prefix)
        response = self.table.query(KeyConditionExpression=expression)
        return response.get("Items", [])

    create_ingestion_metadata = InMemoryControlPlane.create_ingestion_metadata
    get_ingestion_metadata = InMemoryControlPlane.get_ingestion_metadata
    update_ingestion_metadata = InMemoryControlPlane.update_ingestion_metadata
    put_ingestion_history = InMemoryControlPlane.put_ingestion_history
    update_current_sor = InMemoryControlPlane.update_current_sor
    get_current_sor = InMemoryControlPlane.get_current_sor
    put_full_config_table = InMemoryControlPlane.put_full_config_table
    list_full_config_tables = InMemoryControlPlane.list_full_config_tables
    put_process_header = InMemoryControlPlane.put_process_header
    get_process_header = InMemoryControlPlane.get_process_header
    update_process_header = InMemoryControlPlane.update_process_header
    put_process_list = InMemoryControlPlane.put_process_list
    get_process_list = InMemoryControlPlane.get_process_list
    create_input_lock = InMemoryControlPlane.create_input_lock
    get_input_lock = InMemoryControlPlane.get_input_lock
    require_input_lock = InMemoryControlPlane.require_input_lock
    list_input_locks = InMemoryControlPlane.list_input_locks
    put_step_status = InMemoryControlPlane.put_step_status
    get_step_status = InMemoryControlPlane.get_step_status


def control_plane_from_env() -> DynamoDBControlPlane:
    table_name = os.environ["CONTROL_TABLE_NAME"]
    return DynamoDBControlPlane(table_name)
