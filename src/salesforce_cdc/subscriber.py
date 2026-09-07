import io
import queue
from datetime import UTC, datetime
from typing import Iterator

import avro.io
import avro.schema

from generated import pubsub_api_pb2
from src.salesforce_cdc.pubsub_client import (
    OPPORTUNITY_CHANGE_TOPIC,
    SalesforcePubSubClient,
)

EVENT_LIMIT = 5


def _decode_payload(schema, payload: bytes) -> dict:
    decoder = avro.io.BinaryDecoder(io.BytesIO(payload))
    return avro.io.DatumReader(schema).read(decoder)


def _field_names_from_bitmap(bitmap: str, schema) -> list[str]:
    hex_value = bitmap.removeprefix("0x")
    bits = f"{int(hex_value, 16):0{len(hex_value) * 4}b}"[::-1]
    return [
        schema.fields[index].name
        for index, bit in enumerate(bits)
        if bit == "1" and index < len(schema.fields)
    ]


def _value_schema(schema):
    if schema.type == "union":
        non_null_schemas = [item for item in schema.schemas if item.type != "null"]
        record_schemas = [item for item in non_null_schemas if item.type == "record"]
        if record_schemas:
            return record_schemas[0]
        if len(non_null_schemas) == 1:
            return non_null_schemas[0]
    return schema


def _changed_field_names(schema, bitmap_fields: list[str]) -> list[str]:
    changed_fields: list[str] = []
    for bitmap_field in bitmap_fields:
        if bitmap_field.startswith("0x"):
            changed_fields.extend(_field_names_from_bitmap(bitmap_field, schema))
            continue

        parent_position, separator, child_bitmap = bitmap_field.partition("-")
        if not separator or not parent_position.isdigit():
            continue

        parent_field = schema.fields[int(parent_position)]
        child_schema = _value_schema(parent_field.type)
        if child_schema.type == "record":
            child_fields = _field_names_from_bitmap(child_bitmap, child_schema)
            changed_fields.extend(
                f"{parent_field.name}.{field_name}" for field_name in child_fields
            )
    return changed_fields


def _format_commit_timestamp(timestamp) -> str:
    if isinstance(timestamp, int):
        return datetime.fromtimestamp(timestamp / 1000, tz=UTC).isoformat()
    return str(timestamp)


def _print_event(client: SalesforcePubSubClient, schema_cache: dict, event) -> None:
    schema_id = event.event.schema_id
    if schema_id not in schema_cache:
        schema_info = client.get_schema(schema_id)
        schema_cache[schema_id] = avro.schema.parse(schema_info.schema_json)

    schema = schema_cache[schema_id]
    payload = _decode_payload(schema, event.event.payload)
    header = payload["ChangeEventHeader"]
    changed_fields = _changed_field_names(schema, list(header["changedFields"]))

    print("CDC event received")
    print(f"Record IDs: {', '.join(header['recordIds'])}")
    print(f"Change type: {header['changeType']}")
    print(f"Changed fields: {', '.join(changed_fields) or '(none)'}")
    print(f"Commit timestamp: {_format_commit_timestamp(header['commitTimestamp'])}")
    print(f"Schema ID: {schema_id}")


def subscribe(event_limit: int = EVENT_LIMIT) -> None:
    request_queue: queue.Queue = queue.Queue()
    request_queue.put(
        pubsub_api_pb2.FetchRequest(
            topic_name=OPPORTUNITY_CHANGE_TOPIC,
            replay_preset=pubsub_api_pb2.LATEST,
            num_requested=event_limit,
        )
    )

    def request_stream() -> Iterator:
        while (request := request_queue.get()) is not None:
            yield request

    with SalesforcePubSubClient() as client:
        topic = client.get_topic()
        if not topic.can_subscribe:
            raise PermissionError(f"Cannot subscribe to {topic.topic_name}")

        responses = client.stub.Subscribe(request_stream(), metadata=client.metadata)
        schema_cache = {}
        received = 0

        print(f"Subscribed to {topic.topic_name}")
        print("Waiting for events. Edit the Opportunity StageName in Salesforce.")

        try:
            for response in responses:
                for event in response.events:
                    _print_event(client, schema_cache, event)
                    received += 1
                    if received >= event_limit:
                        return
        finally:
            request_queue.put(None)
            responses.cancel()


if __name__ == "__main__":
    try:
        subscribe()
    except KeyboardInterrupt:
        print("Subscription stopped")