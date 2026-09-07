import os

import certifi
import grpc

from generated import pubsub_api_pb2, pubsub_api_pb2_grpc
from src.salesforce_cdc.auth import get_access_token

PUBSUB_ENDPOINT = "api.pubsub.salesforce.com:7443"
OPPORTUNITY_CHANGE_TOPIC = "/data/OpportunityChangeEvent"


class SalesforcePubSubClient:
    def __init__(self) -> None:
        token_data = get_access_token()
        org_id = os.environ["SALESFORCE_ORG_ID"]

        with open(certifi.where(), "rb") as certificate_file:
            credentials = grpc.ssl_channel_credentials(certificate_file.read())

        self.channel = grpc.secure_channel(PUBSUB_ENDPOINT, credentials)
        self.stub = pubsub_api_pb2_grpc.PubSubStub(self.channel)
        self.metadata = (
            ("accesstoken", token_data["access_token"]),
            ("instanceurl", token_data["instance_url"]),
            ("tenantid", org_id),
        )

    def get_topic(self, topic_name: str = OPPORTUNITY_CHANGE_TOPIC):
        request = pubsub_api_pb2.TopicRequest(topic_name=topic_name)
        return self.stub.GetTopic(request, metadata=self.metadata, timeout=30)

    def get_schema(self, schema_id: str):
        request = pubsub_api_pb2.SchemaRequest(schema_id=schema_id)
        return self.stub.GetSchema(request, metadata=self.metadata, timeout=30)

    def close(self) -> None:
        self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()


def validate_connection() -> None:
    with SalesforcePubSubClient() as client:
        topic = client.get_topic()

    print("Pub/Sub connection established")
    print(f"Topic: {topic.topic_name}")
    print(f"Schema ID: {topic.schema_id}")
    print(f"Can subscribe: {topic.can_subscribe}")


if __name__ == "__main__":
    validate_connection()