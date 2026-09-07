# 02 - Proto and gRPC Stubs

## Why the Proto Is Required

Pub/Sub API uses gRPC. Its protobuf file defines the shared messages, enums,
and RPC methods. Generating stubs from an empty proto creates valid-looking
Python files with no useful service or message types.

The official contract comes from Salesforce:

```text
https://github.com/forcedotcom/pub-sub-api/blob/main/pubsub_api.proto
```

It is stored at `proto/pubsub_api.proto`. This phase verified that it is not
empty and contains `service PubSub`, `GetTopic`, `GetSchema`, `Subscribe`,
`FetchRequest`, and `FetchResponse`.

## Python Generation

```bash
./.venv/bin/python -m grpc_tools.protoc \
  -Iproto \
  --python_out=generated \
  --grpc_python_out=generated \
  proto/pubsub_api.proto
```

This produces:

```text
generated/pubsub_api_pb2.py
generated/pubsub_api_pb2_grpc.py
```

The first file contains messages, enums, and descriptors. The second contains
`PubSubStub` and generated gRPC service classes.

## Package Imports

The gRPC plugin emits `import pubsub_api_pb2` as a top-level import. Because
this project stores both files in the `generated` package,
`generated/__init__.py` registers the protobuf module under the expected name.
Generated files remain untouched and can be overwritten safely.

```bash
./.venv/bin/python -c \
  "from generated import pubsub_api_pb2, pubsub_api_pb2_grpc; print('OK')"
```

## Reproducible Checks

```bash
wc -c -l proto/pubsub_api.proto
grep -nE \
  'service PubSub|rpc GetTopic|rpc Subscribe|message FetchRequest|message FetchResponse' \
  proto/pubsub_api.proto
```

To verify the source byte for byte:

```bash
sha256sum proto/pubsub_api.proto
curl --fail --location --silent --show-error \
  https://raw.githubusercontent.com/forcedotcom/pub-sub-api/main/pubsub_api.proto \
  | sha256sum
```

The hashes should match.

## Maintenance Rule

When updating the proto:

1. fetch it again from the official repository;
2. review contract changes for compatibility;
3. regenerate both stubs with the pinned `grpcio-tools` version;
4. run the import test;
5. run `GetTopic` and `GetSchema` in a non-production environment;
6. never hand-edit serialized protobuf logic in `_pb2.py` files.

## Official References

- [Salesforce Pub/Sub API repository](https://github.com/forcedotcom/pub-sub-api)
- [Pub/Sub API documentation](https://developer.salesforce.com/docs/platform/pub-sub-api/overview)
- [gRPC Python generated code](https://grpc.io/docs/languages/python/generated-code/)