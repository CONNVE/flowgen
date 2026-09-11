# NATS JetStream Publisher

Publishes events to a NATS JetStream subject.

## Configuration

```yaml
- nats_jetstream_publisher:
    name: publish_results
    credentials_path: /etc/nats/credentials.json
    url: "{{env.NATS_URL}}"
    subject: "results.processed"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | optional | Path to NATS credentials file. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `subject` | string | required | Subject to publish to. Supports templating. |
| `msg_id` | string | | NATS message ID for server-side deduplication. Can be a static string or templated from event data (e.g. `"{{event.data.record_id}}"`, `"fixed-key"`). Overrides `event.id` as the `Nats-Msg-Id` header. Requires `duplicate_window` on the stream to take effect. |
| `stream` | object | | Optional stream configuration (same as subscriber). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `stream` | string | JetStream stream name. |
| `sequence` | int | Message sequence number. |
| `duplicate` | bool | Whether the message was deduplicated. |

## Example

```yaml
- nats_jetstream_publisher:
    name: publish_orders
    credentials_path: /etc/nats/credentials.json
    subject: "orders.processed.{{event.data.region}}"
    stream:
      name: PROCESSED_ORDERS
      subjects:
        - "orders.processed.>"
      create_or_update: true
```
