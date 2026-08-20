# NATS JetStream Subscriber

Consumes messages from a NATS JetStream stream. Source task — typically first in a flow.

## Configuration

```yaml
- nats_jetstream_subscriber:
    name: order_events
    credentials_path: /etc/nats/credentials.json
    url: "{{env.NATS_URL}}"
    subject: "orders.>"
    durable_name: order_processor
    stream:
      name: ORDERS
      subjects:
        - "orders.>"
      create_or_update: true
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | optional | Path to NATS credentials file. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `subject` | string | required | Subject to subscribe to (supports wildcards). |
| `durable_name` | string | | Durable consumer name for persistent subscriptions. |
| `stream` | object | | Stream configuration (see below). |
| `max_messages` | int | | Max messages per batch fetch. |
| `max_ack_pending` | int | | Max unacknowledged messages. |
| `max_deliver` | int | | Max delivery attempts before discarding. |
| `delay` | duration | | Delay between fetch requests. |
| `throttle` | duration | | Delay between individual messages. |
| `ack_timeout` | duration | wait indefinitely | Max time to wait for flow completion before letting JetStream redeliver. Also applied to the JetStream context timeout and the consumer `ack_wait`. |
| `backoff` | list | | Redelivery backoff schedule (list of durations). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Stream options

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Stream name. |
| `subjects` | list | required | Subject patterns (supports wildcards). |
| `create_or_update` | bool | false | Create or update stream if it does not match. |
| `max_age` | duration | | Max message age. |
| `max_messages` | int | | Max messages in stream. |
| `max_bytes` | int | | Max stream size in bytes. |
| `retention` | string | `limits` | Retention policy: `limits`, `interest`, `work_queue`. |
| `discard` | string | `old` | Discard policy: `old`, `new`. |
| `discard_new_per_subject` | bool | false | When `true`, rejects new messages if `max_messages_per_subject` limit is reached for that subject. Requires `discard: new`. |
| `duplicate_window` | duration | | Deduplication window. |

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) / [Avro](https://docs.rs/apache-avro/latest/apache_avro/) / [Arrow](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) | [async-nats](https://docs.rs/async-nats/latest/async_nats/) | Message payload from JetStream. Format depends on how the message was published. |

## Example

```yaml
flow:
  tasks:
    - nats_jetstream_subscriber:
        name: orders
        credentials_path: /etc/nats/credentials.json
        subject: "orders.created"
        durable_name: order_processor
        max_ack_pending: 100
        stream:
          name: ORDERS
          subjects:
            - "orders.>"
          create_or_update: true
          retention: work_queue

    - script:
        name: transform
        code: |
          let order = event.data;
          #{id: order.id, total: order.amount}

    - log:
        name: output
```
