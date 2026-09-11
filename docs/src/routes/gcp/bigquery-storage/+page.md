# BigQuery Storage API

Read from and write to BigQuery tables using the Storage API. Higher throughput than the query API for large datasets.

## Storage Read

Reads table data directly via the BigQuery Storage Read API. Returns Arrow RecordBatch.

```yaml
- gcp_bigquery_storage_read:
    name: read_accounts
    credentials_path: /etc/gcp/service-account.json
    project_id: my-project
    dataset_id: salesforce
    table_id: accounts
    selected_fields:
      - id
      - name
      - industry
    row_restriction: "industry = 'Technology'"
```

### Read fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | | GCP service account credentials. Falls back to Application Default Credentials when omitted. |
| `project_id` | string | required | GCP project ID. |
| `job_project_id` | string | | GCP project ID for billing (if different). |
| `dataset_id` | string | required | BigQuery dataset. |
| `table_id` | string | required | BigQuery table. |
| `selected_fields` | list | | Columns to read (all if omitted). |
| `row_restriction` | string | | WHERE clause for filtering rows. |
| `sample_percentage` | float | | Random sampling percentage. |
| `compression_codec` | string | `unspecified` | Response compression: `unspecified` or `lz4`. |
| `snapshot_time` | string | | Time-travel query timestamp (RFC 3339). |
| `max_stream_count` | int | | Max parallel read streams. |
| `preferred_min_stream_count` | int | | Preferred minimum number of streams (optimization hint). |
| `data_format` | string | `arrow` | Result format: `arrow` or `avro`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Storage Write

Streams data into BigQuery tables via the Storage Write API. Accepts Arrow RecordBatch input.

```yaml
- gcp_bigquery_storage_write:
    name: write_accounts
    credentials_path: /etc/gcp/service-account.json
    project_id: my-project
    dataset_id: salesforce
    table_id: accounts
```

### Write fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | | GCP service account credentials. Falls back to Application Default Credentials when omitted. |
| `project_id` | string | required | GCP project ID. |
| `dataset_id` | string | required | BigQuery dataset. |
| `table_id` | string | required | BigQuery table. |
| `stream_name` | string | | Custom write stream name. Uses the default stream when omitted. |
| `trace_id` | string | | Trace ID for debugging and request tracking. Supports templating. |
| `change_type` | string | | CDC change type: `upsert` or `delete`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

### Read

| Format | Crate | Description |
|---|---|---|
| [Arrow RecordBatch](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) | [google-cloud-bigquery](https://github.com/googleapis/google-cloud-rust) | Table data via the BigQuery Storage Read API. Emits one event per batch. Empty tables emit an empty batch with the target schema. |

### Write

| Field | Type | Description |
|---|---|---|
| `rows_written` | int | Number of rows appended. |
