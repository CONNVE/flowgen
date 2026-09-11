# Credentials

Tasks that talk to external systems load credentials from a JSON file referenced by `credentials_path`. Different connectors use different formats — there is no single global schema — but the convention is uniform: a file path on disk, parsed at task initialisation, never logged.

## Where credentials_path appears

| Task family | Format | What it authenticates |
|---|---|---|
| `http_request`, `ai_completion` (`mcp_servers`) | `HttpCredentials` (bearer / basic / OAuth 2.0 client credentials) | The outgoing HTTP request header. |
| `http_endpoint` | `HttpCredentials` (bearer / basic) | Incoming requests, checked against the expected token. |
| `mcp_tool` | `Credentials` (`api_keys` list) | Incoming MCP calls, checked against the allowed keys. |
| `nats_jetstream_*`, `nats_kv_store` | NATS credentials JSON | NATS server connection. |
| `gcp_*` (BigQuery) | GCP service account JSON | Google Cloud APIs. |
| `salesforce_*` | Salesforce credentials JSON | Salesforce REST / Pub/Sub / Bulk APIs. |
| `mssql_query` | MSSQL credentials JSON | SQL Server connection. |
| `object_store` | Cloud-specific credentials JSON | S3, GCS, or Azure. |
| `git_sync` | Git credentials JSON (HTTPS token) | Git remote. |

The format details live on each task's documentation page. This page covers what is shared.

## Why files, not environment variables

Credentials live in JSON files for three reasons:

- **Kubernetes secrets** mount cleanly as files. `kubectl create secret generic ...` then mount at `/etc/flowgen/credentials/...` — no env-var indirection.
- **Multiple credentials per task** (e.g., bearer plus basic auth fallback) compose naturally as one JSON object.
- **No accidental logging.** Environment variables show up in process inspection, error reports, and log scrapes. File paths in config do not leak the secret content.

If you need to inject secrets at deploy time without writing them to disk, use a secrets-management sidecar (External Secrets, Vault Agent, Sealed Secrets) to materialise the JSON file at the configured path.

## HTTP credentials

The most common shared format is `HttpCredentials`, used by `http_request`, `http_endpoint`, and similar tasks:

```json
{
  "bearer_auth": "your-password"
}
```

Or basic auth:

```json
{
  "basic_auth": {
    "username": "your-username",
    "password": "your-password"
  }
}
```

Both fields are optional. If neither is set, no `Authorization` header is added.

### OAuth 2.0 client credentials

For APIs that require short-lived access tokens (e.g. Salesforce Marketing Cloud hosted MCP), use the `oauth2_client_credentials` field instead of a static bearer token. Flowgen fetches a token from the token endpoint using the [client credentials grant type](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4), caches it, and auto-refreshes when it expires:

```json
{
  "oauth2_client_credentials": {
    "token_url": "https://auth.example.com/oauth2/token",
    "client_id": "my-client-id",
    "client_secret": "my-client-secret",
    "scope": "api"
  }
}
```

The `scope` field is optional. When `oauth2_client_credentials` is present, it takes precedence over `bearer_auth` and `basic_auth`.

This applies to tasks that *send* authenticated requests — `http_request` and the `mcp_servers` entries on `ai_completion`. Tasks that *validate* incoming requests (`http_endpoint`) only read `bearer_auth` and `basic_auth`; an `oauth2_client_credentials` block there has no effect.

If the token endpoint omits `expires_in`, flowgen assumes a 5-minute lifetime and refreshes accordingly.

## Example — webhook with bearer auth

`/etc/flowgen/credentials/webhook.json`:

```json
{
  "bearer_auth": "your-password"
}
```

`/etc/flowgen/flows/secure-webhook.yaml`:

```yaml
flow:
  tasks:
    - http_endpoint:
        name: ingest
        endpoint: /events
        method: POST
        credentials_path: /etc/flowgen/credentials/webhook.json
```

Incoming requests must include `Authorization: Bearer your-password`. Anything else returns 401.

## Server-level fallback for HTTP

The HTTP server has a shared credentials path that webhooks can inherit:

```yaml
http_server:
  enabled: true
  credentials_path: /etc/flowgen/credentials/http.json
```

Individual `http_endpoint` tasks override the shared default by setting their own `credentials_path`. This is useful when most webhooks share the same shared-secret token but a few endpoints have stricter requirements.

## User-level authentication is separate

`credentials_path` authenticates the task itself (e.g., the bearer token for incoming webhook requests). User-level authentication — JWT, OIDC, session tokens — happens via the server's `auth` configuration. See [Authentication](/docs/flowgen/concepts/auth).

The two compose: a webhook can require a shared secret (via `credentials_path`) **and** a user-level JWT (via `auth.required: true`). Both checks must pass.

## Connection pooling

Tasks that share the same `credentials_path` automatically share the same authenticated client. The runtime hashes credential fields to detect duplicates — no configuration needed.

For example, if three flows each have a `salesforce_restapi_sobject` task pointing at the same credentials file, the first task to initialise performs the OAuth handshake. The other two wait and reuse the same session. This avoids hammering external services with redundant login calls and respects rate limits (e.g., Salesforce OAuth).

The key fields used for deduplication depend on the task family:

| Task family | Identity fields |
|---|---|
| `salesforce_*` | `credentials_path` |
| `gcp_*` (BigQuery) | `credentials_path` |
| `mssql_query` | `credentials_path`, `max_connections`, `connection_timeout`, `query_timeout` |
| `nats_jetstream_*`, `nats_kv_store` | `credentials_path`, `url` |
| `object_store_*` | `path`, `credentials_path` |

Tasks with different identity fields get separate clients. Two MSSQL tasks with the same credentials but different `max_connections` do not share a pool.

## Operational notes

- **File permissions** should be `0600` (read-write for the flowgen user only). Flowgen does not enforce this — the operator is responsible.
- **Hot reload** is not supported. Credentials are read once at task initialisation. To rotate, restart the worker. OAuth 2.0 access tokens are an exception — they auto-refresh transparently when they expire.
- **Errors at startup**: if the file is missing or malformed, the task fails to initialise and the standard retry circuit breaker fires. After ~15 minutes the task gives up. Watch worker logs for `Failed to read credentials` or `Failed to parse credentials` messages.
- **Never commit credential files to Git.** Pair with `git_sync` carefully: sync flow YAML, but mount credentials separately via a Kubernetes secret.
