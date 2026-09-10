# Changelog

## 0.135.0

### Breaking

- **`cache.db_name` is replaced by `cache.runtime.db_name`, and leases now
  live in their own bucket.** The cache config declares two buckets on one
  connection: `runtime` (flow state, reachable from scripts via `ctx.cache`)
  and `system` (leader-election leases and peer registration, deliberately
  out of reach). Both are optional and default to `flowgen_cache` /
  `flowgen_system`.

  ```yaml
  cache:
    enabled: true
    url: nats.example.svc.cluster.local:4222
    type: nats
    runtime:
      db_name: flowgen_cache
    system:
      db_name: flowgen_system
  ```

  Rename `cache.db_name` to `cache.runtime.db_name` if you set it. The old
  key is no longer read, and a config that still carries it is rejected at
  startup with the replacement spelled out — serde ignores unknown keys, so
  accepting it silently would have moved the deployment onto the default
  bucket and left the existing data behind. The system bucket is now created whenever the cache
  is enabled — previously it only appeared as a side effect of loading flows
  from the cache, so deployments serving flows from disk kept their leases
  and peer keys in the runtime bucket, where any flow's script could read or
  overwrite them. Those deployments move to the system bucket on restart:
  the stale runtime leases expire after roughly a minute and flows resume,
  so expect a brief pause on first start rather than a manual migration.

  Leader election with the cache disabled now warns at startup — leases are
  per-pod in memory there, so every pod elects itself and runs the same
  flows concurrently.
- **`object_store` tasks now take TLS root certificates from the host's
  system trust store.** The `object_store` upgrade to 0.14 removed the
  `tls-webpki-roots` feature flowgen was building with, so S3, GCS, and
  Azure connections validate against the host's CA bundle instead of a
  copy of the webpki roots compiled into the binary. The published
  flowgen image is unaffected — its `distroless/cc-debian12` base ships
  a CA bundle. If you run the binary somewhere that doesn't have one (a
  `scratch` image, a stripped-down host), install `ca-certificates`
  before upgrading or `object_store` connections will fail certificate
  verification.
- **`nats_jetstream_subscriber.max_messages` is renamed to
  `max_messages_per_batch`, and `delay` is deprecated.** The new name
  matches the underlying NATS option. The subscriber now uses a
  continuous pull stream instead of looping over `fetch()` calls; the
  old `fetch()` loop returned immediately on an empty queue and could
  spin at hundreds of requests per second. The stream keeps each batch
  request open with a server-side expiry, so an idle consumer waits on
  NATS instead of polling. The `delay` field no longer has any effect —
  use `throttle` to rate-limit individual messages. Dropping `fetch()` also
  removes the subscriber's `Error::ConsumerBatch` variant, which nothing
  can return now that no code path fetches a batch; anything matching on it
  by name needs that arm deleted.

### Features

- **HTTP credentials now support the OAuth 2.0 client credentials flow.**
  A credentials file can carry an `oauth2_client_credentials` block
  (`token_url`, `client_id`, `client_secret`, optional `scope`) instead
  of a static `bearer_auth` token. Flowgen fetches an access token from
  the token endpoint, caches it, and refreshes it before it expires, so
  APIs that only issue short-lived tokens no longer need a restart to
  pick up a new one. Used by `http_request` and by the `mcp_servers`
  entries on `ai_completion`; when present it takes precedence over
  `bearer_auth` and `basic_auth`.
- **`bigquery_storage_write` now warns about fields the table does not have.**
  The encoder walks the table's schema, so a field in the data with no
  matching column was dropped without a trace — a column added to the
  pipeline but not to the table simply stopped arriving. The task now logs
  the field names once per event. Event metadata belongs in `event.meta`, so
  anything in `event.data` is expected to match the table.
- **`gcp_bigquery_query` parameters can now be loaded from a resource file.**
  A parameter written as `{ resource: "schema.json" }` is read from the
  resource path and bound as its parsed contents — arrays and objects arrive
  as BigQuery's `JSON` type, so SQL walks them with `JSON_QUERY_ARRAY`. This
  lets a query work from a schema file the flow already keeps on disk instead
  of restating the same column list inside the SQL. See
  `examples/gcp/bigquery_query_schema_parameter.yaml`, which adds only the
  columns a table is missing by diffing against `INFORMATION_SCHEMA` —
  relevant because BigQuery counts every `ALTER TABLE` against a limit of
  1500 table metadata updates per table per day, including ones that change
  nothing. Only a lone `resource` key marks a file reference: an inline
  object that happens to carry a `resource` field alongside others is still
  bound as data, rather than having its remaining fields dropped.
- **`salesforce_restapi_composite` can auto-populate each record's
  `attributes`.** The Composite API requires every record to carry
  `attributes: { type: "..." }`. Setting the task's `sobject_type` now
  fills that in for any record that doesn't already have one, so
  single-type batches no longer need the boilerplate on every record.
  Records that already specify `attributes` are left untouched, so
  mixed-type batches keep working. Without `sobject_type`, a record
  missing `attributes` now fails with an error naming the record's
  index instead of a generic API rejection.
- **NATS JetStream publisher can now set a custom `Nats-Msg-Id` for
  server-side deduplication.** The new `msg_id` field accepts a
  templated string (e.g. `{{event.data.id}}`), overriding the previous
  hardcoded fallback to `event.id`. Combined with a stream's
  `duplicate_window`, republishing the same logical record no longer
  stores a second copy — the server acks it as a duplicate instead of
  erroring.
- **JetStream streams can now hard-reject duplicate messages on a
  subject.** The new `discard_new_per_subject` option, used together
  with `discard: new` and `max_messages_per_subject`, makes the server
  refuse a second publish to a subject that's already at its cap,
  rather than silently evicting the first message to make room.

### Fixes

- **A failing BigQuery, MSSQL, or MongoDB task exposed over MCP returned
  an empty success instead of the error.** These tasks reported a
  retry-exhausted failure by sending an error event downstream, which
  works mid-flow but does nothing when the task is the flow's last one —
  there is no downstream to send to. The waiting `mcp_tool` source saw a
  closed channel rather than a failure, so MCP clients (Claude Code,
  opencode) got a result with no error set and no indication anything had
  gone wrong. `bigquery_query`, `bigquery_job`, `bigquery_storage_read`,
  `bigquery_storage_write`, `mssql_query`, and `mongodb_collection` now
  signal the error back to the source, matching what the other 21 tasks
  already did.
- **`bigquery_storage_write`'s `trace_id` field was accepted in config
  but never sent anywhere.** It's now passed through to the BigQuery
  Storage Write API's `AppendRowsRequest`, so request tracing actually
  works as documented.
- **Tasks tracking their position could skip data when a pod shut down
  mid-batch.** `git_sync`, `oci_sync`, `generate`, and the Salesforce
  Pub/Sub subscriber persist a cursor (last commit, manifest digest,
  timestamp, replay ID) after a batch completes. On shutdown that write
  could still land even though the batch had been cut short, so the next
  start resumed past events that were never emitted. All four now skip
  the cursor write once cancellation is signalled, re-emitting on the
  next start rather than silently dropping.
- **`mongodb_collection` dropped upstream event metadata.** The task
  emitted its events outside the event-context scope, so `meta` set by
  earlier tasks in the flow never reached anything downstream of a
  Mongo read or write.
- **`oci_sync` binary layers discarded upstream metadata.** Layers that
  aren't valid UTF-8 attach their own path and digest metadata, which
  replaced the incoming event's `meta` wholesale instead of merging into
  it.
- **Salesforce Pub/Sub tasks opened one gRPC connection each.** Every
  subscriber and publisher built its own channel to the Pub/Sub endpoint,
  so a worker running several of them against one org held that many TLS
  connections open, each with its own HTTP/2 keepalive ticking whether or
  not events were flowing. Tasks sharing an endpoint now share one channel
  through the client registry and multiplex over it — a subscriber and
  publisher on the same org included. A reconnect evicts the channel
  alongside the client, so a dropped connection is still rebuilt rather
  than reused.
- **`salesforce_pubsubapi_publisher` never recovered from a dropped
  connection.** It built its gRPC channel once at startup and had no way to
  replace it: a connection lost to anything other than an auth error (a
  reset, a server-side restart) left the task publishing into a dead
  transport, failing every event until the pod was restarted. It now
  rebuilds the connection before the next event whenever a publish
  exhausts its retries, the way the subscriber already reconnects on an
  event-loop failure.
- **`salesforce_pubsubapi_publisher` ignored its `endpoint` setting.** The
  field was accepted in config but the publisher always connected to the
  default Pub/Sub host, so pointing one at a regional endpoint (e.g. the
  EU instance) silently did nothing. It now honours `endpoint` the way the
  subscriber already did.
- **A standby pod could stop being able to take over a flow.** The retry
  loop waiting for a held lease used an uncapped exponential backoff that
  was never reset, so a pod that had been waiting doubled its interval
  indefinitely — after a day it re-checked roughly once every 36 hours, and
  a leader dying at that point went unnoticed for just as long. Waiting for
  a healthy holder is now a flat poll at the renewal interval; backoff
  still applies to genuine errors, and resets once the lease is merely
  held by someone else.
- **Leader election cost more idle CPU than it needed to.** Renewing a
  lease read it back first, purely to carry its generation counter forward,
  making every renewal two round-trips to the cache. The holder already
  knows the generation from when it acquired the lease, so it now passes it
  through and renewal is a single write. On a worker with several
  leader-elected flows this halves the steady background traffic, which on
  an otherwise idle pod is most of what it was doing. The revision check
  that guards against split-brain is unchanged.
- **A lease deleted out from under its holder now reports as such.** The
  renewal path returned a bare `NotFound` error instead of the
  `LeaseDeleted` result it already had a variant for, because the removed
  read hit the missing key first. Renewal now surfaces `LeaseDeleted` and
  the flow re-arms acquisition.
- **The peer registry logged a "Registered peer" line on every renewal.**
  The renewal timer fired immediately on start rather than after the
  interval, and each tick re-logged at INFO. Renewals now log at DEBUG
  and the first one waits a full interval.
- **`nats_jetstream_subscriber` no longer spins on an empty queue.**
  Switching from a `fetch()` loop to a continuous pull stream with a
  server-side batch expiry (default `30s`) eliminates the empty
  request/reply storm that consumed CPU and NATS round-trips when no
  messages were available.

- **The Helm chart's liveness probe restarted healthy pods under load.** It
  ran with Kubernetes' default `timeoutSeconds` of 1, so a worker busy
  enough to delay the health handler by a second was killed after 45s —
  losing in-flight work and shifting its load onto replicas that then timed
  out in turn. Liveness now allows 5s per check and 75s of unresponsiveness
  before restarting, and a new `startupProbe` covers slow starts so neither
  of the other probes needs an `initialDelaySeconds`. Readiness stays
  sensitive, since removing a pod from the load balancer is reversible and
  a restart is not. All three are configurable under
  `flowgen.health.probes.{startup,readiness,liveness}`.

### Technical improvements

- Integration test coverage for the OAuth 2.0 token flow (fetch, cache,
  refresh, error paths, and concurrent callers collapsing into a single
  token request) and for a DAG fan-out where two branches re-join the
  same parent, one implicitly and one through `depends_on`.
- The NATS JetStream integration tests now share publisher-spawn and
  stream-inspection helpers, and each test starts its own container so
  streams can't collide between tests.
- Integration coverage for the subscriber's idle behaviour: one test reads
  the NATS server's own `in_msgs` counter to assert an idle consumer is
  waiting on a parked pull request rather than polling, and a second checks
  that a message published after the consumer went idle still arrives on
  that already-open request. The first fails against the old `fetch()` loop,
  so the regression that motivated the switch stays covered.

## 0.134.0

### Features

- **Optional admin UI login via OIDC.** The admin web UI can now be
  gated behind sign-in through any standard-compliant identity provider
  (Okta, Zitadel, Auth0, or one that itself federates to an upstream
  IdP), configured under the new `web.auth`/`web.cookie_secret`/
  `web.cookie_secure` options. There's no server-side session store —
  the browser's cookie holds the encrypted identity-provider tokens
  directly, so a restart never invalidates existing sessions. Omit
  `web.auth` and the admin UI stays unauthenticated, as before.

### Fixes

- **The Agents chat's selected model reset every time you left and came
  back to a conversation.** It's now saved per conversation and
  restored on load.
- **Tool calls a chat agent made mid-turn disappeared once you reloaded
  the page**, even though they rendered fine while streaming. They're
  now persisted alongside the message and survive a reload.

## 0.133.0

### Fixes

- **NATS JetStream subscriber could go silently stale after a pod
  restart.** A dead connection can surface as a clean empty fetch batch
  rather than an error, so the receive loop just kept polling forever
  without ever re-entering the reconnect path — no error, no log, no
  events. The subscriber now checks the underlying connection's live
  state when a fetch comes back empty and forces a reconnect if it's
  disconnected.
- **Salesforce Pub/Sub subscriber could keep reusing a dead cached client
  across reconnects.** The shared client is now evicted from the client
  registry after a connection failure, so the next reconnect attempt
  builds a fresh one instead of retrying against the same broken
  connection — matching the NATS subscriber's existing behavior.
- Reconnect-backoff logging for both subscribers is now visible at the
  default production log level instead of `debug`, and a
  "reconnected successfully" line is emitted once a prior failure
  actually recovers.
- **Leader-elected flows could all pile onto one pod instead of spreading
  out.** Pods now hash-distribute flow ownership hints across themselves
  before racing for a lease, so flows land roughly evenly instead of every
  pod defaulting to whichever wins the race first. Falls back to normal
  racing if the preferred pod doesn't show up.
- **`mongodb_collection` reads matching zero documents left the flow
  hanging.** The source task waited indefinitely for a completion signal
  that never came, since an empty result set emitted no event at all. A
  read that matches nothing now emits one event and completes normally.
- **Two task types with matching credentials could collide in the shared
  client registry**, causing the second one to fail with a "client
  registry type mismatch" error. Registry keys now include the task
  type, so different task types never share an entry.

### Improvements

- **MongoDB connections are now shared through the same client registry
  NATS and Salesforce already use.** Tasks with identical credentials
  reuse one client instead of each opening an independent connection
  pool to the same deployment.
- `mongodb_change_stream` and `mongodb_collection` config fields
  (`credentials_path`, `db_name`, `collection_name`, `filter`) now
  support template rendering, matching other task types.

## 0.132.0

### Features

- **MongoDB support.** `mongodb_collection` reads or writes documents in a
  collection (`operation: read` with a filter, or `operation: write` of
  the incoming event); `mongodb_change_stream` watches a database for
  real-time document changes. Credentials' `host` field accepts either a
  single host or an array, for connecting to a self-hosted replica set
  without a DNS seedlist.

### Fixes

- **`bigquery_query` now fails fast on permanent errors** (bad SQL, missing
  table, permission denied) instead of retrying 10 times over ~15 minutes.
  5xx and rate-limit (429) responses still retry as before.

### Improvements

- **`mcp_resource` and `mcp_prompt` can scope which callers can see and use
  them**, the same way `mcp_tool` already could. Both gained a `headers`
  field: a map of HTTP headers a request must carry, with matching values,
  to be listed or read/fetched at all. An unauthorized caller gets the same
  "unknown resource"/"unknown prompt" error as a nonexistent one.
- **Calls rejected by `headers` scoping now show up in that flow's Activity
  panel** (`mcp_tool`, `mcp_resource`, `mcp_prompt`, `llm_proxy`), not just
  the global log.

## 0.131.0

### Fixes

- **Per-flow Activity panel showed "No events yet" even with real activity.**
  The panel's event list now comes from the same `/api/logs` history +
  `/api/logs/stream` live tail the global `/logs` viewer uses (scoped to the
  flow), instead of a metrics-only SSE stream with no history replay. A flow
  that already finished its startup burst now shows its history immediately
  on open instead of staying empty until the next live event.
- **Per-flow SSE connections could pile up and starve other requests.** Only
  one per-flow event subscription is kept open at a time; switching flows,
  closing the modal, or navigating away now closes the previous connection
  instead of leaving it open for the rest of the session (browsers cap
  concurrent connections per origin on HTTP/1.1).
- **Flow graph nodes could overlap on a flow's second open.** The DAG
  layout's re-layout guard didn't reset when node measurement restarted on
  a subsequent open, so nodes could be left at their pre-layout stacked
  positions instead of dagre's computed layout.
- **DAG node status badge could overflow into neighboring nodes.** The
  duration badge had no maximum width, so a wide duration string could push
  past the node's reserved space. The node, its status column, and the
  badge are now all fixed-width end to end.
- Raising the `/logs` line limit while lowering the level filter to remove
  a stale UI state where the trim used the previous limit instead of the
  newly applied one.
- **Agents chat model picker stayed open after selecting a model.** Clicking
  a model kept focus inside the dropdown, so the CSS-only `:focus-within`
  panel never saw focus leave and closed itself.
- **Log attributes like token counts and latency showed as one joined JSON
  string instead of separate rows.** `tracing` requires field names to be
  static per callsite, so multiple `.context(key, value)` calls on one log
  line are joined server-side into a single `context` field; the admin UI
  now unpacks it back into individual attributes, so e.g. `prompt_tokens`,
  `completion_tokens`, and `latency_ms` each show as their own row again.

### Improvements

- **`llm_proxy` and `mcp_tool` can scope which callers can see and use
  them.** Both gained a `headers` field: a map of HTTP headers a request
  must carry, with matching values, to be listed or invoked at all — an
  agent meant to use only a specific database tool can now be kept from
  seeing or calling unrelated tools. This replaces
  `llm_proxy`'s old `clients` list, which only hid a proxy from a client's
  model list but never blocked the request itself; `headers` blocks both.
  `ai_completion` (`headers`) and its `mcp_servers` entries (`headers`) can
  now send arbitrary headers to identify themselves to a scoped downstream
  proxy or MCP server. The admin server's own outbound requests (the
  built-in Agents chat) now source their identifying header from
  `web.headers` in config instead of a hardcoded value.

- **Per-flow Activity panel gains a history limit control and Debug/Trace
  chips**, matching the global `/logs` viewer. The limit is independent per
  view (raising it in the panel doesn't affect `/logs`); Debug/Trace default
  off everywhere, Info/Warning/Error default on in the panel (you're
  inspecting one flow's behavior) versus Warning/Error only on `/logs`
  (system-wide error scanning). Level colors and labels are now a single
  shared implementation so the two views can't drift.
- **Observability backend split into two traits**, mirroring the existing
  cache trait pattern: `LogsStore` (event/log history and live tail) and
  `MetricsStore` (per-flow counters and status, plus OTLP emission). The ad
  hoc `FlowRegistry` type is gone; both traits have one in-memory
  implementation today and are structured so a vendor-backed implementation
  can be added later without touching the admin API or UI.

## 0.130.0

### Features

- **Agents chat in the admin UI.** A new "Agents" tab (the first nav
  item) hosts a built-in chat that talks to the AI gateway through the
  admin server, so the browser stays same-origin and the gateway needs
  no CORS. Messages stream token-by-token over SSE; assistant replies
  render Markdown, code (Prism-highlighted), and inline HTML using the
  same palette as the resource viewer. The model selector is populated
  from the gateway's `/models` list. Each message has a copy button and
  a hover timestamp. An "agent" is just a flow with an `llm_proxy`
  endpoint — there is no separate agent registry.

- **Conversation history for the Agents chat.** Conversations are
  persisted through the admin API (`GET/PUT/DELETE
  /api/agents/conversations[/{id}]`) into the configured **system**
  cache, which is out of user-script reach — one tenant's flow script
  cannot read another's chats. Each conversation lives under a
  `/agents/{id}` URL so it deep-links and survives a reload. History is
  written as soon as a message is sent (not only after the reply) and
  the TTL is refreshed on every write. Retention defaults to 30 days,
  configurable via `web.agents.conversation_history_ttl`; long-term
  history belongs in a database via a persistence flow. When no system
  cache bucket is configured (single-binary / in-memory), history falls
  back to the runtime cache with a warning, since that mode has no
  tenant isolation to protect.

- **`llm_proxy` advertises its models.** The `llm_proxy` config gains a
  `models` list that the gateway returns from `/models` as full
  `<proxy>/<model>` ids, so clients (including the built-in chat) can
  populate a model selector without knowing the routing prefix. Routing
  itself still lives in the proxy's script — declaring a model does not
  decide where it goes. `models` is a discovery menu, not a whitelist:
  an unlisted model is still forwarded for the script/provider to handle.

- **Proxies can hide the model and auto-route.** A proxy that declares no
  `models` is advertised by its bare name and accepts a bare id (no
  `<model>`): the client picks nothing and the routing script chooses the
  downstream model. A proxy that *does* list models requires the client
  to pick one — a bare id to it is rejected. Auto-routed requests carry
  no `requested_model` in `event.meta` (absent, not a sentinel), so they
  are filtered by that field's absence.

- **Agents chat shows the tools an agent ran.** When a server-side agent
  (`ai_completion` with `tool_passthrough: false`) invokes a tool mid-turn,
  the gateway now streams it as a standard OpenAI `delta.tool_calls` frame
  (informational — no `tool_calls` finish reason, so tool-loop clients
  ignore it). The built-in chat renders these as a collapsible "tool calls"
  section in the reply, separated from the text, each row expandable to its
  arguments. This also fixes reply text that used to run together across a
  tool call (the pre-tool and post-tool text segments are now distinct).

### Fixes

- **Tool-using agents no longer fail every tool call.** `ai_completion`'s
  `max_turns` is now a required setting with a non-zero default (10) instead
  of an `Option` that left rig's turn budget at 0. With a 0 budget the agent
  could make one model call but had no turn to act on a tool result, so any
  tool invocation died with `MaxTurnError` and tore down the in-flight MCP
  connection as "Transport closed" — the marketing-assistant demo agent
  could never actually run its BigQuery tool.

- **Agents chat: stop button and a live status line.** A generation can be
  stopped mid-stream with the composer's Stop button or Esc (aborts the
  streaming request, keeps what arrived). While a turn is in progress the
  reply shows a shimmering brand-gradient status line (`✱ Working…` /
  `✱ <tool>…`) instead of a text cursor; tool calls render as expandable
  status rows (arguments shown only when expanded).

- **Agents chat model selector no longer collapses a proxy's models into
  identical rows.** `display_name`/`description` are proxy-level labels the
  gateway copies onto every model entry, so a proxy exposing two models
  rendered two indistinguishable rows and the model names were lost. The
  selector now groups models by proxy — the name and description (wrapped
  to two lines) show once, with the proxy's models listed beneath as
  pickable rows — and falls back to the bare proxy name when no
  `display_name` label is set. It also hides proxies whose `protocol` the
  built-in chat can't drive (it speaks OpenAI; Anthropic proxies are
  skipped), and `/models` entries now carry a `protocol` field.

- **`clients` visibility filter is exclusive.** A proxy's `clients` list
  now scopes it: a named client (the built-in chat sends
  `X-Flowgen-Client: flowgen-ui`) sees only proxies that list it, and an
  anonymous request sees only proxies with an empty `clients`. Previously
  an empty `clients` meant "visible to everyone", so every public proxy
  leaked into the built-in chat. A proxy must now list `flowgen-ui` in
  `clients` to appear in the chat.

- **Log viewer sorts newest-first and trims correctly.** Records arrive
  out of order — the snapshot flattens per-flow ring buffers in hash-map
  order and the live tail interleaves retries from many flows — so rows
  displayed in arrival order jumped around by timestamp and new records
  could land buried mid-list. The `/logs` viewer now sorts the buffer by
  timestamp (newest at the top, live tail pinned there), and the line
  limit evicts the genuinely oldest records instead of dropping by insert
  order.

- **Gateway no longer duplicated streamed replies.** When a downstream
  provider streamed token deltas, the final completion event re-emitted
  the whole text, so responses appeared twice. The gateway now suppresses
  the final full-text emission once progress deltas have streamed.

- **Consistent icon buttons, tooltips, and brand green across the admin
  UI.** Toolbar actions (Live/Pause, Clear, Resume) are now icon-only
  buttons with styled tooltips instead of icon+text, every icon-only
  control has a daisyUI tooltip and `aria-label` (replacing bare
  `title=` attributes), icon sizes are unified, and the dark-mode
  primary green is tuned to match the brand.

- **Empty and error states share one friendly design.** Every full-panel
  empty or failed-to-load state (no flows, no resources, no models, a
  service that didn't respond) now renders as a centered message with a
  soft brand glow instead of a raw red or amber alert. The Agents chat no
  longer flashes the greeting before switching to "no models".

- **Flow graph no longer overlaps edges on fan-in.** The DAG lays out
  from each node's real rendered size, so branches that merge back into
  one task (for example a diff flow's put/delete branches) route cleanly
  instead of collapsing onto a single line.

- **Long folder names truncate in the flows sidebar.** A deeply nested or
  verbose folder name is clipped with an ellipsis and its full name shown
  on hover, instead of overflowing the sidebar.

- **Esc clears the Agents chat composer.** Pressing Esc with a drafted
  message clears it; while a reply is streaming, Esc still stops
  generation.

- **Flow status label reads `Ok`.** The `ok` status pill and filter now
  read `Ok`, matching the sentence case of `Warn`, `Error`, and `Info`.

### Internal

- Conversation request/response types are defined in `openapi.yaml` and
  generated for both the Rust handlers (`flowgen_client::types`) and the
  web client (`generated.ts`), keeping the two in lockstep.

## 0.129.0

### Breaking

- **Default cache key prefixes dropped the `flowgen.` segment.** The
  default flow and resource cache prefixes changed from `flowgen.flows`
  and `flowgen.resources` to `flows` and `resources` — the bucket
  (`flowgen_system`) already namespaces by `flowgen`, so the prefix no
  longer repeats it. Deployments that relied on the defaults will find
  their existing `flowgen.flows.*` / `flowgen.resources.*` cache keys
  orphaned after upgrade: re-run the workspace sync (or re-push the
  artifact) so the bootstrap flow rewrites the keys under the new prefix
  and deletes the stale ones. Deployments that set `flows.cache.prefix`
  and `resources.cache.prefix` explicitly are unaffected.

### Fixes

- **Flows in nested folders no longer fail on leader election and
  stateful sources.** After 0.128 made the file path the flow identity,
  that slash-bearing value flowed straight into cache and lease keys,
  where a path separator is not a valid key character. Any flow in a
  subfolder that used leader election, or a stateful source
  (`salesforce_pubsubapi_subscriber` replay ids, `generate` last-run and
  counter state, `git`/`oci` sync digests, or Rhai `ctx.cache`), errored
  as soon as it tried to read or write its key. Identities are now
  projected to a key-safe form (base64url) at every cache, lease, and
  peer-ownership boundary, so nested-folder flows work regardless of
  depth. Human-facing surfaces — the tracing `flow=` field, activity
  keys, the admin API `path`, and resource URIs — keep the readable
  slash form.

- **A duplicate flow identity now fails fast at startup.** If two flows
  resolve to the same path identity (for example under an unusual
  symlinked config mount), the process exits with a clear error instead
  of letting the two silently share a registry entry, cache namespace,
  and lease key.

- **`oci_sync` and `git_sync` tasks now report activity.** Both process
  each event in a spawned task that did not carry the tracing span, so
  their events — including the "digest unchanged, skipping" log — were
  emitted without a `flow` or `task` field. The admin UI therefore showed
  no activity on these nodes and the logs were unattributed. The spawned
  task now inherits the task span, so both surface per-event activity like
  every other task.

- **Flow identity is stable across ConfigMap reloads.** Flows loaded from
  a directory now ignore hidden (dot-prefixed) path segments during
  discovery. A ConfigMap volume presents every file three ways — the
  clean top-level name, a `..data` symlink, and a timestamped `..<ts>`
  directory that is replaced on every reload — so the recursive scan
  matched each flow three times and could derive its identity from the
  timestamped path, minting a new identity (and a new lease and cache
  namespace) on every reload and forcing a full re-sync and re-election.
  Only the clean top-level entry is now considered, so identity stays
  constant across reloads. This follows the POSIX hidden-file convention
  and needs no knowledge of the mount's internal layout.

### Internal

- **`FlowIdentity` type carries both projections.** A single
  `flowgen_core::identity::FlowIdentity` owns the identity: it `Display`s
  as the human path (so log and lease-manager fields stay readable) and
  projects to the key-safe id via `as_key()`. The task manager, lease
  keys, and peer ownership take the typed identity rather than a bare
  string, so the two forms cannot drift apart.

- **`flow.name` no longer participates in identity.** Identity always
  comes from the path; `flow.name` is a pure display-name override.
  Configs that still declare `name` continue to parse — it is simply
  ignored for identity.

## 0.128.0

### Breaking

- **Flow identity is the file path, not `flow.name`.** A flow's identity
  is now the file path relative to `flows.path` with the extension
  stripped (e.g. `flows/demo/salesforce/reader.yaml` → identity
  `demo/salesforce/reader`). This value drives the registry key, cache
  namespaces, MCP URIs, activity keys, and the tracing `flow=` field.
  Two flows with the same basename in different folders no longer
  collide. The YAML `flow.name` field is now optional and only
  meaningful when a flow is constructed programmatically (e.g. via the
  admin API) without a filesystem path. `FlowConfig` validation rejects
  configs without either a `source_path` or a `name`. Rhai `ctx.cache`
  keys owned by flows loaded from a nested path change from
  `<basename>.<key>` to `<full/path>.<key>` — cached state under old
  keys becomes orphaned on upgrade.

- **`log` task: `structured` field removed; body is always pretty JSON.**
  The `structured` boolean is gone. The log line body is now always the
  pretty-printed JSON of `event.data`, and the event `id` / `subject`
  are emitted as structured tracing fields (`event.id`,
  `event.subject`). A leftover `structured: true` in YAML is silently
  ignored. Use `include_meta: true` to fold `event.meta` into the body.

- **`FlowStatus` API enum renamed.** The admin API `status` value on
  `/api/flows` changes `running` → `ok` and `warning` → `warn`
  (`idle` and `error` unchanged). External consumers of the flow list
  that match on these strings must update.

- **JWT HMAC secret is redacted when config is serialized.** The
  `auth.secret` field (MCP server / AI gateway JWT config) is now a
  `secrecy::SecretString`: it never appears in `Debug` output or logs,
  and serializes as `"***"`. The system config viewer therefore shows
  `secret: "***"` instead of the plaintext key. Token validation is
  unchanged. Anything that serialized this config and read the value
  back out would now get `"***"` — the runtime never round-trips config
  from its serialized form, so this only affects the read-only viewer.

### Features

- **System console at `/system`.** A new read-only admin view (reached
  from the sidebar footer) renders the running `AppConfig` as YAML so
  operators can inspect cache, telemetry, server, and flow settings
  without shelling into the pod. Secrets are redacted (see above).
  Backed by `GET /api/config`.

- **Global log viewer at `/logs`.** A new admin UI tab streams every
  retained log record (framework, lifecycle, and per-task activity) with
  level filters, free-text search, live tail with pause/resume, a
  detail drawer, and an adjustable line limit (default 1000, up to
  10000). Backed by `GET /api/logs` (snapshot, `?limit=` capped at
  10000) and `GET /api/logs/stream` (SSE). The per-flow activity feed
  (`/api/flows/stream`) is a client-filtered subset of the same source;
  both now ship the raw `LogRecord` shape and classify client-side.

- **Three-way theme toggle.** The admin UI theme control is now
  System / Light / Dark. `System` tracks the OS `prefers-color-scheme`
  and reacts to it changing at runtime; the choice persists under the
  `flowgen-theme` localStorage key (values `system` | `light` | `dark`,
  replacing the previous boolean).

- **Version badge links to release notes.** The version badge in the
  sidebar footer links to the matching GitHub release
  (`github.com/connve/flowgen/releases/tag/v<version>`).

- **Activity events surface task-scope failures.** The admin UI's per-
  flow activity panel now shows warnings and errors emitted from any
  span inside `task.run` or `task.handle`, not just successful event
  handling. Subscriber init failures ("Subscriber initialization
  failed", DNS errors, permission denied) that used to only bump the
  error counter without appearing in the feed are now visible with
  their full context. Framework logs outside task scope (leader
  election, HTTP server startup, cache reconnects) remain excluded.

### Fixes

- **Admin UI folder sidebar.** Flows and resources pages get a
  collapsible folder tree derived from the identity path. Breadcrumb
  shows the selected scope; row display is `display_name` (bold) +
  path (mono, dimmed). Persists collapsed state per view.

### Internal

- **`activity = true` marker removed from every processor.** Activity
  classification is now purely topological (span chain contains
  `task.run` or `task.handle`) instead of a per-emit boolean that every
  processor had to remember to declare. `StoredLog` gains structural
  fields (`level`, `timestamp`, `target`, `spans: Vec<StoredSpan>`,
  `fields`) so consumers no longer parse a flat `attributes` bag.

## 0.127.0

### Features

- **Generated `flowgen_client` crate + `openapi.yaml` spec.** The
  admin API (`/api/flows`, `/api/flows/{name}`, `/api/flows/stream`,
  `/api/resources`, `/api/resources/{key}`, `/api/version`) and the
  health endpoints (`/livez`, `/readyz`, `/healthz`) are now
  described by `flowgen/api/client/openapi.yaml`, from which
  `flowgen_client` is generated via `progenitor` at build time.
  Consumers (web UI, CLI, external tooling) can depend on the crate
  or fetch the spec directly.

### Fixes

- **Admin web UI works at any `web.path`.** The SvelteKit bundle
  hardcodes `/flowgen` as its base at build time. `serve_embedded`
  now rewrites `/flowgen/…` and bare `"/flowgen"` in every text
  asset to whatever `web.path` the operator set, so mounting the UI
  on `/`, `/admin`, or any custom prefix works without a rebuild.

## 0.126.0

### Fixes

- **`oci_sync` HEAD-check now skips unchanged multi-arch tags.** The
  cache stored the per-platform manifest digest, while HEAD returned
  the index digest for multi-arch tags — so the compare never matched
  and every tick re-pulled every layer. The cache now stores whatever
  HEAD returned. Integration test
  `oci_sync_second_tick_skips_when_manifest_unchanged` covers the
  single-arch skip path.

## 0.125.0

### Breaking

- **Kubernetes probes moved to a dedicated port.** The helm chart
  (v0.19.0) points `livenessProbe` at `/livez` and `readinessProbe`
  at `/readyz` on a new `health` port (default 8081), instead of
  `/healthz` on the API port. Pins that override `livenessProbe` or
  `readinessProbe` in `values.yaml` need to be updated. Anything
  hardcoding `/healthz` on port 3000 in external tooling (ingress
  health checks, external monitors) needs to switch to `/livez` on
  port 8081. `/healthz` is preserved as an alias of `/livez` on the
  new listener for pre-1.16 k8s convention.

- **`telemetry.otlp_endpoint` replaced by `telemetry.backend`.** The
  telemetry section now selects between an in-process backend and a
  remote OTLP collector through a tagged `backend` block. Existing
  configs that set `enabled: true` with no other fields still work
  (they fall back to the in-memory backend). To keep pushing to a
  collector, replace:

  ```yaml
  telemetry:
    enabled: true
    otlp_endpoint: "http://otel-collector:4317"
  ```

  with:

  ```yaml
  telemetry:
    enabled: true
    backend:
      type: remote
      endpoint: "http://otel-collector:4317"
  ```

### Features

- **Dedicated k8s health listener at `flowgen_core::health`.** Spawns
  its own axum listener on `health.port` (default 8081), independent
  of the API listeners. Exposes `GET /livez` (always 200), `GET
  /readyz` (200 once at least one flow is registered, 503 before),
  and `GET /healthz` as an alias for `/livez`. Probes keep working
  regardless of which API surfaces (`http_server`, `mcp_server`,
  `ai_gateway`, `web`) are enabled. Set `health.enabled: false` to
  disable the listener entirely.

- **JSON stdout logs consumable by the admin UI.** Logs continue to be
  written as JSON via `tracing_subscriber::fmt::json()`. In `memory`
  backend mode a copy of that stream is parsed into an in-process
  per-flow ring buffer (default 1000 entries per flow, configurable
  via `backend.logs_per_flow`) exposed through the `LogsQuery` trait
  the admin UI reads. In `remote` mode logs stay on stdout only —
  operators plug in Fluent Bit / Vector / Grafana Alloy to forward
  them to Loki, VictoriaLogs, Elasticsearch, or wherever.

- **Backend-agnostic `LogsQuery` trait.** The web layer no longer
  knows what backend serves its activity view; a memory
  implementation ships today, remote backends (Loki, VictoriaLogs)
  land in follow-up work through backend-specific crates.

- **`telemetry.backend` tagged enum with per-backend fields.** The
  `memory` variant accepts `logs_per_flow` and `metrics_per_flow`;
  the `remote` variant accepts `endpoint`. Invalid combinations fail
  at parse time.

- **Per-invocation `task.handle` span on source tasks.** NATS
  JetStream subscribers, Salesforce Pub/Sub subscribers, HTTP
  endpoints, MCP handlers, the AI gateway LLM proxy, `git_sync`,
  `oci_sync`, and `buffer` flushes now emit a `task.handle` span per
  message / request / tick. The DAG duration badge and the activity
  panel show live per-invocation timing for these tasks instead of
  `—`.

- **`duration_ms` backfilled onto every `task.handle` span.** The
  activity layer records the elapsed wall-clock on the current
  `task.handle` span via a `field::Empty` placeholder declared on
  the `#[instrument]` macro. `tracing_subscriber::fmt::json()` then
  includes it in the emitted `spans` array, so downstream consumers
  (admin UI, log shippers, OTel span exporters) all see the same
  per-event duration.

- **Admin activity panel renders structured attributes.** Custom
  tracing fields beyond the known system attributes (flow, task,
  level, timestamp, event.id, duration_ms) now surface as a
  key/value list under the message in the drawer. Context fields
  attached via `EventLogger::context()` are serialized as a JSON
  object so consumers can split them back into individual
  attributes rather than parsing a joined string.

- **Copy full log JSON from the activity drawer.** The drawer's
  copy button now serializes the entire selected record — message,
  timestamps, level, flow/task/processor, event id, duration, and
  every extra attribute — into one JSON blob suitable for pasting
  into tickets or chat.

- **Explicit `activity = true` marker on every `task.handle` span.**
  The admin activity feed filters on this marker so lifecycle logs
  emitted from `task.run` scope (endpoint registration, resource
  registration, startup traces) stay in stdout but do not clutter
  the per-event UI. Errors and warnings that arise in the same
  scope still surface — the marker only filters info-level noise.

- **NATS `credentials_path` is now optional.** The `cache`,
  `nats_kv_store`, `nats_jetstream_subscriber`, and
  `nats_jetstream_publisher` config sections accept an omitted
  `credentials_path`. When omitted, flowgen sends no credentials on
  connect — the connection then succeeds only against a NATS server
  that does not require authentication.

- **Integration test coverage for NATS.** The `flowgen_nats` crate
  gains `cache_integration`, `jetstream_integration`, and
  `kv_store_integration` test suites that spin up a real NATS
  JetStream server via testcontainers and exercise every method on
  the `Cache` trait plus the publisher / subscriber / KV-store
  processors. `flowgen` adds `init_cache_regression` covering the
  unified `App::init_cache` code path against a real broker. All
  integration tests are `#[ignore]`-gated and run in the
  `test-integration` CI job with a single
  `cargo test --workspace --tests -- --ignored` invocation.

### Fixes

- **k8s probes no longer 404.** During the earlier HTTP server
  refactor from `flowgen_http::server` into
  `flowgen_core::http_server` the previous `/healthz` handler was
  lost, silently — probes returned 404 and pods crashlooped as soon
  as the readiness `failureThreshold` was reached. Replaced with the
  dedicated health listener above so the regression cannot recur:
  even a service with every API listener disabled still answers
  probes.

- **`init_cache` unified between system + user cache paths.** The two
  cache init call sites in `App` previously diverged on the
  `history` / `tombstone_ttl` overrides. Both paths now go through a
  single `App::init_cache` so per-tenant history overrides
  (e.g. NATS KV's server-side cap of 64) apply uniformly and the
  system cache no longer races with the runtime cache during
  bootstrap.

- **Default KV history dropped from 1000 → 64.** The NATS KV server
  hard-caps per-subject history at 64; the previous default caused
  `too long history` errors on real deploys. Callers that want fewer
  retained versions can still lower the value via `cache.history`.

- **`Cache::delete_with_revision` returns `RevisionMismatch` on
  stale revisions.** The NATS backend used to fold every underlying
  `UpdateError` into the generic `DeleteFailed` variant, so lease
  release code that pattern-matched on `RevisionMismatch` never
  detected loss of ownership. Mapping now mirrors what
  `Cache::update` already did.

## 0.124.0

### Breaking

- **`worker:` config namespace collapsed to the top level.** Flowgen ships
  as a single binary — there is no separate worker/server split — so the
  `worker: { http_server, mcp_server, ai_gateway, web, retry,
  event_buffer_size }` grouping was misleading. These fields now live at
  the root of the config alongside `cache`, `flows`, `resources`, and
  `telemetry`. Migrate existing configs by unindenting one level and
  removing the `worker:` header line.

- **Node.js 22+ required to build from source.** `build.rs` in the
  `flowgen` crate runs `npm ci && npm run build` in `web/` so a plain
  `cargo build` produces a complete binary. Set
  `FLOWGEN_SKIP_WEB_BUILD=1` to skip the UI build when working on
  unrelated crates. Prebuilt binaries and Docker images are unaffected.

### Features

- **Admin web UI.** New embedded SvelteKit dashboard served by the
  worker (opt-in via `web.enabled: true`). Lists loaded flows with
  description, tags, status, and last-run timestamp; click a row to
  preview the raw source YAML with Prism syntax highlighting.
  Adds a Resources tab that browses everything under the configured
  `resources.path` and renders `.md` files with `marked` plus code
  files with Prism (SQL, YAML, JSON, Rhai, JS/TS, Bash, Python).
  Sidebar layout with collapsible navigation, dark-mode toggle, and
  the running flowgen version. Backend exposes read-only
  `GET /api/flows`, `GET /api/flows/{name}`, `GET /api/resources`,
  `GET /api/resources/{key}`, and `GET /api/version`.

- **Flow labels: `description` and `tags`.** Flows can declare
  `labels.description` (free-form string) and `labels.tags` (array of
  strings) — surfaced on the admin UI so operators can categorize and
  search loaded flows without reading the raw YAML.

- **AI completion preserves upstream event meta.** `ai_completion`
  used to overwrite `event.meta` with only the completion context
  (provider, model, latency, usage), silently dropping any custom
  fields upstream tasks had stashed via `ctx.meta.<field> = ...`.
  The response now merges the completion context into the incoming
  meta, so multi-step flows (script → ai_completion → downstream
  script) can round-trip per-event state through the LLM turn.

- **OpenAI content-parts array on the AI gateway.** The gateway's
  `POST /v1/chat/completions` handler previously required
  `messages[].content` to be a bare string; Rig, the OpenAI SDKs,
  and several third-party clients emit the array form
  (`[{"type":"text","text":"..."}]`) even for pure-text turns and
  were rejected with a 422. `Message::content` now accepts either
  shape and flattens the array back to a single string internally.

### Helm chart (0.18.0)

- **Opt-in `Service` blocks for `ai_gateway` and `web`.** Mirrors the
  existing `mcp_server` pattern — commented out by default so no
  extra Service is rendered unless the operator opts in. Both default
  to `ClusterIP` when enabled.

### Security

- **Admin UI is unauthenticated.** The embedded `web` UI does not yet
  ship with a login flow. Do not expose the admin port publicly.
  Recommended access patterns:
  - `kubectl port-forward svc/<release>-web 8080:8080` for one-off use.
  - An authenticated reverse proxy (OAuth2-proxy, Cloudflare Access,
    tailnet, etc.) in front of a `ClusterIP` Service for shared team
    access.
  The backend `/api/*` handlers are read-only; even so, they expose
  loaded flow configs and resource contents which may include
  templated secrets or query text.

## 0.17.0

### Helm chart

- **`flowgen.podTemplate.merge` and `flowgen.podTemplate.patch` hooks** let
  operators inject arbitrary pod spec fields without expanding the chart
  surface. `merge` overrides top-level fields such as `hostNetwork` and
  `dnsPolicy`; `patch` applies RFC 6902 JSON Patch operations to the
  rendered pod spec. The `jsonpatch` helper is included in the chart and
  follows the same convention used by the NATS Helm chart.

- **Rendered-manifest tests for the chart.** New `flowgen-chart-tests`
  workspace crate shells out to `helm template` and asserts on the
  resulting Kubernetes manifests. Covers the `podTemplate.merge` and
  `podTemplate.patch` hooks so regressions in the pod-spec injection
  surface are caught in CI. Runs in a dedicated `test-helm-chart` job
  that installs Helm alongside Rust; the crate is excluded from the
  workspace `default-members` so `cargo test` at the root does not
  require `helm` on `PATH`.

## 0.123.0

### Features

- **`EventData::Bytes(bytes::Bytes)`** carries binary payloads through the
  pipeline for content that cannot be safely coerced to UTF-8. Rendered as
  a base64 string by `TryFrom<&EventData> for Value` so Handlebars
  templates against `event.data` do not crash; downstream tasks that need
  the raw bytes match `EventData::Bytes` directly.

- **`http_request response_type: json | text | bytes`** picks how outbound
  response bodies are decoded. Default remains `json` for backwards
  compatibility. `bytes` emits `EventData::Bytes` — enables downloading
  ZIP archives, image blobs, or other binary payloads without lossy UTF-8
  coercion.

- **`object_store::write` writes raw bytes.** New `WriteFormat::Bytes`
  (auto-selected when the incoming event carries `EventData::Bytes`)
  passes the payload through verbatim with a `.bin` extension.

### Bug fixes

- **`oci_sync` no longer rejects binary layers.** When a pulled layer
  (or tar entry inside a layer) is not valid UTF-8, the processor emits
  the bytes as `EventData::Bytes` with `path`, `digest`, and
  `artifact_digest` on `event.meta`, instead of returning
  `InvalidLayerEncoding` and failing the pull after retries. oras
  layers holding UTF-8 content still emit the historical JSON
  `FileEvent` shape so existing bootstrap flows are unaffected.

- **`oci_sync` merges Docker image layers with overlay-fs semantics.**
  Layers without the `org.opencontainers.image.title` annotation are
  classified as Docker image layers and merged in manifest order: later
  layers override earlier ones, `.wh.<name>` markers delete files from
  lower layers, and `.wh..wh..opq` markers hide entire subtrees. Only
  the surviving final state is emitted downstream. oras artifact
  layers (carrying `image.title`) keep the per-layer emission
  behaviour, so existing bootstrap flows are unaffected. Detection is
  fully automatic — no config.

## 0.122.0

### Features

- **`braze_export_users_ids` task for Braze user profile exports.** New
  `flowgen_braze` crate wraps the Braze REST SDK and exposes
  `POST /users/export/ids` as a flowgen processor. Supports all Braze
  identifier types (`external_ids`, `user_aliases`, `device_id`,
  `braze_id`, `email_address`, `phone`) and `fields_to_export`.
  Identifier values can be static strings or Handlebars templates
  rendered against the incoming event. The Braze response is emitted as
  a JSON event downstream. Configuration uses `credentials_path`
  pointing to a JSON file with `api_key` and `rest_endpoint`. See
  `examples/braze/export_users_ids.yaml`.

- **AI gateway speaks Anthropic and forwards token usage.** The
  `llm_proxy` task grows a `protocol` field: leaving it at the
  default `openai` keeps `POST /v1/chat/completions` behaviour
  unchanged, while `protocol: anthropic` mounts `POST /v1/messages`
  on the same server so clients honouring `ANTHROPIC_BASE_URL`
  (Claude Code, the official Anthropic SDKs) reach downstream
  OpenAI-compatible providers through the gateway with no client
  changes. Anthropic's `x-api-key` header is folded into the
  standard `Authorization: Bearer` before endpoint-level auth runs,
  so existing credentials JSON files keep working unchanged. Model
  routing follows the same `<proxy-name>/<downstream-model>`
  convention as OpenAI. Both protocols share a single
  `EventPayload` in `event.data`, so a leaf `ai_completion` task
  never learns which protocol the client used.

  Token usage now round-trips end-to-end. Non-streaming responses
  populate `ChatCompletionResponse.usage` (OpenAI) or the response
  `usage` field (Anthropic) whenever the leaf task reports it. For
  OpenAI streaming the gateway follows the spec: usage rides an
  extra SSE frame with `choices: []` and `usage: {...}` immediately
  before `data: [DONE]`, and only when the client opts in with
  `stream_options.include_usage: true`. Without the opt-in the
  stream stays byte-for-byte compatible with clients that never
  expect the extra frame. Anthropic streaming always attaches usage
  to `message_delta`, matching the upstream API. Malformed leaf
  usage is logged and dropped rather than propagated so token
  accounting never fails an otherwise successful completion.

- **`bigquery_query` streams one event per result page.** Previous
  path buffered all `getQueryResults` pages into a single
  `RecordBatch` before emitting; peak RAM was ~2× the query result.
  Now each page is emitted as it arrives, with `completion_tx`
  peeked one page ahead. `job_id` stamped on the first event only.

- **`bigquery_query` optional Storage Read backend.** New
  `use_storage_read: bool` (default `false`) on the task config.
  When enabled, the query result flows through the BigQuery Storage
  Read API and is emitted one Arrow batch at a time via the same
  `RecordBatchIterator` the `bigquery_storage_read` task uses, so
  large result sets (over one million rows) skip the per-row `Tuple`
  conversion the REST backend pays. Trade-off: adds temporary-table
  overhead for small queries and is not compatible with
  data-definition or data-manipulation statements. Backed by the new
  upstream `query_record_batches` helper in
  `connve/google-cloud-rust`.

- **`bigquery_storage_read` streams RecordBatches from the wire.**
  The processor no longer collects the full result set before emitting
  the first downstream event; each Arrow `RecordBatch` returned by the
  Storage Read API is emitted as its own event as it arrives, with
  `completion_tx` peeked one batch ahead so it still attaches to the
  final event. Small tables that used to choke on ~5K rows now feel
  instant: the previous `RecordBatchMarker` + `StructDecodable` hack
  forced the upstream library to allocate one marker per row (a
  5,000-element `VecDeque` for a single Arrow batch, 4,999 of which
  were immediately discarded), deep-clone each column's `DataType`,
  and rebuild the schema client-side for every batch. All of that is
  gone. Backed by a new upstream `read_table_record_batches` /
  `RecordBatchIterator` API in the pinned `connve/google-cloud-rust`
  fork, which decodes Arrow IPC bytes once per BQ batch instead of
  once per row and yields `RecordBatch` directly. Arrow bumped from
  56.1 → 58.3 workspace-wide to match the fork; no API drift in
  flowgen call sites.

- **`oci_sync` unpacks tar and tar+gzip layers.** Container images
  pushed with `docker push` (scratch-based or otherwise) and OCI
  artifacts whose layers use `application/vnd.oci.image.layer.v1.tar+gzip`
  or `application/vnd.docker.image.rootfs.diff.tar.gzip` now flow
  through the same bootstrap pipeline as raw oras artifacts. The
  layer's media type dispatches the extractor: raw layers still emit
  one `FileEvent` per layer with the path taken from
  `org.opencontainers.image.title`; tar and tar+gzip layers emit one
  `FileEvent` per file entry, with the path taken from the tar entry
  name (leading `/` stripped). Directories, symlinks, hardlinks, and
  docker whiteout markers (`.wh.*`) are skipped. This unblocks
  tenants whose CI can push Docker images but not `oras`
  artifacts — a common Artifactory / enterprise-CI constraint.

  Two new fields on `oci_sync` guard against tar-bomb layers:
  `max_file_size` (default 10 MB, per extracted file) and
  `max_total_size` (default 100 MB, cumulative across all layers of
  one artifact). Both are enforced during tar iteration, so a
  malicious layer fails fast before consuming memory. Raw layers
  are subject to the same caps.

### Breaking changes

- **AI gateway event meta: `model` → `requested_model`.** The gateway
  now writes the client-requested alias under `event.meta.requested_model`;
  the completion leaf still writes the actual upstream model under
  `event.meta.model`. Update flows and scripts accordingly.

### Fixes

- **`git_sync` now sees new upstream commits without a pod restart.**
  The previous `fetch_existing` path only advanced the
  remote-tracking ref; it never updated `refs/heads/<branch>` or the
  working tree, so `head_commit` kept returning the original
  shallow-clone commit and the file walk was skipped on every tick.
  `git_sync` now wipes and re-clones the `clone_path` at the start
  of every sync. Shallow clone at depth 1 transfers the same pack
  file the fetch would have — the cost is one `remove_dir_all` on
  ~KB of shallow state — and guarantees the working tree matches
  HEAD every tick. Discovered by the new git integration tests;
  `Error::Fetch` (unused after the removal) went with the same
  commit. Also fixes a related bug: when `path` was `None`, the file
  walk descended into `.git/` and leaked `.git/config` and shallow
  markers into the downstream pipeline; the walker now excludes the
  `.git/` directory unconditionally.

- **`oci_sync` size-cap breaches now fail fast instead of retrying
  for ~15 minutes.** `Error::FileTooLarge` and
  `Error::ArtifactTooLarge` were classified as transient by the
  handler-level retry loop, so a tar-bomb artifact would loop through
  the full retry budget before surfacing the failure to downstream —
  defeating the "fails fast before consuming memory" promise of the
  size caps themselves. Both errors are now classified as permanent
  alongside `InvalidReference`, `ParseCredentials`, and
  `InvalidLayerEncoding`, so a busted artifact reports its error to
  downstream on the first attempt. Discovered by the new size-cap
  integration tests.

## 0.121.0

### Features

- **MCP resources, prompts, and argument completion.** Three new
  task types round out flowgen's MCP surface beyond the existing
  `mcp_tool`: `mcp_resource` exposes curated read-only content
  (schemas, glossaries, configmap-backed docs) at either fixed URIs
  or RFC 6570 URI templates whose `{param}` bindings are rendered by
  Handlebars per read; `mcp_prompt` publishes slash-command
  templates with typed arguments (required/optional, defaults,
  multi-message few-shot form) that MCP clients like Claude Desktop
  surface in their prompt palette. `completion/complete` serves
  argument autocomplete for both, backed either by an inline
  `values: [...]` list or a loader-backed file (`resource:
  "completions/x.txt"`, one value per line, `#` comments and blanks
  skipped). The shared `Completion` enum and `parse_completion_lines`
  helper live in the new `flowgen_mcp::completion` module so both
  task types stay in sync without cross-module coupling.

  The MCP server now hosts a long-lived SSE stream on `GET
  <path>`; connecting clients receive an `Mcp-Session-Id` header
  and get pushed `notifications/resources/list_changed` and
  `notifications/prompts/list_changed` whenever a flow registers or
  hot-reloads MCP entries. Broadcast is non-blocking (`try_send`
  with lazy eviction of sessions whose receiver dropped) and runs
  outside the dispatch-table shard lock so notification fan-out
  never blocks registration. Auto-generated resource URIs use a
  configurable `worker.mcp_server.resource_uri_scheme` (default
  `flowgen`) — white-label deployments override it (`acme`,
  `customer_x`, etc.) so LLM-visible identifiers carry the
  deployment's brand instead of leaking `flowgen://` into every
  cited resource. Users can still set an explicit `uri:` or
  `uri_template:` per task to override the auto form entirely
  (`https://docs.example.com/api.md` for genuinely public URLs,
  custom schemes for template patterns). `deregister_flow_all`
  replaces `HttpServer::deregister_flow` in the reconciler so hot
  reload sweeps every extras-backed table (resources, templates,
  prompts) together and emits notifications only when a table
  actually lost an entry (`retain_and_count` helper counts inside
  the atomic retain closure to close the len-before/len-after race
  window).

  Wire layer: replaced the last preexisting `serde_json::json!()`
  call sites in `flowgen/mcp/src/server.rs` with `Serialize` structs
  (`InitializeResult`, `ToolListEntry`, `ResourceListEntry`,
  `ResourceTemplateListEntry`, `ResourceContent`, `PromptListEntry`,
  `PromptMessageWire`, `CompleteResult`, and their result envelopes),
  and made `json_rpc_response` generic over any `T: Serialize` so
  handlers no longer produce untyped `Value`s. Error surface
  extended with typed variants for every JSON-RPC error path
  (`UnknownResourceUri`, `UnknownPrompt`, `MissingPromptArgument`,
  `ConcreteBodyIsTemplate`, `TemplateBodyIsConcrete`,
  `ResourceTemplateRender`, `PromptMessageRender`,
  `SseResponseBuild`, `SessionIdEncoding`, plus per-registration
  `ResourceUriCollision` / `PromptNameCollision`) instead of ad-hoc
  `format!` strings, so callers get structured errors and the log
  line matches the wire message exactly.

- **AI gateway tool-use passthrough.** New `tool_passthrough:
  bool` field on the `ai_completion` task config (default
  `false`). When set, the processor bypasses rig's agent loop and
  forwards the request's `tools` and `tool_choice` verbatim to the
  upstream provider; tool invocations returned by the model surface
  as `tool_calls` on the response instead of being executed
  in-process. Unblocks opencode, Claude Desktop, and any other
  OpenAI-tool-use SDK from driving models through the flowgen AI
  gateway — before this the `tools:` block was silently dropped by
  serde and the model never saw it. Passthrough is a no-op when the
  request omits `tools`, so a single flow serves both tool-using
  and plain chat clients. Works across every provider flowgen
  supports (13 rig-backed providers + `provider: custom`) via a
  provider-agnostic `Passthrough` trait with a blanket impl over
  `rig::client::CompletionClient`; no per-provider dispatch code.

  OpenAI wire types (`Message`, `ToolDefinition`, `ToolCall`,
  `ChatCompletionRequest`, `ChatCompletionResponse`,
  `Delta.tool_calls`, `ToolCallDelta`) added to
  `flowgen/ai-agent/src/ai_gateway/config.rs`. Conversion between
  the OpenAI shape and rig's provider-agnostic completion types
  lives in the new `flowgen/ai-agent/src/completion/passthrough.rs`
  module. Streaming path emits an open-delta + arguments-delta pair
  per tool call plus `finish_reason: "tool_calls"` on the final
  chunk, matching OpenAI's incremental tool-call SSE format.

  The `local/flows/ai-gateway/llm-proxy.yaml` proxy opts in.

## 0.120.1

### Build

- **CI toolchain source unified with `rust-toolchain.toml`.** All
  three workflows (`lint`, `test`, `security`) migrated from the
  deprecated `actions-rs/toolchain@v1` to
  `dtolnay/rust-toolchain@stable`, which reads the channel and
  components from `rust-toolchain.toml`. Removes the duplicated
  `toolchain: stable` overrides that could drift from the repo pin
  and reintroduce the developer-vs-CI mismatch that produced the
  0.120.0 `result_large_err` false-negative locally.
- **MSRV enforced in CI.** New `test-msrv` job in
  `test.yml` compiles the workspace on the exact
  `rust-version = "1.88"` declared in `Cargo.toml`, so the field
  stops being an aspirational promise. Uses `cargo check --locked`
  to catch dependency bumps that raise the minimum silently.
- **Docker builder image switched to `rust:slim-bookworm`.** Smaller
  base (fewer system packages) means fewer CVE hits without changing
  the toolchain contract — the channel still resolves from
  `rust-toolchain.toml` at the first `cargo` invocation. Runtime
  image (`gcr.io/distroless/cc-debian12:nonroot`) is untouched.

## 0.120.0

### Features

- **`buffer` flushes on upstream completion signal.** New
  `flush_on_completion: bool` field on the `buffer` task config
  (default `false`). When set, the buffer flushes immediately upon
  receiving an event that carries an upstream completion handle,
  instead of waiting for `size` or `timeout`. Sources that emit a
  known-finite batch (`git_sync`, `oci_sync`) already attach the
  completion handle to the final event in the batch, so the buffer
  now sees the end of the batch deterministically and forwards a
  complete view downstream. Previously the buffer relied on
  `timeout` alone — picking an artifact pull that took longer than
  `timeout` produced a partial batch, which the bootstrap's
  `compute_diff` mistook for "those files no longer exist" and
  emitted spurious delete actions, oscillating the cache. The flag
  is honoured by both buffer code paths (`process_events_single`
  and `process_events_keyed`); the bootstrap sync flows do not use
  `partition_key`, so the first cut that only patched the keyed
  path went unused in production. The bootstrap sync flows in
  `local/flowgen.yaml` and the four `examples/{git,oci}/system_sync_*`
  flows opt into the new flag.

- **`oci_sync` and `git_sync` skip the pull when nothing changed.**
  Each tick now compares a cheap signature against the last
  successful run before re-fetching content. `oci_sync` keeps the
  artifact's manifest digest in the shared cache under
  `flow.{flow}.oci_digest.{artifact}`; the change signal is a plain
  HTTP HEAD against the manifest URL (`fetch_manifest_digest`), so
  a cache hit costs one zero-body request — no manifest JSON, no
  config blob, no layer blobs. If the HEAD digest matches, the
  source emits only the upstream completion signal.
  `git_sync` keeps the HEAD commit hash under
  `flow.{flow}.git_head.{repo}`; if the post-fetch hash matches, the
  file walk is skipped. Works for both mutable tags
  (`ghcr.io/org/flows:prod`) and immutable digests
  (`…@sha256:abcd`) — the manifest digest is authoritative in
  either case, so re-tagging `:prod` to a new release still
  triggers a pull. The signature is persisted only after every
  downstream event for the tick has been sent, so a mid-batch
  failure causes the next tick to re-emit the full set
  (idempotency mirrors `salesforce::pubsubapi::subscriber`'s
  replay_id handling). A tenant reported 4.7K identical blob pulls
  against a single GHCR artifact before this landed; the OCI fix
  collapses that to one manifest HEAD per tick once the cache is
  warm. Both tasks accept `force_pull: true` to bypass the cache
  for the operator case where the downstream cache was mutated
  out of band and needs re-seeding.

- **`oci_sync` integration test against a real registry.** New
  `flowgen/oci/tests/sync_integration.rs` stands up a
  `registry:2.8.3` container via `testcontainers`, pushes a
  multi-layer artifact that mirrors the tenant-connve GHCR layout
  (empty config + N file layers, each tagged with its relative path
  in `org.opencontainers.image.title`), drives a single tick
  through `oci_sync`, and asserts: one downstream event per file
  layer, in upstream push order, with the `completion_tx` handle
  attached to the final event only. This catches the class of bugs
  the bootstrap oscillation belonged to — anything that breaks the
  end-of-batch contract between `oci_sync` and the downstream
  buffer surfaces here, instead of in production logs. `oci_sync`'s
  `init` also gained a small loopback-host check that switches to
  plain HTTP when the artifact reference points at `127.0.0.1`,
  `localhost`, or `[::1]`, so this test (and any future
  registry-backed test) can run without TLS termination. The test
  is marked `#[ignore]` so the default `cargo test` skips it on
  machines without a Docker daemon; a dedicated `test-integration`
  job in `.github/workflows/test.yml` runs it on every PR via
  `cargo test -- --ignored`.

- **`nats_kv_store list` can include current values.** New
  `include_values: bool` field on the `nats_kv_store` config
  (default `false`). When set with `operation: list`, the result
  carries a `values: { key: content }` map alongside `keys` so
  downstream scripts can diff incoming content against what is
  already in cache and skip identity puts. Without this, every
  bootstrap tick wrote every flow YAML even when the content was
  unchanged; every put bumped the NATS KV revision and fired a
  watcher hot-reload on every pod, which in turn raced
  leader-election leases. The bootstrap `compute_diff` now consults
  `event.data.values[key]` and only emits a `put` action when the
  cached content differs from the new content.

- **Config hot-reload.** Flowgen now watches the config directory for
  changes and automatically reconciles flows without restarting. Added
  flows start, removed flows stop, and modified flows are restarted.
  File system events are debounced to avoid churn during multi-file
  saves. HTTP server registrations (webhooks, MCP tools, AI gateway
  proxies) are deregistered per-flow before restart so stale entries
  never linger. The reconciler also calls
  `TaskManager::unregister(flow_name)` on the old flow before swapping
  in the replacement, which aborts that flow's lease renewal background
  task without touching the lease key in NATS. Without this step the
  old renewer survived the `Flow` drop in tokio and kept writing
  renewals to `lease.<flow_name>` under the same pod-level
  `holder_identity`, racing the replacement flow's renewer and
  producing endless `Lost lease ownership during renewal` warnings
  whose `current_holder` was the pod itself.

- **System cache separated from runtime cache.** Leader-election
  leases now live in the system bucket (typically `flowgen_system`)
  instead of the runtime bucket (`flowgen_cache`). The runtime bucket
  is what user scripts reach through `ctx.cache`; keeping leases out
  of it removes the risk of a script overwriting a coordination key,
  and lets the two buckets carry different retention policies (short
  history plus per-message TTL for leases, longer retention for
  per-flow state like replay IDs and counters). `FlowBuilder` now
  requires both `cache` and `system_cache`; in single-binary or
  in-memory deployments the same `Arc` is passed for both, so the
  separation is opt-in by configuration. Existing NATS deployments
  should drop the stale `lease.*` keys from the runtime bucket after
  upgrade; they are no longer renewed and will linger as dead
  entries until manually removed.

- **`Event::Display` drops `None` fields from the pretty output.**
  The pretty-printer used by the `log` task (and any `format!("{event}")`
  call) now omits `id`, `meta`, and `error` when they are `None`
  instead of emitting `"id": null`, `"meta": null`, `"error": null`.
  Industry-standard structured-log convention (`omitempty`, serde
  `skip_serializing_if = "Option::is_none"`, Pydantic
  `exclude_none=True`) — null fields are noise that bloats log
  storage and forces query parsers to handle `field == null` vs
  `field missing` as distinct cases. Always-present fields
  (`subject`, `data`, `timestamp`, `task_id`, `task_type`) keep
  appearing on every line.

- **`log` task drops `event.meta` from output by default.** Added
  `include_meta: bool` (default `false`) on the `log` task config.
  Meta is in-flow context for downstream tasks — stashed batches,
  correlation ids, per-event state — and tends to dwarf the data
  payload in any log line that prints it. With `include_meta: false`
  the log target sees only `subject`, `data`, `id`, `timestamp`,
  `task_id`, `task_type`, and `error`; meta still rides the
  pass-through event downstream unchanged. Operators that actually
  want meta in the log line opt in explicitly with `include_meta:
  true`. This unblocks observability for the system-sync bootstrap
  flows, which stash the full git batch (including file contents)
  in meta so `compute_diff` can read it after `nats_kv_store list`
  overwrites `event.data`.

- **Bootstrap sync flows self-heal on cache drift.** The
  `system_sync_flows` and `system_sync_resources` bootstrap flows
  in `local/flowgen.yaml` replaced their `skip_if_unchanged`
  short-circuit with `skip_if_in_sync`, which also checks that the
  cache key count matches the file count from the repo. Previously
  the bootstrap stamped `last_synced_commit` once it had written
  any keys and then short-circuited every subsequent tick on commit
  match alone — so any out-of-band deletion of a `flowgen.flows.*`
  or `flowgen.resources.*` key left the cache permanently missing
  that entry until an operator manually cleared `last_synced_commit`.
  The new check forces a full diff whenever the key count diverges,
  and the per-tick `log_actions` task makes every action
  (`put`/`delete`) visible at `info!` so a stuck pipeline is no
  longer silent.

- **`git_sync` emits include file path and short commit as log
  context.** The per-file `send_with_logging` call in
  `flowgen/git/src/sync/processor.rs` attaches `path=<file>` and
  `commit=<7-char hash>` via `EventLogger::context`, so every
  `event.subject=pull_repo` line names the file and revision
  instead of just the event id. Makes git-sync activity readable
  at a glance and lines up with the bootstrap's per-action log
  output for end-to-end traceability.

- **`EventExt::send_silent` for streaming hot paths.** Per-event logging
  via `send_with_logging` remains the default and stays at `info!` so
  operators see event flow without `RUST_LOG=debug`. Streaming processors
  whose intermediates would flood the log (the `ai_completion` per-token
  chunk path) now route through a new `send_silent` method, with the
  final chunk still going through `send_with_logging` so completion is
  observable. The MCP tool dispatch in `mcp/server.rs` also moves to
  `send_silent` because it already emits a richer `MCP tool invoked`
  info line of its own; previously it used a raw `tx.send` to dodge
  the double log. `grep -rn send_silent` lists every place we
  suppress event-flow visibility.

- **`#[allow(...)]` attributes banned workspace-wide.** Workspace
  `Cargo.toml` declares `[workspace.lints.clippy] allow_attributes =
  "deny"` and every crate opts in via `[lints] workspace = true`.
  Suppressing a lint now requires `#[expect(lint, reason = "...")]`,
  which turns into an error if the lint stops firing — dead
  suppressions surface automatically instead of rotting into the
  codebase. The one legitimate suppression
  (`flowgen/app/src/config.rs`'s `TaskType` enum, whose lowercase
  variants are YAML tags) has been migrated to `#[expect]` with a
  reason.

- **Generic HTTP server infrastructure.** Webhooks, MCP, and AI gateway
  now share a single `HttpServer<D>` type parameterised by a `Dispatcher`
  trait. Each role owns its URL layout and request handling; the server
  owns the listener, dispatch table, and hot-reload lifecycle. This
  eliminates duplicated server boilerplate across roles.

- **AI gateway (LLM proxy).** New `llm_proxy` task type exposes flows as
  OpenAI-compatible chat completion endpoints. Supports both OpenAI and
  Anthropic upstream protocols with automatic request/response
  translation. Multiple gateways can run on a single port, routed by the
  `model` field prefix.

- **OCI artifact sync.** New `oci_sync` task pulls flow definitions from
  an OCI registry (GHCR, ECR, GAR, Artifactory, Harbor) and emits one
  event per layer. Output shape mirrors `git_sync` so bootstrap
  pipelines can swap one for the other. Credentials auto-detect both
  the flowgen-native `{username, password}` shape and the standard
  Docker `config.json` (`kubernetes.io/dockerconfigjson`) payload — the
  same Secret used as the pod's `imagePullSecrets` can authenticate
  artifact pulls. Pure-Rust HTTPS stack via `oci-client`, no shell-out
  to `oras` or `docker`.

- **Versioned API paths.** Default MCP path changed from `/mcp` to
  `/mcp/v1` and default HTTP webhook path changed from
  `/api/flowgen/workers` to `/api/flowgen/workers/v1`, following the
  Google MCP convention. Paths remain configurable.

- **Force shutdown on second signal.** A second SIGTERM or SIGINT now
  forces an immediate exit instead of hanging. First signal initiates
  graceful shutdown, second signal calls `process::exit(1)`.

- **Git sync refactor.** Removed SSH auth support (`GitAuth`,
  `GitAuthType` enums) in favour of a simpler `credentials_path` field
  pointing to a JSON file with an HTTPS token. SSH URLs are rejected at
  startup with a clear error. The token is presented through a gix
  credential helper that responds to the server's `WWW-Authenticate`
  challenge — it never appears in the repository URL, in `.git/config`,
  or in logs. The credentials JSON now also accepts an optional
  `username` field (defaults to `x-access-token`) for hosts that
  require a specific literal username (GitLab OAuth tokens, Bitbucket
  Cloud token-auth).

### Improvements

- **Template rendering supports full object substitution.** A simple
  path template like `"{{event.data}}"` in a payload field now renders
  into the complete JSON object, preserving structure and types. This
  allows templates to replace `from_event: true` on HTTP and Salesforce
  payload configs. `from_event` still works but is now considered
  deprecated in favour of the template approach.

- **Per-event templating in `ai_completion`.** Credentials, endpoint, and
  model fields are now resolved at handle time against the incoming event
  instead of once at init. A single `ai_completion` task can route to
  multiple providers based on event data, with clients keyed and reused
  via the worker-wide `ClientRegistry`. Enables a consolidated LLM proxy
  flow (one task fans out to z.ai, Moonshot, OpenAI, etc.) instead of
  one flow per provider.

- **Leaf-task errors surface to source.** Added `signal_completion_with_error`
  to `CompletionState` so failed leaves deliver the underlying error to the
  source (HTTP webhook, MCP tool call, AI gateway request) instead of
  letting the source hang until `ack_timeout`. Wired across all 19
  leaf-capable processors. MCP tool callers and HTTP/AI-gateway clients
  now see the actual upstream error (Salesforce `MALFORMED_SEARCH`, LLM
  provider 4xx body, etc.) instead of an opaque timeout.

- **Fail-fast on upstream 4xx in Salesforce processors.** Adopts
  `salesforce_core 0.16.0`'s new `is_retryable()` helper on every
  `restapi`, `bulkapi`, `toolingapi`, `soapapi`, and `pubsubapi` error
  type. Permanent failures (bad input, auth, malformed query) now exit
  the retry loop immediately instead of being wrapped as transient. Same
  classification added to `ai_completion` for upstream LLM 4xx responses
  (text-matched against rig's error string until rig exposes a typed
  status). HTTP 429 is treated as permanent for LLMs since rate-limit
  recovery exceeds retry-loop timescales.

- **SOSL phrase content auto-escape.** `salesforce_restapi_search` now
  escapes SOSL reserved characters (`- ? & | ! { } [ ] ( ) ^ ~ * : \ " ' +`)
  inside `FIND {"..."}` phrase clauses before sending the query. Search
  terms with hyphens, ampersands, or other reserved characters work
  without manual escaping or `MALFORMED_SEARCH` errors. Already-escaped
  characters are passed through verbatim.

- **AI gateway error surfacing.** OpenAI-compatible streaming responses
  now emit `data: {"error": ...}` SSE frames when a leaf task fails;
  non-streaming responses return HTTP 502 with the underlying message
  instead of dropping the error and returning empty content.

- **`Source::Inline` now renders against event data.** Inline content
  passed through `Source::Inline::render()` previously assumed
  `config.render()` had already substituted templates at init time and
  returned the raw string. Processors that defer rendering to handle time
  (e.g. `ai_completion`) need the actual substitution. Inline templates
  without `{{...}}` syntax are unaffected (idempotent pass).

- **`ack_timeout` default documented.** The MCP `mcp_tool` task gained a
  fields table covering all configuration options. All five source-task
  docs (`generate`, `http/endpoint`, `nats/subscriber`, `salesforce/pubsub`,
  `ai/mcp`) now state the `ack_timeout` default as "wait indefinitely"
  with accurate per-source semantics.

- **SOSL prefix matching documented.** Added a "Prefix matching" section
  to `salesforce/rest` docs explaining how to append `*` inside the
  phrase to match prefixes. Example MCP/HTTP SOSL flows updated to use
  the wildcard form by default.

### Breaking changes

- **Lease cache keys preserve the flow name as written.** The task
  manager used to lowercase the flow name and convert underscores to
  hyphens when building the lease key (`flow.name`
  `salesforce_pubsubapi_account_subscriber` →
  `lease.salesforce-pubsubapi-account-subscriber`). The transform was
  a left-over from when the executor wrote to Kubernetes Lease
  objects, which require RFC 1123 DNS-safe names; the current NATS KV
  backend has no such constraint, and `validate_name` already keeps
  `flow.name` in a NATS-safe character set. The lease key is now
  built verbatim, so it matches what shows up in tracing fields,
  cache-prefix flow keys, and operator-facing logs. Existing
  deployments will see new lease keys appear after upgrade; the old
  `lease.<dashed>` entries linger as zombies until their TTL expires
  (60 s by default) and can be removed manually with
  `nats kv del flowgen_system lease.<old-name>` if desired.

- **`http_webhook` task renamed to `http_endpoint`.** The old name was
  misleading — the task handles any HTTP method (`GET`, `POST`, `PUT`,
  `DELETE`, `PATCH`), not only callback webhooks. The new name matches
  what the task actually does: it registers an endpoint on the worker's
  HTTP server. No alias is provided; existing flows must rename the task
  type. The docs route also moved from `/http/webhook` to `/http/endpoint`.

### Fixes

- **Dead dot-to-dash conversion in hot-reload key parsing removed.** The
  reconciler's cache-key-to-flow-name derivation rewrote every dot in
  the key suffix to a dash — a defensive transform left over from an
  earlier shape of the code. `validate_name` already rejects `.` in
  `flow.name` at config load, so the transform never had input to act
  on, but it would have silently broken round-trip if validation were
  ever loosened. The suffix is now returned verbatim.

- **Source ack hung in fan-out / fan-in DAGs.** When two branches re-merged
  at a buffer (or any other fan-in point), the source's completion channel
  was sized by summing children's leaf counts, which double-counted
  re-merged paths. The source then waited for more signals than there were
  real leaves, never received a successful ack, and never incremented its
  counter — a `generate` with `count: 1` fired every interval forever.
  Leaf counting now deduplicates by leaf index.

- **Buffer dropped acks when coalescing events from concurrent sources.**
  When the buffer accumulated events from distinct sources (e.g. concurrent
  webhook requests, parallel NATS deliveries), it kept only the last
  event's completion handle and silently dropped the rest, so N-1 sources
  per flush hung or saw their ack timeout fire. The buffer now collects
  every distinct upstream and fans the downstream ack back out to all of
  them. Events that share an upstream (single source fanned out and
  re-merged) still pass through directly with no proxy overhead.

- **`parse_json` in Rhai scripts now supports arrays.** The built-in
  Rhai `parse_json` only handles JSON objects at the root. The engine
  now registers a custom override that uses serde, supporting both
  arrays and objects. Fixes flows that load JSON array resources and
  pass them to `iterate`.

- **MCP Streamable HTTP spec compliance.** `notifications/initialized`
  now returns 202 Accepted instead of 200 OK. `ToolResult` serializes
  `isError` in camelCase per the MCP `CallToolResult` spec. SSE result
  events use the default format (`data:` only) instead of
  `event: message`, matching what standard MCP clients expect.

- **MCP and AI gateway servers start regardless of loaded tasks.** Servers
  now start when `enabled: true` in config, even if no matching tasks are
  loaded at startup. This supports hot-reload of flows from cache where
  tasks arrive after initial boot.

- **Improved config-not-found error message.** The error now includes the
  working directory and uses sentence case for clarity.

- **Unified NATS cache error messages.** All three NATS connection failure
  paths (system cache, flow cache, resource cache) now use a consistent
  message style and severity.

- **Suppressed noisy OpenTelemetry logs.** Default log filter now includes
  `opentelemetry=warn,opentelemetry_sdk=warn` to hide `MeterProvider`
  INFO messages.

- **Distinct server startup log messages.** Each server now logs its own
  identity ("Starting HTTP server", "Starting MCP server", "Starting AI
  gateway server") with port and path instead of all saying "Starting
  HTTP server".

- **Subscriber cancellation no longer hangs reconciler tear-down.** The
  salesforce pubsub, NATS jetstream, and `generate` (cron) subscribers
  spawned their reconnect loop into a detached `tokio::spawn` whose
  `JoinHandle` was dropped on the floor. When the hot-reload reconciler
  cancelled a flow's token, the running task was orphaned and kept
  hammering the upstream — the 30s deregistration timeout would always
  fire and stale subscribers would continue logging errors long after
  the flow's cache entry was deleted. `run()` now drives the loop
  directly and wraps every `await` (init, handle, backoff sleeps) in
  `tokio::select!` with the cancellation token so cancellation
  interrupts mid-backoff instead of waiting out the full delay.

- **Log message style consistency.** Stripped trailing periods from
  ~40 log messages across the codebase and converted internal
  `". Skipping."`-style sentence breaks into single-sentence-with-comma
  form, matching the convention "sentence (no period), structured
  fields after". `...` ellipses on shutdown messages are preserved.

### Docs

- **Inline vs external scripts.** Added guidance on when to keep Rhai
  inline in YAML versus moving it to a `resources/scripts/*.rhai` file,
  with the recommended project layout, a link to the official Rhai
  Visual Studio Code extension, and a new `script_fan_in_join.yaml`
  example showing a fan-in DAG joined by an external script. The example
  runs against the dummy CSV data shipped in the repository.
## 0.119.0

### Features

- **`Sforce-Duplicate-Rule-Header: allowSave=true` support on REST
  and Composite operations.** New `allow_duplicate_save: bool` field
  (default `false`) on both `salesforce_restapi_sobject` and
  `salesforce_restapi_composite` task configs. When set, the
  underlying request carries the header so Salesforce accepts the
  record even when a duplicate detection rule would normally block
  it. Salesforce ships "Block" as the default action on
  Account / Lead / Contact duplicate rules, so without this flag
  composite create / update / upsert silently no-op on duplicates
  — a tenant report flagged exactly this. Applies to `create`,
  `update`, `upsert`, and `tree` on composite; `create`, `update`,
  and `upsert` on sobject.

### Dependencies

- **salesforce_core 0.17.0.** Migrated to the new request-builder
  API: every REST and Composite operation now returns a builder
  that accepts per-call headers via `.header(name, value)` and is
  dispatched with `.send().await`. Replaces the previous flat
  `client.create(...).await` style. The migration was mechanical
  on flowgen's side (14 call sites in `restapi/sobject.rs` and
  `restapi/composite.rs`); the upstream change unblocks any
  Salesforce request header (`Sforce-Auto-Assign`,
  `Sforce-Call-Options`, `Sforce-Mru`, …), of which
  `Sforce-Duplicate-Rule-Header` is the first one exposed through
  the YAML config.

## 0.118.1

### Fixes

- **`quinn-proto` upgraded to 0.11.15 (RUSTSEC-2026-0185, severity
  high 7.5).** Transitive `quinn-proto 0.11.14` is vulnerable to
  remote memory exhaustion from unbounded out-of-order stream
  reassembly. Pulled in by every HTTP client in the workspace via
  `reqwest` (Salesforce, BigQuery, MCP, OCI, AI gateway, NATS,
  git-sync). `cargo update -p quinn-proto` only; no API change.

### Tests

- **Reproducer tests for the `"None"` / `"String(None)"` field
  poisoning report.** Internal tenants reported `Email__c` and other
  varchar fields landing on Salesforce composite as the literal
  string `"None"` and on MSSQL as `"String(None)"`. Three new tests
  (`rhai_unit_in_map_must_serialize_as_json_null`,
  `rhai_unit_string_coercions_are_diagnostic`,
  `null_value_templating_does_not_produce_none_strings`) verify that
  flowgen's serialization layer round-trips `()` → `Value::Null`
  cleanly and that Handlebars renders null as `""` rather than
  `"None"`. The strings therefore originate either in the
  tenant-authored Rhai script (a `to_string()` / template
  interpolation on `()`) or in pre-flowgen ETL writes that the SF→MSSQL
  sync now keeps round-tripping. Fix is on the tenant side: audit
  the Rhai script and `UPDATE ... SET col = NULL WHERE col IN
  ('String(None)', 'None')` for the poisoned varchar columns.

## 0.118.0

### Features

- **Shared connection pooling via ClientRegistry.** Tasks with identical
  credentials now automatically share the same authenticated client.
  The runtime hashes credential fields into a `ClientKey` and deduplicates
  at the worker level — no configuration needed. The first task to
  initialise performs the actual authentication; all others wait on a
  per-key mutex and reuse the result. Supported across Salesforce (REST,
  SOAP, Bulk, Tooling, PubSub), BigQuery (query, job, storage_read,
  storage_write), MSSQL, NATS (subscriber, publisher, kv_store), and
  Object Store (read, write, list, move).

### Dependencies

- **salesforce_core 0.15.0.** Fixes a bug where `reconnect()` replaced
  the `token_state` Arc instead of updating in-place, causing stale
  tokens for cloned clients. Shared Salesforce clients now see refreshed
  tokens immediately after reconnect.

## 0.117.0

### Features

- **Startup jitter for all background tasks.** Every background task now
  sleeps a random duration (up to `initial_backoff`) before its first
  initialization attempt. This staggers connections across flows and
  replicas to avoid thundering-herd login storms on external services.
  Blocking tasks (`http_webhook`, `mcp_tool`, `ai_gateway`) and
  `generate` tasks skip the jitter. The jitter is automatic and requires
  no configuration.

- **Exponential reconnect backoff for subscribers.** NATS JetStream and
  Salesforce PubSub subscribers now use exponential backoff (capped at
  5 minutes) when reconnecting after connection loss, replacing the
  previous flat `initial_backoff` sleep. Each successful reconnect resets
  the backoff.

- **AGENTS.md.** Committed agent instructions (code quality standards and
  architecture reference) to the repository root so they are shared
  across all AI coding tools.

## 0.116.0

### Features

- **Object store recursive listing.** New `recursive: true` option on
  `object_store` list operations traverses nested subdirectories instead of
  listing only the immediate prefix level. Enables `list → iterate → read`
  patterns on directory trees of unknown depth.

- **Pipeline tracing context.** `gcp_bigquery_query` now logs `num_rows`,
  `iterate` logs `array_length`, and `nats_jetstream_publisher` logs `subject`
  and `sequence` on each event — making it straightforward to trace row counts
  through a fan-out pipeline.

### Fixes

- **S3 virtual-hosted style requests by default.** S3 requests now use
  virtual-hosted style URLs (`{bucket}.s3.{region}.amazonaws.com`)
  matching the AWS SDK default. Previously path-style URLs were used,
  which can fail against bucket policies or VPC endpoint policies that
  expect virtual-hosted requests. Override with
  `aws_virtual_hosted_style_request: "false"` in `client_options`.

- **All processors now await spawned tasks on channel close.**
  Previously most processors used fire-and-forget `tokio::spawn` without
  collecting handles. All processors now collect `JoinHandle`s and await
  them when the input channel closes or cancellation is signalled,
  matching the pattern already used by `nats_jetstream_publisher`.

## 0.115.0

### Features

- **Salesforce SOAP API merge.** New `salesforce_soapapi_merge` task merges
  duplicate SObject records (Account, Contact, Lead, Individual) into a single
  master record via the Salesforce SOAP API. Related records are automatically
  reparented to the master. Supports field overrides and duplicate detection bypass.

### Fixes

- **BigQuery Storage Write: silent data loss on type mismatch.**
  `encode_scalar_value` now returns typed errors (`FieldTypeMismatch`,
  `FieldBase64Decode`) instead of silently skipping fields when JSON
  values don't match the expected BigQuery column type. Previously a
  string in an INT64 column was quietly dropped.

- **BigQuery Storage Write: nested RECORD/STRUCT support.**
  `build_proto_descriptor` and `json_to_proto_bytes` now recursively
  handle nested RECORD/STRUCT fields. Previously `nested_type: vec![]`
  was hardcoded, silently dropping all data in RECORD columns.

### Dependencies

- Upgraded `salesforce_core` from 0.13.6 to 0.14.0 (adds `soapapi::Client::merge()`).

## 0.114.0

### Features

- **`timestamp_format` Rhai function.** Format any Unix timestamp with a
  custom format string: `timestamp_format(timestamp_now(), "%Y-%m-%d %H:%M:%S")`.
  Uses [chrono strftime](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) syntax.

### Docs

- Added concrete output format tables to all 24 task documentation pages
  with links to underlying crate docs (arrow, serde_json, salesforce_core,
  async-nats, rhai, object_store, reqwest, tiberius).
- Added retry configuration links to all task configuration tables.
- Added `identify()` to PostHog integration so newsletter signups create
  persons instead of anonymous UUIDs.

### CI

- Fixed release notes shell interpolation bug — commit messages with
  backticks or special characters no longer break `gh release create`.
  Uses `--notes-file` instead of inline `--notes`.
- Added docs CI pipeline (`docs-ci.yml`) — runs `svelte-check` and build
  on PRs that touch `docs/`.

## 0.113.0

### Fixes

- **S3 credential chain now works out of the box.** The object store
  client for S3 now uses `AmazonS3Builder::from_env()`, which picks up
  IRSA (web identity token), EKS Pod Identity, ECS task roles, instance
  profiles, and environment variable credentials automatically. Previously
  `parse_url_opts` used `AmazonS3Builder::new()` which skipped the
  environment credential chain entirely, causing timeouts when IMDS was
  unreachable.

### Docs

- Fixed blank page on initial load caused by DOM manipulation breaking
  Svelte hydration. Tables use CSS-only overflow, copy buttons are
  appended without reparenting nodes.
- Fixed escaped underscores in installation table (`x86\_64`).

## 0.112.0

### Features

- **Application Default Credentials for GCP.** The `credentials_path`
  field is now optional on all BigQuery processors (`gcp_bigquery_query`,
  `gcp_bigquery_job`, `gcp_bigquery_storage_read`,
  `gcp_bigquery_storage_write`). When omitted, credentials are resolved
  via the Application Default Credentials chain:
  `GOOGLE_APPLICATION_CREDENTIALS_JSON` env, `GOOGLE_APPLICATION_CREDENTIALS`
  env, or `~/.config/gcloud/application_default_credentials.json`
  (written by `gcloud auth application-default login`).

## 0.111.0

### Features

- **Salesforce SOSL search task.** New `salesforce_restapi_search` task
  type for executing SOSL queries across multiple Salesforce objects.
  Supports Handlebars templating in the query string.

### Fixes

- **Default log level is now `info`.** When `RUST_LOG` is not set,
  flowgen defaults to `info` instead of `error`. New users no longer
  see a silent startup.
- Fixed "Loaded 1 flows" pluralization — now correctly says "Loaded 1 flow".

### Docs

- **Salesforce guides.** Added four end-to-end guides under Salesforce:
  CDC Replication, Data Export (Bulk API), Data Activation (Composite +
  Platform Events), and REST API (SObject, SOSL, MCP tools).
- Guides are nested under the Salesforce sidebar section with a
  subsection header.
- Added `protoc` as a build prerequisite in the installation docs.
- Mobile-friendly tables with horizontal scroll.
- Footer now matches connve.com (newsletter signup, GitHub, Helm icons).
- Mermaid diagram component for architecture visualizations in guides.

### Examples

- Reorganized Salesforce examples under `examples/salesforce/` with
  subdirectories per guide: `cdc-replication/`, `data-export/`,
  `data-activation/`, `rest-api/`.
- Removed old flat `examples/salesforce/` example files.

## 0.110.0

### Fixes

- macOS release binaries now cross-compile from a single `macos-latest`
  ARM runner instead of using the slow `macos-13` Intel runner.

## 0.109.0

### Fixes

- Fixed Docker `ENTRYPOINT` for distroless image — resolves binary
  extraction failure in release workflow and `docker run` without
  explicit command.

## 0.108.0

### Infrastructure

- **Native multi-arch Docker builds.** Split Docker build into parallel
  native runners (`ubuntu-latest` for amd64, `ubuntu-24.04-arm` for arm64)
  with digest-based merge into a single multi-arch manifest. Eliminates
  QEMU emulation overhead.
- **Pre-built binary extraction from Docker.** Linux release binaries are
  now extracted directly from the Docker image, guaranteeing the released
  binary is identical to what runs in containers.
- Fixed Helm chart install command in docs to use `helm.connve.com`.
- Docs: mobile hamburger menu with drawer navigation, matching connve.com
  navbar style.
- Added `ENTRYPOINT` to Dockerfile for distroless image compatibility.

## 0.107.0

### Features

- **Amazon S3 support for object store.** Read, write, list, and move
  operations now work with `s3://` paths. Credentials can be provided via
  a JSON file (`credentials_path`), inline `client_options`, or
  automatically from environment variables and IAM roles.

### Infrastructure

- **Pre-built binaries on GitHub Releases.** Each release now publishes
  binaries for Linux (amd64, arm64) and macOS (Intel, Apple Silicon).
  Linux binaries are extracted from the Docker image; macOS binaries
  are built natively.
- **Multi-arch Docker images.** Docker images now include both
  `linux/amd64` and `linux/arm64` platforms, built natively in parallel
  (no QEMU emulation) and merged into a single manifest.
- Replaced deprecated `actions/create-release@v1` with `gh release create`.

## Docs 0.2.0

- Connve branding: favicon, DaisyUI theme matching connve.com, full-width
  footer with company info and social links.
- Route structure flattened for `/docs/flowgen` base path, preparing for
  `connve.com/docs/flowgen/...` subpath hosting.
- Removed GitHub App token from docs CI workflow (uses default `GITHUB_TOKEN`).

## Chart 0.16.1

### Fixes

- WireGuard gateway: remove pod-level `sysctls` that cause
  `SysctlForbidden` on clusters with restricted pod security policies.
  The required sysctls (`net.ipv4.ip_forward`, `net.ipv4.conf.all.src_valid_mark`)
  are now set via the init container using `NET_ADMIN` capability instead.

## Chart 0.16.0

### Infrastructure

- **WireGuard gateway** replaces the WireGuard sidecar in the Helm
  chart. A standalone pod holds a single VPN identity and forwards
  ports via socat; flowgen pods connect through the gateway's
  ClusterIP Service. Fixes intermittent timeouts caused by multiple
  replicas sharing the same WireGuard private key and IP, which made
  the VPN server flap handshakes between pods. The `flowgen.wireguard`
  sidecar section is removed and replaced by the top-level
  `wireguardGateway` section with tunnel-based configuration.

## 0.106.0

### Infrastructure

- HTTP server `/healthz` endpoint for Kubernetes readiness and
  liveness probes. Pods that crash or haven't bound the listener
  are removed from the Service endpoints, preventing traffic from
  being routed to unhealthy replicas.

## 0.105.0

### Fixes

- `salesforce_bulkapi_query_job` no longer buffers all parsed Arrow
  RecordBatches in memory before emitting. Batches are now streamed
  one at a time via the centralized `FromReader` iterator, keeping
  peak memory to CSV bytes + one batch instead of CSV bytes + all
  batches. Fixes OOM kills on large Salesforce Bulk API exports
  (600k+ rows).

- `object_store` read now streams all multi-record formats (CSV,
  Parquet, Avro) one item at a time instead of collecting everything
  in memory before emitting. Uses the same `FromReader` iterator.

### Internal

- `FromReader` trait returns a lazy `ReaderIter` (boxed iterator)
  instead of `Vec`. Format-specific parsing (CSV, Parquet, Avro, JSON)
  stays centralized in `flowgen_core::buffer`; callers drive the
  iterator and can perform async sends between items. No call site
  collects all items in memory.

- `salesforce_bulkapi_query_job` completion handling extracted into
  `send_final_event` helper, removing five duplicated completion_tx
  wiring blocks across create, get, delete, abort, and get_results
  operations.

## 0.104.0

### Breaking

- **`event_buffer_size` default lowered from 10,000,000 to 10,000.**
  The previous 10M default dated from an earlier single-channel architecture
  and effectively disabled backpressure, allowing flows to consume
  unbounded memory under load. The new default caps each inter-task channel
  at 10,000 events; when full the upstream task blocks until the downstream
  task drains a slot. No events are dropped. Flows that previously relied
  on the large buffer as a shock absorber will now apply backpressure
  sooner, which bounds memory at the cost of brief producer stalls during
  bursts. Override per-worker via `worker.event_buffer_size` in the config
  file if your workload requires a larger buffer.

### Tasks

- `http_request` supports a new `response_type` field (`json` | `bytes` |
  `text`, default `json`). Set `response_type: bytes` to download binary
  payloads (archives, images, etc.) as `EventData::Bytes`; set
  `response_type: text` to capture plain-text responses as a JSON string.

- New `EventData::Bytes` variant for binary payloads. Tasks that need raw
  bytes pattern-match on the variant directly. The JSON projection (used
  by templating and logging) returns `{"$bytes": true, "len": N}` — the
  actual payload is never inlined into JSON.

- `object_store` write accepts `EventData::Bytes` input and writes raw
  bytes. Auto-detection maps bytes to `format: bytes` with a `.bin`
  extension; explicit `format: bytes` is also supported.

## 0.103.0

### Tasks

- `http_request` now transparently decompresses gzip-, brotli-, and
  deflate-encoded responses (`Content-Encoding: gzip`, `br`, `deflate`).
  The client sends `Accept-Encoding: gzip, br, deflate` on every outbound
  request; servers that do not compress ignore the header. Previously the
  client read raw compressed bytes as text and silently corrupted any
  response a server compressed. No config change.

## 0.102.0

### Breaking

- **Top-level config key `flow_resources` is `resources`** as of
  0.101.0. The field was first introduced as `flow_resources` (PR
  #146) and renamed to `resources` (PR #147) without a release
  note, which silently broke any tenant whose `config.yaml` still
  used the older name — resource-loading tasks (`bulkapi`, `mssql`,
  `gcp_bigquery_*`) fail with "Resource path is not configured".
  Tenants on `flow_resources:` must rename it to `resources:` in
  their inline config and any external config ConfigMaps. No
  backward-compatibility alias is provided.

### Tasks

- `iterate` now accepts `EventData::ArrowRecordBatch` and
  `EventData::Avro` inputs by converting them to a JSON array of
  row objects internally before walking the rows. Previously the
  task rejected non-JSON inputs with `ExpectedJsonGotArrowRecordBatch`
  / `ExpectedJsonGotAvro`, breaking ETL chains where an upstream
  task (BigQuery storage read, MSSQL query) emits Arrow and a
  downstream `iterate` walks the result set. The two old error
  variants are removed; conversion failures surface via the new
  `EventConversion` variant.

### Configuration

- `script::config::RhaiLimits` defaults raised: `max_map_size` and
  `max_array_size` go from `100_000` to `1_000_000`. The previous
  defaults flatten nested entries, so a script processing N rows ×
  ~20 fields hits the cap at N ≈ 5k — well within normal ETL batch
  sizes. Operators on tight memory budgets can override via the
  `limits` block on individual `script` tasks; the per-script
  override semantics are unchanged.

### Internal

- New unit test `test_rhai_string_suffix_strip_lowercase` covering
  the `split` + `ends_with` + `len` + `sub_string` + `to_lower`
  pattern used by Salesforce CDC routing scripts. Locks the
  contract so a Rhai dependency bump that changes string-op
  semantics fails fast in CI.

## 0.101.0

### Highlights

- New `git_sync` task: clones a Git repository and emits one event per
  file. Pure-Rust via `gix`; no `git` binary required at runtime.
- New `nats_kv_store` task: `get`, `put`, `list`, `delete` against
  NATS JetStream KV. Bucket auto-created on first use.
- System cache for flow loading: bootstrap a `system_sync_flows` flow
  on disk, let it pull user flows from a Git repo into NATS KV, and
  the worker loads them from the cache on the next start.
- New `validate.rs` in `flowgen_core` with shared name and path
  validators. Flow loaders reject names containing `/`, `\`, `..`,
  empty strings, or non-`[A-Za-z0-9_-]` characters before any flow
  is accepted.

### Security

- **Webhook body limit**: `http_webhook` now enforces
  `max_body_bytes` (default 10 MiB). Oversized requests are rejected
  with HTTP 413 before being read into memory.
- **Outbound HTTP timeouts**: `http_request` now applies `timeout`
  (default 30s) and `connect_timeout` (default 10s) on the reqwest
  client to prevent slow upstreams from pinning worker tasks.
- **Rhai resource limits**: `script` config exposes `limits` with
  `max_operations` (10M), `max_call_depth` (64), `max_string_size`
  (16 MiB), `max_array_size` and `max_map_size` (100k). Caps a
  single misbehaving script from stalling the worker.
- **Rhai env-var isolation**: split `render_template` into the
  env-injecting variant (operator-controlled YAML) and
  `render_template_no_env`. The Rhai-exposed `render(template,
  data)` now uses the no-env variant so flow-author scripts cannot
  read pod environment variables.
- **`clone_path` traversal guard**: operator-supplied `clone_path`
  values containing `..` segments are rejected at task init.

### Tasks

- `http_request`: error chain walking surfaces the underlying cause
  for opaque reqwest builder errors (e.g. URL parse, header value).
- `http_request`: `query_params` payload mode wired up for sending
  pure query parameters.
- `iterate`: `depends_on` semantics revamped; multi-leaf
  acknowledgement via shared `CompletionState`.
- `script`: `render(template, data)` Rhai helper for Handlebars
  rendering against script data (env access excluded — see Security).
- `git_sync`: derived `clone_path` defaults to
  `<temp>/<flow_name>/<task_name>` so multiple `git_sync` tasks in
  one worker do not collide. Override via `clone_path`.

### Configuration

- New typed validation errors in `flowgen_core::validate` (`Error`,
  `NameField`, `PathField`).
- `script::config::RhaiLimits` struct with serde defaults; safe to
  omit, fully overridable per task.
- `http::config::Processor` gains `timeout`, `connect_timeout`, and
  `max_body_bytes` fields, all with defaults.

### Documentation

- New docs pages introducing flowgen, the system cache loading
  story, and `nats_kv_store`.
- Landing page (`/`) redirects to `/docs/getting-started/why-flowgen`.

### Internal

- `flowgen_core::validate` module shared across crates.
- gix wired with `sha1` and `max-performance-safe` features.
- `reqwest` workspace deps include `query`, `gzip`, `brotli`,
  `charset`, `json`, `form`, `rustls`.

### Dependencies

- `salesforce_core` 0.13.4 → 0.13.6. Brings docs.rs build fixes
  and per-API cargo features upstream; we keep all four API modules
  (`restapi`, `bulkapi`, `toolingapi`, `pubsubapi`) enabled.
