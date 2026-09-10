# Salesforce REST API

CRUD operations on Salesforce SObjects, composite batch operations, and SOSL search.

## Output

### SObject CRUD

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [salesforce_core](https://crates.io/crates/salesforce_core) | Salesforce API response. For `create`: includes `id`, `success`, `errors`. For `get` / `get_by_external_id`: the full record object. Record ID is set as `event.id`. |

### Composite

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [salesforce_core](https://crates.io/crates/salesforce_core) | Array of sub-request results, each with `statusCode`, `result`, and `errors`. |

### Search / Query

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [salesforce_core](https://crates.io/crates/salesforce_core) | Salesforce SOSL/SOQL response with `searchRecords` or `records` array. |

## SObject operations

Single-record operations: create, get, get_by_external_id, update, upsert, delete.

```yaml
- salesforce_restapi_sobject:
    name: create_account
    operation: create
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Account
    payload:
      Name: "{{event.data.company_name}}"
      Industry: "Technology"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `create`, `get`, `get_by_external_id`, `update`, `upsert`, `delete`. |
| `credentials_path` | string | required | Path to Salesforce credentials. |
| `sobject_type` | string | required | SObject type (e.g., `Account`, `Contact`). |
| `payload` | object | | Record fields — explicit values or `from_event: true`. |
| `record_id` | string | | Salesforce record ID (for get, update, delete). Supports templating. |
| `fields` | string | | Comma-separated field list (for get). |
| `external_id_field` | string | | External ID field name (for upsert, get_by_external_id). |
| `external_id_value` | string | | External ID value. Supports templating. |
| `allow_duplicate_save` | bool | `false` | Send `Sforce-Duplicate-Rule-Header: allowSave=true` so the request bypasses Salesforce duplicate-detection rules. See [Duplicate-rule override](#duplicate-rule-override). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Examples

**Upsert by external ID:**

```yaml
- salesforce_restapi_sobject:
    name: upsert_contact
    operation: upsert
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Contact
    external_id_field: External_Id__c
    external_id_value: "{{event.data.external_id}}"
    payload:
      FirstName: "{{event.data.first_name}}"
      LastName: "{{event.data.last_name}}"
```

**Get a record:**

```yaml
- salesforce_restapi_sobject:
    name: get_account
    operation: get
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Account
    record_id: "{{event.data.account_id}}"
    fields: "Id,Name,Industry"
```

## Composite operations

Batch operations on multiple records: create, get, update, upsert, delete, tree.

```yaml
- salesforce_restapi_composite:
    name: bulk_create
    operation: create
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Account
    payload:
      from_event: true
```

### Composite fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `create`, `get`, `update`, `upsert`, `delete`, `tree`. |
| `credentials_path` | string | required | Path to Salesforce credentials. |
| `sobject_type` | string | | SObject type. When set, records missing `attributes` are auto-populated with `attributes: { type: sobject_type }`. Records that already include `attributes` are left untouched, so mixed-type batches work unchanged. When unset, each record must include `attributes` explicitly or the task errors with a clear message. |
| `payload` | object | | Records — explicit list or `from_event: true`. |
| `ids` | list | | Record IDs (for get, delete). |
| `fields` | list | | Field list (for get). |
| `external_id_field` | string | | External ID field (for upsert). |
| `all_or_none` | bool | | Atomic transaction — all succeed or all fail. |
| `allow_duplicate_save` | bool | `false` | Send `Sforce-Duplicate-Rule-Header: allowSave=true` so the batch bypasses Salesforce duplicate-detection rules. See [Duplicate-rule override](#duplicate-rule-override). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Auto-populated `attributes`

The Salesforce Composite API requires every record to include an `attributes: { type: "..." }` sub-object. When `sobject_type` is set on the task, flowgen auto-populates it for any record that doesn't already have one, so simple single-type batches don't need boilerplate:

```yaml
- salesforce_restapi_composite:
    name: bulk_create
    operation: create
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Account
    payload:
      from_event: true
```

Each incoming event record needs only its fields — `attributes: { type: "Account" }` is injected automatically.

For mixed-type batches (e.g. Accounts and Contacts in the same request), set `attributes` on each record explicitly. flowgen never overwrites an existing `attributes` key.

## Duplicate-rule override

Salesforce ships duplicate detection rules with a default action of **Block** on standard SObjects (Account, Lead, Contact). When a request tries to create or update a record that matches an existing one on a rule-watched field (e.g. `Email`, `Phone`, `Name`), the call returns a 200 OK but the record is rejected with `DUPLICATES_DETECTED` in the per-record errors array — silently from the caller's perspective.

Set `allow_duplicate_save: true` on the task to attach the [`Sforce-Duplicate-Rule-Header: allowSave=true`](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/headers_duplicaterulesheader.htm) header to the request. Salesforce will then save the record and surface a duplicate warning instead of blocking the write.

```yaml
- salesforce_restapi_composite:
    name: upsert_contacts
    operation: upsert
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Contact
    external_id_field: ExternalId__c
    all_or_none: true
    allow_duplicate_save: true
    payload:
      from_event: true
```

The flag applies to write operations only:

- `salesforce_restapi_sobject`: `create`, `update`, `upsert`
- `salesforce_restapi_composite`: `create`, `update`, `upsert`, `tree`

It is a no-op on `get`, `get_by_external_id`, and `delete`.

## SOSL Search

Execute [SOSL](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_sosl.htm) queries to search across multiple objects simultaneously. Returns matching records as a JSON event.

```yaml
- salesforce_restapi_search:
    name: find_accounts
    credentials_path: /etc/salesforce/credentials.json
    query: "FIND {Acme} IN ALL FIELDS RETURNING Account(Id, Name, Industry)"
```

### Search fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to Salesforce credentials. |
| `query` | string | required | SOSL query string. Supports templating. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Templating the search term

SOSL wraps the search term in curly braces (`FIND {term} ...`). Since Handlebars only treats double `{{` as special, a single `{` is a literal character. Wrap the templated term in double quotes to make it a phrase search, then use single-quoted YAML so the inner `"` doesn't need escaping:

```yaml
- salesforce_restapi_search:
    name: dynamic_search
    credentials_path: /etc/salesforce/credentials.json
    query: 'FIND {"{{event.data.search_term}}"} IN ALL FIELDS RETURNING Account(Id, Name), Contact(Id, FirstName, LastName, Email)'
```

Inside the `{"..."}` phrase, the processor auto-escapes SOSL reserved characters (`- ? & | ! { } [ ] ( ) ^ ~ * : \ " ' +`) before sending the query, so search terms with hyphens, spaces, or other reserved characters work without manual escaping.

### Prefix matching

Phrase search matches whole tokens. To match terms that *start with* the search input (e.g. `test` should also find `test-rmp` and `Test Account 1`), append `*` inside the phrase:

```yaml
query: 'FIND {"{{event.data.search_term}}*"} IN ALL FIELDS RETURNING Account(Id, Name)'
```

### Examples

**Search with specific scope and field limits:**

```yaml
- salesforce_restapi_search:
    name: search_name_fields
    credentials_path: /etc/salesforce/credentials.json
    query: "FIND {Acme} IN NAME FIELDS RETURNING Account(Id, Name, Industry LIMIT 10), Contact(Id, FirstName, LastName LIMIT 5)"
```

**Webhook-triggered search:**

```yaml
flow:
  tasks:
    - http_endpoint:
        name: trigger
        method: POST
        endpoint: /search

    - salesforce_restapi_search:
        name: sosl_search
        credentials_path: $SALESFORCE_CREDENTIALS_PATH
        query: 'FIND {"{{event.data.term}}"} IN ALL FIELDS RETURNING Account(Id, Name), Contact(Id, Email)'

    - log:
        name: log_results
        level: info
```

