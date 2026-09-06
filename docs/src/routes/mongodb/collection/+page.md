# MongoDB Collection

Reads, writes, or upserts documents in a MongoDB collection, depending on `operation`.

## Configuration

```yaml
- mongodb_collection:
    name: read_customers
    operation: read
    credentials_path: /etc/mongodb/credentials.json
    db_name: sales
    collection_name: customers
    filter:
      status: "active"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `read`, `write`, or `upsert`. |
| `credentials_path` | string | | Path to MongoDB credentials file. Omit to connect to `localhost:27017` without authentication. See [Credentials](/docs/flowgen/mongodb#credentials). |
| `db_name` | string | required | Database name. |
| `collection_name` | string | required | Collection name. |
| `filter` | map | | Key-value pairs to filter documents. Used by `operation: read` and `operation: upsert`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Examples

**Read with a filter:**

```yaml
- mongodb_collection:
    name: read_customers
    operation: read
    credentials_path: /etc/mongodb/credentials.json
    db_name: sales
    collection_name: customers
    filter:
      status: "active"
```

**Write the incoming event as a document:**

```yaml
- mongodb_collection:
    name: write_customer
    operation: write
    credentials_path: /etc/mongodb/credentials.json
    db_name: sales
    collection_name: customers
```

**Upsert the first document matching `filter`:**

The incoming event's JSON payload is applied to the first document matching `filter`: a plain document is wrapped in `$set` (its `_id`, if any, is moved to `$setOnInsert` so it is only set on insert), while a document whose keys are all update operators (e.g. `$set`, `$inc`) is applied verbatim. If nothing matches `filter`, a new document is inserted (upsert). The resulting document is emitted downstream. See the [MongoDB update operators](https://www.mongodb.com/docs/manual/reference/operator/update/) reference and the [Rust driver `find_one_and_update` options](https://docs.rs/mongodb/latest/mongodb/options/struct.FindOneAndUpdateOptions.html).

```yaml
- mongodb_collection:
    name: upsert_customers
    operation: upsert
    credentials_path: /etc/mongodb/credentials.json
    db_name: sales
    collection_name: customers
    filter:
      email: "ada@example.com"
```

```json
{ "_id": "696a1d842f9c12344cd86eab", "name": "Sayan updated", "status": "active" }
```

Or, to pass update operators through verbatim:

```json
{ "$set": { "name": "Ada Lovelace", "status": "active" } }
```

See [Credentials](/docs/flowgen/mongodb#credentials) for the credentials file format.

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [mongodb](https://docs.rs/mongodb/latest/mongodb/) | `read`: each matching document, converted to JSON, `event.id` set to the document's `_id`. `write`: the insert result with the generated `ObjectId`, `event.id` set to the inserted document's `_id`. `upsert`: the resulting document after the update/insert (`return_document: After`), `event.id` set to the document's `_id`. |
