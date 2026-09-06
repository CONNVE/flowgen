# MongoDB

Flowgen reads, writes, and upserts MongoDB documents, and watches change streams for real-time updates.

- [Collection](/docs/flowgen/mongodb/collection) — reads, writes, or upserts documents in a collection.
- [Change Stream](/docs/flowgen/mongodb/change_stream) — watches a database for real-time document changes.

## Credentials

`credentials_path` is optional and points to a JSON file with structured connection details. Every field is optional: `scheme` defaults to `mongodb`, `host` to `localhost`, `port` to `27017`. Omitting `credentials_path` entirely connects to `localhost:27017` without authentication.

```json
{
  "host": "mongo.example.com",
  "port": 27017,
  "username": "user",
  "password": "pass",
  "options": { "authSource": "admin", "replicaSet": "rs0" }
}
```

MongoDB Atlas connection strings use the `mongodb+srv://` scheme, where DNS resolves the actual cluster hosts and port — set `scheme` and omit `port`:

```json
{
  "scheme": "mongodb+srv",
  "host": "cluster0.abcde.mongodb.net",
  "username": "user",
  "password": "pass"
}
```

A self-hosted replica set has no SRV record, so it needs an explicit seed list instead of a single host — `host` also accepts an array, with `port` as the fallback for any entry that omits its own:

```json
{
  "host": ["mongo-0:27017", "mongo-1:27017", "mongo-2:27017"],
  "username": "user",
  "password": "pass",
  "options": { "replicaSet": "rs0" }
}
```

`options` is a map of arbitrary connection string query parameters (`authSource`, `tls`, `replicaSet`, `retryWrites`, `w`, ...), passed through verbatim.
