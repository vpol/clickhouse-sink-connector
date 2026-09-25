# Local Patch Context

This repository is a fork of `Altinity/clickhouse-sink-connector`.
For each upstream release that needs ClickHouse Cloud compatibility, create a
local branch named `patched/<version>` from the matching upstream tag, then
apply the compatibility patch on top.

## Current Patches

### SharedReplacingMergeTree

This compatibility patch teaches `DBMetadata` to recognize
`SharedReplacingMergeTree`, which ClickHouse Cloud can return for replacing
table engines.

Touched file:

- `sink-connector/src/main/java/com/altinity/clickhouse/sink/connector/db/DBMetadata.java`

Patch shape:

- Add `TABLE_ENGINE.SHARED_REPLACING_MERGE_TREE`.
- Add the `SharedReplacingMergeTree(` version-column prefix.
- Parse `SharedReplacingMergeTree` parameters the same way the existing
  replicated replacing engine parser handles path, replica, version, and
  optional delete-marker parameters.
- Detect `SharedReplacingMergeTree` before the broader
  `ReplacingMergeTree` match.
- Treat `SharedReplacingMergeTree` as a replacing engine in the writer path so
  generated `_version` and `is_deleted` values are bound during inserts.
- Re-run replacing-engine column configuration after metadata refreshes, and
  create prepared statement executors only after table metadata is available.
- Keep binding the replacing delete column to the not-deleted value for
  non-delete rows, even when `ignore_delete=true`.

### Ignore delete payloads before JDBC binding

With `ignore_delete=true`, skip DELETE payload binding and all delete writes
in `PreparedStatementExecutor`. PostgreSQL `REPLICA IDENTITY DEFAULT` supplies
only key values on deletes, so binding an absent non-key UUID can fail even
when the delete marker would be set to the not-deleted value. Keep these events
in the original batch and update block metadata so the normal ordered offset
commit path still acknowledges them after the batch succeeds. INSERT, UPDATE,
TRUNCATE, and `ignore_delete=false` keep their existing behavior.

### ClickHouse Cloud JDBC Settings

This compatibility patch prevents startup failures on ClickHouse Cloud where
the server rejects client attempts to change `allow_experimental_object_type`.

Touched files:

- `sink-connector/src/main/java/com/altinity/clickhouse/sink/connector/db/BaseDbWriter.java`
- `sink-connector/src/main/java/com/altinity/clickhouse/sink/connector/db/HikariDbSource.java`
- `sink-connector/src/test/java/com/altinity/clickhouse/sink/connector/db/BaseDbWriterTest.java`
- `doc/configuration.md`
- `sink-connector-lightweight/docker/config.yml`
- `sink-connector-lightweight/docker/config_local.yml`
- `sink-connector-lightweight/helm/sink-connector-lightweight/templates/configmap.yaml`

Patch shape:

- Remove `allow_experimental_object_type=1` from default custom settings.
- Strip `allow_experimental_object_type` from configured
  `clickhouse.jdbc.settings` so existing copied configs remain Cloud-safe.
- Keep `insert_allow_materialized_columns=1` in default custom settings.
- Throw a clear connection initialization error instead of returning `null`
  and failing later with a misleading `NullPointerException`.

### Debezium JDBC Storage Config Keys

Debezium 3.x renamed JDBC storage table properties. Config files should use
the new names directly:

- `offset.storage.jdbc.offset.table.*` -> `offset.storage.jdbc.table.*`
- `schema.history.internal.jdbc.schema.history.table.*` ->
  `schema.history.internal.jdbc.table.*`
- `schema.history.internal.table.*` ->
  `schema.history.internal.jdbc.table.*`

For deployed configs, update `/config/config.yml` before starting the
connector. The patched branch only updates bundled examples/templates and does
not add runtime alias translation.

## Refresh Workflow

```sh
git fetch upstream --tags
git switch -c patched/<version> <version>
git cherry-pick <previous-shared-replacing-merge-tree-commit>
```

### Port to 2.10.3

`patched/2.10.3` retained its existing base `3c0759b1f`, including the
post-2.10.3 upstream snapshot-heartbeat fix. All 12 commits in
`2.9.1..patched/2.9.1` were ported in order, with their source hashes recorded
in the cherry-pick commit messages:

| Source | Patch | Adaptation |
| --- | --- | --- |
| `ac9fb8814` | SharedReplacingMergeTree metadata | Applied |
| `f5a457986` | Local patch documentation | Updated for this port |
| `c79c8cac6` | Cloud-safe JDBC settings | Retained upstream V2 property filtering, connection recovery, and pool isolation |
| `d9a2e1031` | Debezium 3 storage keys | Retained upstream offset-table ordering |
| `866d3463c` | Shared replacing writes | Applied common engine predicate |
| `c6921717d` | Metadata refresh before writes | Retained sorting-key refresh and supplier when moving executor creation |
| `5b7755bff` | Delete-marker binding with ignored deletes | Applied |
| `709858d13` | Bounded deduplicator memory | Applied |
| `7dec1ffb4` | Release written batches awaiting commit | Retained upstream handoff accounting and commit serialization |
| `556213aee` | Avoid per-row schema reload; close JDBC resources | Retained exact system.columns queries and SQL retry classification |
| `ac4bacc62` | Non-ArrayList array values | Collection support was upstream; retained Object[] support and tests |
| `5e9121a4d` | Honor Hikari minimum idle | Applied |

The new upstream sorting-key UPDATE path also uses the shared replacing-engine
predicate, so SharedReplacingMergeTree emits the old-key tombstone when an
UPDATE moves a row. Regression coverage verifies both changed and unchanged
sorting keys.

The ClickHouse 26.4 harness additionally reproduced missing schema evolution
on PostgreSQL UPDATE after ADD COLUMN. The existing fix `a3c397bea` from
`codex/postgres-update-schema-evolution` was ported, retaining the missing-column
guard from `556213aee` and updating the writer's cached column map in place.

Both module POMs already select ClickHouse JDBC **0.9.8** in the target base.
The default is the V2 implementation; `clickhouse.jdbc.v1=true` explicitly opts
into the legacy implementation bundled inside 0.9.8. No 0.6.5 dependency is
required by the port.

To inspect the local delta for a patched branch:

```sh
git diff <version>..patched/<version>
```

## Build Verification

The project needs JDK 17 for compilation. If Maven picks up an older
`JAVA_HOME`, run verification with the local Homebrew JDK 17 path:

```sh
JAVA_HOME=/opt/homebrew/Cellar/openjdk@17/17.0.16/libexec/openjdk.jdk/Contents/Home ./mvnw -pl sink-connector -DskipTests compile
```

If Homebrew has since upgraded JDK 17, the `openjdk@17` symlink can be used
instead:

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./mvnw -pl sink-connector -DskipTests compile
```
