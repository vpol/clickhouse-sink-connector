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

For this branch, `patched/2.9.1` was created from upstream tag `2.9.1` and
cherry-picked the compatibility commit from `patched/2.7.1`:

```sh
git cherry-pick c865682c
```

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
