# postgresql

PostgreSQL instance backing the OLake charts. A single server holds a database
per consumer — OLake UI, Temporal, and Fusion — recorded as separate keys in one
credentials secret.

This chart is not meant to be installed on its own. It is a dependency of both
[`olake`](../olake/README.md) and [`fusion`](../fusion/README.md), aliased to
`postgresql` in each, so it is configured through `postgresql.*` in whichever
chart you install:

```yaml
postgresql:
  enabled: true
  auth:
    postgresUser: temporal
    postgresPassword: change-me
  persistence:
    size: 8Gi
```

Set `postgresql.enabled: false` to skip it entirely and point the parent at a
database you already run. Each parent keeps its own `postgresql.external` block
for that; those keys are read by the parent's templates and ignored here.

## Names you should not change

Two values are load bearing on an existing install:

- **`serviceName`** (default `postgresql`) names both the StatefulSet and its
  headless Service. The StatefulSet's volume claim is
  `postgresql-storage-<serviceName>-0`, so changing it on a live release strands
  the database volume and comes up with an empty database.
- **`componentLabel`** (default `database`) is part of the StatefulSet's
  selector. Kubernetes treats `spec.selector` as immutable, so changing it makes
  `helm upgrade` fail outright.

The defaults reproduce exactly what the `olake` chart created before this chart
was split out of it, so existing releases upgrade in place. The `fusion` chart
overrides both, letting a standalone Fusion install run its own PostgreSQL
alongside an `olake` install in the same namespace.

## Databases

`databases` maps a secret key to a database name:

```yaml
databases:
  database: temporal
  fusion_database: fusion
```

`primaryDatabaseKey` picks which of them becomes `POSTGRES_DB`, i.e. the
database the server creates when it first initialises. The others are created by
their own consumers.

See [values.yaml](values.yaml) — every key is documented inline.
