# Lakeflow Apis Community Connector

This documentation provides setup instructions and reference information for the **Apis** source connector, which ingests industrial/IoT historian data from the **Apis REST Service** into Databricks.

The Apis REST Service is the HTTP front end of a "Hive" — an industrial data historian / SCADA-style runtime (the API in this connector matches Prediktor's *Apis Foundation* Hive platform). A Hive **instance** hosts pluggable **modules**, each module exposes **items** (points/tags), and each item has a **current value** and — when the item is configured for logging — **recorded history**.

The connector reads three tables from a Hive instance: the item catalog (`items`), current values (`values`), and historical/aggregated values (`timeseries`).

> **Validation status**: this connector has not yet been validated against a live Apis deployment. Several response shapes are inferred from the source's OpenAPI specification rather than declared by it. See [Known Limitations](#known-limitations) before using it in production.

## Prerequisites

- **A reachable Apis REST Service endpoint**: the root URL of the service in front of your Hive, for example `http://hive-host:8080` or `https://hive-host:443`. Apis is a self-hosted / on-premises product, so this is a customer-specific host and port — there is no vendor-hosted cloud endpoint.
- **A bearer token for the service**: the API requires an `Authorization` header on *every* request. The token is issued by your Apis administrator out of band; the API itself defines no login, token, or refresh endpoint, so the token is a static, pre-issued credential.
- **A running Hive instance**: the instance must be running for module/item/value endpoints to return data. If you do not know your instance name, the connector can discover it, but you must at least be authorized to list instances.
- **Items configured for logging** (only for the `timeseries` table): historical values exist only for items that the Hive has been configured to log into its time-series store. An item appearing in `items` does not guarantee it has history.
- **Network access**: the Databricks compute running the pipeline must be able to reach the Apis host and port. Because Apis is typically deployed on a private/plant network, this usually requires network connectivity (for example, a serverless network policy, VPC peering, or a private link) to the historian's network.
- **Lakeflow / Databricks environment**: a workspace where you can create a Unity Catalog connection for a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when creating the Unity Catalog connection.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `base_url` | string | yes | Root URL of the Apis REST Service (scheme, host, and port; no trailing path). Accepted aliases: `host`, `url`. | `http://hive-host:8080` |
| `token` | string | yes | Pre-issued bearer token sent in the `Authorization` header. Accepted aliases: `api_token`, `access_token`, `bearer_token`. | `eyJhbGciOi...` |
| `auth_scheme` | string | no | Prefix used for the `Authorization` header value. Defaults to `Bearer`, which sends `Authorization: Bearer <token>`. Set it to an empty string (or `none` / `raw`) to send the bare token with no prefix. | `Bearer` |
| `instance` | string | no | Comma-separated Hive instance name(s) to read from. When omitted, the connector discovers the available instance names from the service. Accepted alias: `instances`. Can be overridden per table. | `ApisHive` |
| `request_timeout` | string | no | Per-request timeout in seconds. Defaults to `30`. | `60` |
| `max_retries` | string | no | Attempts per request before failing. Defaults to `4`, with exponential backoff on transient failures (`429`, `500`, `502`, `503`, `504`). | `6` |
| `verify_ssl` | string | no | Set to `false` to skip TLS certificate verification. Defaults to `true`. On-premises Hive deployments often present self-signed certificates; prefer installing the certificate over disabling verification. | `false` |
| `externalOptionsAllowList` | string | **yes** | Comma-separated list of table-specific option names allowed to pass through to the connector. This connector relies on table-specific options, so this parameter is required. | see the definitive list below |

The full, definitive value for `externalOptionsAllowList` is:

```
instance,instances,item,items,item_patterns,attrib,attribs,quality,aggregate,interval,start_timestamp,backfill_days,window_seconds,max_partitions,max_records_per_batch,max_windows_per_read
```

> **Note**: the options above (`item`, `quality`, `aggregate`, `window_seconds`, and so on) are **not** connection parameters. They are supplied per table under `table_configuration` in the pipeline spec, and their names must appear in `externalOptionsAllowList` for the connection to forward them.

### Obtaining the Required Parameters

This connector uses a simple bearer token — there is no OAuth app to register, no redirect URI, no scopes, and no browser sign-in.

1. **Find the service root URL (`base_url`)**
   - Ask your Apis / plant IT administrator for the host and port where the Apis REST Service is published, for example `http://hive-host:8080`.
   - Do **not** use `http://localhost:8080`; that value appears in the API specification only as a development placeholder.
   - Confirm the URL resolves and responds from a machine on the same network before configuring the pipeline.

2. **Obtain the bearer token (`token`)**
   - Request a token from your Apis administrator. The Apis REST Service specification defines no token-issuing endpoint, so the token must be provisioned for you by the platform.
   - Store it as the `token` connection option; it is treated as a secret and never logged.
   - You can verify the token and the expected header form with a direct call:

     ```bash
     curl -i -X GET "http://hive-host:8080/hive?format=json" \
       -H "Authorization: Bearer <token>"
     ```

   - If that returns `401`/`403` but the same call **without** the `Bearer ` prefix succeeds, set the `auth_scheme` connection option to an empty string. The Apis specification declares `Authorization` as a raw header rather than a formal bearer scheme, so both forms are plausible depending on deployment.

3. **Identify the Hive instance name (`instance`, optional but recommended)**
   - The response to `GET /hive` (as in the `curl` above) lists the instance names, for example `ApisHive`.
   - Pinning `instance` explicitly avoids the discovery call on every pipeline run and makes the ingestion deterministic. You may list several instances, comma-separated.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** flow from the **Add Data** page.
2. Select an existing Lakeflow Community Connector connection for this source, or create a new one and supply `base_url` and `token` (plus any of the optional parameters above).
3. Set `externalOptionsAllowList` to:
   `instance,instances,item,items,item_patterns,attrib,attribs,quality,aggregate,interval,start_timestamp,backfill_days,window_seconds,max_partitions,max_records_per_batch,max_windows_per_read`

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector exposes a **static list** of three tables (exact casing as shown):

- `items`
- `values`
- `timeseries`

Other endpoints of the Apis REST Service are intentionally not exposed as tables: the instance list is an internal lookup used to resolve the Hive instance name, and the instance runstate/module listings are health and configuration calls rather than row-producing datasets. Writing values back to the historian is also out of scope — this connector is read-only.

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|---|---|---|---|---|
| `items` | Catalog of items (points/tags) in a Hive instance, optionally enriched with requested attributes | `snapshot` | `instance`, `item_name` | n/a (fully re-listed each run) |
| `values` | Current (real-time) value of each matching item | `cdc` (upsert-only) | `instance`, `item_name` | `timestamp` |
| `timeseries` | Historical — raw or server-aggregated — values per item over a time window | `append` | n/a (append-only) | `timestamp` |

**Incremental behavior**

- `items` is a full re-list on every run. The source offers no change cursor and no created/modified field for the catalog, so the table is treated as a slowly-changing dimension refreshed in full.
- `values` is an upsert stream keyed on `(instance, item_name)`. The connector passes the committed cursor to the source as an "updated since" filter, so each run only requests values that changed since the previous run. Because the source returns one row per item, re-reading a boundary row is harmless — it merges onto the same key.
- `timeseries` is append-only over contiguous, non-overlapping half-open `[start, end)` windows. The cursor advances to the window boundary, not to the last observed record, so empty windows still make progress.
- **No table supports delete synchronization.** The Apis REST Service provides no delete feed and no tombstone field for any object, so no table can be configured for CDC with deletes. A retired item simply stops appearing in subsequent snapshots; detecting removals requires diffing successive `items` snapshots downstream.

### Columns that require attention

- **`instance`** — added by the connector from the Hive instance being read; it is not part of any response body. It is part of the primary key of `items` and `values` because item names are unique only *within* an instance.
- **`item_name`** — the fully-qualified item name, conventionally `<Module>.<Item>` (for example `Worker.Signal1`).
- **`module`** (`items` only) — derived by the connector by splitting `item_name` at the first `.`. The `<Module>.<Item>` convention is observed but not formally guaranteed by the source, so this column is best-effort and may be `null`.
- **`value` / `quality` / `timestamp`** (`values`, `timeseries`) — the source's declared `value: {v, q, t}` triple, flattened onto three top-level columns instead of kept as a nested struct, so downstream SQL can reference each field directly:
  - `value` — the reading itself, stored as a **string**. The source places no type constraint on it; the underlying point may be numeric, boolean, or textual, and only the historian knows which. A numeric point arrives as, for example, `"42.7"`. Cast it downstream where the point's real type is known.
  - `quality` — quality indicator. The source's quality *filter* offers only `good` / `uncertain` / `bad`, but the response field is an open string, so richer raw quality codes (for example OPC codes) are preserved rather than rejected.
  - `timestamp` — timestamp of the reading, kept as a string. It is presumed ISO 8601, but the source declares no format for this field; keeping it as a string prevents an unexpected rendering from failing a whole batch. It also doubles as the incremental cursor for both tables.
- **`attributes`** (`items` only) — an array of `{attrib_id, value}` pairs, populated only when you request attribute IDs via the `attrib` table option. Both sides are open strings: the source documents no registry mapping attribute IDs to meanings, so you must obtain the ID meanings from your Apis administrator.
- **`timeseries` has no declared primary key** because append-only tables are inserted rather than merged. Its *logical* uniqueness key is `(instance, item_name, timestamp)`; use that if you deduplicate downstream.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. Applies to `items` and `values`, not `timeseries`. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

#### Applies to all three tables

| Option | Required | Default | Description |
|---|---|---|---|
| `instance` / `instances` | No | connection value, else discovered | Comma-separated Hive instance name(s) for this table, overriding the connection-level setting. |
| `item` / `items` / `item_patterns` | No | `*` | Comma-separated item name patterns, with `*` as the wildcard — for example `Work*.Sig*,Logger.*`. Each pattern is read independently and in parallel. Narrowing patterns is the primary way to keep responses from being truncated (see [Known Limitations](#known-limitations)). |
| `max_partitions` | No | `64` | Upper bound on parallel reads per micro-batch. When a request needs more time windows than this budget allows, windows are widened rather than producing thousands of tiny tasks. |
| `max_records_per_batch` | No | `1000` | Caps records per batch on the sequential (non-parallel) read path. Ignored for `items`, which is always a full snapshot. |

#### `items` only

| Option | Required | Default | Description |
|---|---|---|---|
| `attrib` / `attribs` | No | none | Comma-separated numeric attribute IDs to request for each item. Returned in the `attributes` array column. When omitted, no attributes are requested. |

#### `values` and `timeseries`

| Option | Required | Default | Description |
|---|---|---|---|
| `quality` | No | none (no filter) | Minimum quality filter. One of `good`, `uncertain`, `bad`. An invalid value fails fast with a clear error instead of a bare HTTP 400. |

#### `timeseries` only

| Option | Required | Default | Description |
|---|---|---|---|
| `start_timestamp` | No | derived from `backfill_days` | Absolute ISO 8601 lower bound for the first run, for example `2024-01-01T00:00:00Z`. **Strongly recommended** — it is the only way to bound the initial backfill precisely. Relative "OPC time" expressions such as `DAY-1D` are not accepted here: the connector always sends absolute instants so incremental reads stay deterministic and replayable. |
| `backfill_days` | No | `1095` (3 years) | How far back the first run reaches when neither a committed cursor nor `start_timestamp` is available. The generous default assumes a historian backfill is the point of the first run; lower it for a quick first run. |
| `window_seconds` | No | `86400` (1 day) | Width of one time window. Windows are contiguous and half-open, so a reading cannot land in two windows. Start small (for example `3600`) when validating a new configuration. |
| `aggregate` | No | none (raw recorded values) | Server-side aggregation function applied per window bucket. Validated locally against the source's full function list, which includes `average`, `timeaverage`, `interpolative`, `minimum`, `maximum`, `total`, `sum`, `count`, `median`, `delta`, `range`, `variance`, `start`, `end`, `duration_good`, `duration_bad`, `percent_good`, `percent_bad`, `worst_quality`, `movingaveragebytime`, `movingaveragebycount`, `lowpassfilter`, and the regression/vector/UA variants. |
| `interval` | No | none (source default `3600`) | Resample bucket size in seconds, between `1` and `86400`. Only meaningful together with `aggregate`; the source does not state whether it is ignored when `aggregate` is absent. |
| `max_windows_per_read` | No | `64` | Maximum number of windows walked per batch on the sequential read path, so it advances in strides comparable to the parallel path. |

## Data Type Mapping

| Apis concept | Source type | Databricks type | Notes |
|---|---|---|---|
| `item_name` | string | `string` | Fully-qualified item name, conventionally `<Module>.<Item>`. |
| `value` (the reading) | untyped — no constraint declared by the source | `string` | Deliberately stringified so a numeric, boolean, or textual point all ingest losslessly instead of failing the batch. Cast downstream (for example `CAST(value AS DOUBLE)`) once the point's real type is known. |
| `quality` (quality) | string | `string` | Open string, not an enum. Raw quality codes beyond `good` / `uncertain` / `bad` are preserved. |
| `timestamp` (timestamp) | string, no declared format | `string` | Presumed ISO 8601 but not declared as such by the source; kept as a string. Cast with `TO_TIMESTAMP(timestamp)` downstream. |
| Attribute ID / value (`items`) | number (request) / undeclared (response) | `array<struct<attrib_id: string, value: string>>` | Both sides kept as open strings because the source publishes no attribute-ID registry. |
| Hive instance name | path segment (not a response field) | `string` | Stamped onto every row by the connector for scoping. |
| Request timestamps (`updatedSince`, `starttime`, `endtime`) | ISO 8601 or relative "OPC time" expression | n/a (request-only) | The connector always sends absolute UTC ISO 8601 instants (`YYYY-MM-DDTHH:MM:SSZ`). |
| Response `format` | `json` or `csv` | n/a (request-only) | The connector always requests `json`; the source defines no CSV schema. |

In short: the declared `value` triple is flattened onto `value`/`quality`/`timestamp` columns, item attributes are preserved as an array rather than flattened, every scalar reading is carried as a string to survive an untyped source, and an absent point surfaces as `null` columns rather than a missing row.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which guides you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (for example `ingest.py`).
2. Reference the Unity Catalog connection created above, and add one `table` entry per table you want to ingest, with its options under `table_configuration`.

```json
{
  "pipeline_spec": {
    "connection_name": "apis_connection",
    "object": [
      {
        "table": {
          "source_table": "items",
          "table_configuration": {
            "instance": "ApisHive",
            "item": "Worker.*,Logger.*",
            "attrib": "1,2"
          }
        }
      },
      {
        "table": {
          "source_table": "values",
          "table_configuration": {
            "instance": "ApisHive",
            "item": "Worker.*",
            "quality": "good"
          }
        }
      },
      {
        "table": {
          "source_table": "timeseries",
          "table_configuration": {
            "instance": "ApisHive",
            "item": "Worker.Signal1,Worker.Signal2",
            "start_timestamp": "2024-01-01T00:00:00Z",
            "window_seconds": "3600",
            "aggregate": "average",
            "interval": "600",
            "quality": "good"
          }
        }
      }
    ]
  }
}
```

Notes on the example:

- `connection_name` must point to the Unity Catalog connection holding your `base_url` and `token`.
- `source_table` must be one of `items`, `values`, `timeseries`.
- Omit `aggregate` and `interval` from the `timeseries` entry to ingest raw recorded values instead of resampled aggregates.
- You can ingest the same source table more than once with different destination tables and different item patterns — for example one destination table per plant area.

3. (Optional) Customize the source connector code if you need behavior beyond these options.

### Step 3: Run and Schedule the Pipeline

On the **first run** of `timeseries`, either set `start_timestamp` to the earliest point you actually need or lower `backfill_days`; otherwise the run reaches back three years. Subsequent runs resume from the committed window boundary. For `values`, the first run reads all matching items and later runs request only values that changed since the previous run.

#### Best Practices

- **Start small.** Validate with a single instance, an explicit short item list, and a small `window_seconds` (for example `3600`) before widening to wildcards and full history.
- **Always pin `instance`.** Discovery works, but pinning the instance name removes a request per run and makes failures easier to diagnose.
- **Keep item patterns narrow.** Because the source has no pagination, a narrow `item` pattern is the main defense against truncated responses — and it also increases read parallelism, since each pattern is read independently.
- **Set `start_timestamp` for `timeseries`.** It is the difference between a bounded first run and a three-year backfill.
- **Use `aggregate` + `interval` to reduce volume** when you do not need every recorded point — resampling at the source is far cheaper than ingesting raw history and aggregating afterward.
- **Schedule to match the data's real cadence.** `values` only reports current values, so polling far faster than the historian updates adds load without adding rows. `items` changes rarely — a daily or weekly refresh is usually plenty.
- **Treat rate limits as unknown.** The source documents none, so the connector retries conservatively with exponential backoff. If you see gateway-level throttling, lower `max_partitions` to reduce concurrent requests.
- **Cast types downstream.** Build a view that casts `value` and `timestamp` to their real types once, and consume that view instead of the raw table.

#### Troubleshooting

**Common Issues:**

- **`401` / `403` on every request** — verify the token is correct and not expired. If a direct `curl` succeeds without the `Bearer ` prefix, set the `auth_scheme` connection option to an empty string; the source does not specify which form the server expects.
- **`404` on an instance-scoped call** — the Hive instance name is wrong or the instance is not running. List instances with `GET /hive` and set `instance` explicitly.
- **TLS / certificate errors** — on-premises Hive deployments frequently use self-signed certificates. Prefer installing the certificate in the runtime's trust store; as a last resort set `verify_ssl` to `false` (not recommended for production).
- **Connection timeouts** — confirm the compute running the pipeline has network reachability to the historian's host and port, then raise `request_timeout` if the historian is simply slow to answer wide queries.
- **A warning about truncated (partial) results** — the source truncated the response and offers no way to resume it. Narrow the `item` pattern (for example per-module wildcards) or shorten `window_seconds`, then re-run. See [Known Limitations](#known-limitations).
- **`timeseries` returns no rows for an item that exists in `items`** — history exists only for items the Hive has been configured to log. Confirm with your Apis administrator that the item is logged, and that the requested window overlaps its recorded range.
- **An error about an invalid ISO 8601 timestamp** — `start_timestamp` must be an absolute instant such as `2024-01-01T00:00:00Z`. Relative "OPC time" expressions like `DAY-1D` are accepted by the source but rejected by the connector, which requires deterministic, replayable bounds.
- **An error about an unsupported `aggregate` or `quality` value** — these are validated before the request is sent; the error message lists the accepted values.
- **Numeric comparisons behave like string comparisons** — `value` is a string by design. Cast it (`CAST(value AS DOUBLE)`) in your downstream transformations.
- **`timeseries` rows look duplicated at window boundaries** — the connector treats windows as half-open `[start, end)`. If your deployment treats the end of a window as inclusive, boundary readings can appear twice; deduplicate on `(instance, item_name, timestamp)` and report the behavior so the windowing can be adjusted.

## Known Limitations

These are limitations of the Apis REST Service itself or of the current, not-yet-live-validated state of this connector — not configuration mistakes.

1. **No pagination exists in the source API.** No endpoint defines a `limit`, `offset`, `cursor`, page-token parameter, or `Link` header — yet `items`, `values`, and `timeseries` can all return a *partial content* response, which means the server **can** truncate a large result with no documented way to detect where it stopped or to resume. The connector therefore issues exactly one request per (instance, item pattern, time window) combination and never invents a continuation parameter. Truncation is mitigated the only way the source allows: **narrow your `item` patterns and shorten `window_seconds`.** A truncated response is logged as a warning rather than silently accepted, so watch pipeline logs for it — if it appears, your table may be incomplete for that run.
2. **The `items` and `timeseries` response schemas are inferred, not declared.** The source's specification declares a response body for the current-values endpoint only. Everything else declares success with no body schema at all. Consequently:
   - `items` columns (`item_name`, `module`, `attributes`) are modeled on the only item identity the source exposes plus the attribute-request parameter. Field names are not confirmed.
   - `timeseries` is modeled on a historian's canonical shape — one row per (item, point in time), carrying the same `v`/`q`/`t` triple as current values. This is the largest gap in the specification.
   - The connector's parsers deliberately accept several plausible response shapes for these two tables rather than hard-failing on an unexpected one, but column names and nesting for `items` and `timeseries` should be treated as **provisional until validated against a live Hive instance**. Pin a schema downstream only after you have confirmed real output.
3. **Not yet validated against a live source.** No live-testing pass has been performed. In addition to the inferred schemas above, the following are unconfirmed: the exact `Authorization` header form (hence the `auth_scheme` escape hatch), whether the end of a `timeseries` window is exclusive, the format of `timestamp`, and the shape of the instance discovery response.
4. **No delete tracking for any object.** The source exposes no delete endpoint and no tombstone field, so no table supports CDC with deletes. Removals can only be inferred by diffing successive `items` snapshots.
5. **No documented rate limits.** The source declares no throttling responses, headers, or guidance. The connector retries conservatively with exponential backoff on throttling and gateway errors, but safe request rates must be established empirically for your deployment.
6. **Attribute IDs have no published meaning.** The `attrib` option takes numeric IDs and the `attributes` column returns them verbatim. There is no ID-to-name registry in the API; obtain the mapping from your Apis administrator.
7. **`interval`'s interaction with `aggregate` is unspecified.** The source does not state whether `interval` is ignored when `aggregate` is omitted. Set them together.
8. **`items` cannot be read incrementally.** The catalog has no change cursor and no modified timestamp, so it is fully re-listed each run. The item pattern narrows scope but is not a cursor.
9. **`values` cursors on wall-clock progress, not on observed record timestamps.** The endpoint returns one current value per item with no ordering guarantee and no upper time bound, so no reliable record-derived cursor exists. Re-reading a boundary row is harmless because the table upserts on `(instance, item_name)`.
10. **No point-in-time historical lookup.** The source has no "value at this instant" endpoint. Retrieving one historical reading requires a `timeseries` request over a narrow window around the desired time.
11. **JSON only.** The source also offers CSV, but defines no CSV schema, so the connector always requests JSON.
12. **Read-only.** The source's write endpoint for pushing values into the historian is intentionally not used by this connector.
13. **Instance runstate and module listings are not ingested.** They are health and configuration lookups rather than row-producing datasets.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/apis/apis.py`
- Table schemas, defaults, and schema provenance notes: `src/databricks/labs/community_connector/sources/apis/apis_schemas.py`
- Source API research and known quirks: `src/databricks/labs/community_connector/sources/apis/apis_api_doc.md`
- Connection parameters and allowed table options: `src/databricks/labs/community_connector/sources/apis/connector_spec.yaml`
- Source OpenAPI specification (Apis REST Service 0.4.0): `src/databricks/labs/community_connector/sources/apis/apis_openapi.json`
- Vendor product documentation (Apis Foundation / Hive, for background on instances, modules, items, and logging): `https://docs.prediktor.com/`
