# WXD/IBM ONLY — DO NOT UPSTREAM

**Do not open this configuration or its dependency changes as an upstream PR.**
This is the experimental AWS GPU benchmark setup: g7e.48xlarge, eight workers,
one 96-GiB GPU per worker, two task drivers, CUDA 13.2 and the WXD UCX 1.22 build.
It is not a production profile for arbitrary Java, CPU-native or GPU clusters.

## Select the configuration, not different Java defaults

[configs/wxd-ibm](configs/wxd-ibm) contains ordinary Presto configuration files.
They preserve the tuned settings without changing Presto's Java or native C++
runtime defaults, adding a configuration framework, or changing existing tests.
The published Velox GPU defaults remain in the dependency itself.

| Profile file | Installed location |
| --- | --- |
| `coordinator/config.properties` | Coordinator `etc/config.properties` |
| `coordinator/catalog/hive.properties` | Coordinator `etc/catalog/hive.properties` |
| `coordinator/session-property-config.properties` | Coordinator `etc/session-property-config.properties` |
| `coordinator/session-property-config.json` | Coordinator `etc/session-property-config.json` |
| `worker/config.properties` | Each GPU worker's `etc/config.properties` |
| `worker/catalog/hive.properties` | Each GPU worker's `etc/catalog/hive.properties` |

These are **tuning layers, not complete deployment configurations**. Merge their
keys into the existing files, replacing duplicate keys rather than appending
conflicting values. Keep deployment-specific discovery, identities, ports,
memory limits, metastore and credential settings. Do not replace a populated
`etc/` directory with these files. Coordinator and worker Hive files differ;
the worker's native S3 options do not belong in the Java catalog.

The two session files use Presto's existing `file-session-property-manager`
plugin, already packaged in both `plugin/` and `native-plugin/` in the standard
server distribution. Custom distributions must include it. The JSON path is
relative to the coordinator's working/data directory; use an absolute path in
`session-property-config.properties` if necessary. Presto discovers the manager
configuration at `etc/session-property-config.properties` in that directory.
If the deployment already uses a session manager, merge the two defaults into
that manager instead of installing a second one.

The rule supplies `native_cudf_exchange_enabled=true` and
`experimental_deterministic_bounded_splits=true` to all sessions on this
dedicated GPU cluster. `overrideSessionProperties=false` preserves explicit
client/session overrides. No Java configuration hook or CPU opt-out flag is
needed: deployments that do not select these files retain the existing Presto
defaults. Do not install this rule on a coordinator serving CPU-only workers.

Once installed, restart the coordinator and GPU workers through the deployment's
normal startup mechanism. There is no new launcher, shell environment layer or
per-query list of tuning arguments. Building alone does **not** install this
profile or rewrite existing configuration. Future tuning edits require a
configuration rollout/restart, not a Java rebuild.

Verify the coordinator's effective **Value** column after installation:

```sql
SHOW SESSION LIKE 'native_cudf_exchange_enabled';
SHOW SESSION LIKE 'experimental_deterministic_bounded_splits';
SHOW SESSION LIKE 'schedule_splits_based_on_task_load';
SHOW SESSION LIKE 'hive.file_splittable';
```

The first three should be `true` and the last `false`. The **Default** column
for the first two remains `false`: the file manager supplies session defaults
without changing the properties' built-in Java defaults.

## What is selected

- Native cuDF exchange, deterministic bounded split placement, task-load
  scheduling, soft Hive affinity and whole-file splits.
- The measured planner/function settings: 20,788-MB broadcast limit, default
  filter factor enabled, automatic partial aggregation, not-null/inequality
  inference, IN-to-inner-join, complex equi-joins, domain filters, optimized
  repartitioning and dereference/subfield pushdown; RE2J and alternative
  aggregation signatures, with Java-side hash generation disabled.
- Phased execution, 2,000-split scheduling minimum and node/pending-task queues,
  512-MiB exchange/sink buffers and 64-MiB coordinator responses. Queue limits
  do not increase executing task drivers; `node-scheduler.max-splits-per-task`
  retains its existing default of 10.
- Two drivers per GPU task, native runtime metrics, 512-MiB native local
  exchange buffers, AsyncDataCache and registered host slabs capped at 32 GiB
  per worker. That cap is neither total cache capacity nor an upfront allocation.
- Metastore caches of 10 minutes / 2-minute refresh / 100,000 entries / 10,000
  per transaction; file listings of 1 GiB / 30 minutes; 16 split loaders and
  partition parsers, with optimized parsing from 200 partitions.

The metadata caches assume **immutable benchmark tables**. Listings are cached
for all tables (`*`) and manifests are preferred. Invalidate the relevant caches
after changing files, statistics or constraints. No metastore location, DDL
permission, primary-key assertion or unknown-filter selectivity change is added.
Deterministic placement retains its existing 100,000-splits-per-scan bound and
worker-membership caveats; it is not durable cache ownership.

Velox supplies compressed mixed SRD/IPC using the normal main async GPU allocator,
with compression allowed on IPC endpoints, plus the measured batch/operator
defaults. KvikIO uses **MULTI_POLL**, 128 concurrent request slots per process,
four reactors, `PER_CHUNK` dispatch, 32-MiB tasks, 16 worker threads and strict
direct receive (`REQUIRE`). The request limit is not 128 reactor threads.
See [the Velox profile](velox/velox/experimental/cudf/WXD_IBM_DEFAULTS.md) for
the complete UCX/KvikIO settings and override precedence.

For the BufferedInput alternative, change only
`cudf.hive.use-buffered-input=true` in the worker config. The worker catalog
already selects AWS SDK strict direct receive and adaptive TCP MSS. No reader
switch is needed on the coordinator. Existing environment/config overrides
still win over Velox's built-in defaults; inspect the worker startup line
`WXD GPU effective settings` to verify them. Defaults added inside the process
will not appear in a separate `docker exec ... printenv` invocation.

## Build directly through Presto

The Velox gitlink is `a4770a274975a9a3596307a816ca4bac4b541089`, published on
`rapidsai/velox:WXD-launch`. It includes the cache/ingress improvements, IPC
compression fix and GPU runtime defaults. Recursive builds must use this pinned
commit; a submodule branch name does not advance the dependency by itself.

From `presto-native-execution` in a recursive checkout, the existing Docker
workflow builds the native dependencies and worker without velox-testing:

```bash
docker build -f scripts/dockerfiles/centos-dependency.dockerfile -t presto/wxd-dependency:cuda13.2 .
docker build -f scripts/dockerfiles/prestissimo-runtime.dockerfile --build-arg DEPENDENCY_IMAGE=presto/wxd-dependency:cuda13.2 --build-arg PRESTO_OPTIONAL_FEATURES=cudf,s3 -t presto/wxd-native:cuda13.2 .
```

The dependency image installs CUDA 13.2, WXD UCX
`462c56777aaf268d7daf1b5d43e6f49e69b0207e` (1.22 plus Blackwell RTX IPC bandwidth
and GDA build fixes), and the pinned direct-receive curl/AWS SDK in an isolated
prefix. GPU builds select UCX exchange; GPU+S3 builds select direct receive.
Explicit versions, local sources and CMake settings, including cached `OFF`,
remain overrides. Rebuild dependencies for dependency changes, not every native
source-only edit. Build the Java coordinator from this Presto revision through
the normal Java workflow; the native image build does not supply a coordinator.

Non-Docker builders can use the pinned Velox `scripts/setup-centos-adapters.sh`
functions `install_cuda`, `install_ucx` and `install_s3_direct_receive_deps`.
The direct installer prints the required CMake prefix/package paths. Do not mix
ordinary curl with the direct AWS SDK/KvikIO runtime.

## Deployment requirements and limits

Provide discovery, node identities, HTTP/cuDF exchange ports, metastore settings,
S3 region and credentials, GPU isolation, CPU/NUMA placement, EFA device access
and the correct **per-worker `UCX_NET_DEVICES`**. Size host/system/query memory
budgets for the machine and container layout: eight workers can retain up to
256 GiB of registered cache slabs. The old generic 57-GB host-system budget is
not a substitute for sizing this deployment. Keep the deployment's memory
arbitration, query guardrails, logging and security configuration as well.

The build/profile does not install ENA drivers, change `tcp_rmem`, select jumbo
S3 IPs, tune queues/IRQs, set affinity or create credentials. Strict direct receive
requires the matching dependencies and a supported kernel/TLS path. UCXX error
handling is disabled as in the measured baseline; reduced fault tolerance,
large GPU batches and the large IPC mapping cache are experimental tradeoffs.

No parked raw/PULL IPC, exportable exchange-pool, private-async access or adaptive
flow-control experiment is included. This does not fix SF3K Q18/Q21 OOMs, resolve
the raw mixed IPC stall or establish an IPC performance improvement.
