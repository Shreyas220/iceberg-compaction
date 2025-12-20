# Distributed Iceberg Compaction

A distributed compaction system for Apache Iceberg tables, built with Rust and DataFusion.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           PLANNER SERVICE                                │
│                                                                          │
│  • Scans tables for files needing compaction                            │
│  • Groups files using bin-packing algorithm                             │
│  • Creates CompactionTasks and distributes to workers                   │
│  • Coordinates commits after workers complete                           │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 │  CompactionTask (serialized)
                                 ▼
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
        ▼                        ▼                        ▼
  ┌───────────┐            ┌───────────┐            ┌───────────┐
  │  WORKER   │            │  WORKER   │            │  WORKER   │
  │           │            │           │            │           │
  │ DataFusion│            │ DataFusion│            │ DataFusion│
  │ Execution │            │ Execution │            │ Execution │
  └─────┬─────┘            └─────┬─────┘            └─────┬─────┘
        │                        │                        │
        └────────────────────────┼────────────────────────┘
                                 │
                                 ▼
                         Output DataFiles
```

## Crates

| Crate | Description |
|-------|-------------|
| `compaction-common` | Shared types, errors, configuration |
| `compaction-proto` | Task definitions for planner↔worker communication |
| `compaction-planner` | File selection, grouping, task creation |
| `compaction-worker` | DataFusion execution engine |

## Key Features

### From Original Repo (Preserved)
- **Merge-on-Read Delete Handling**: SQL ANTI JOINs for position and equality deletes
- **Streaming Execution**: Bounded memory regardless of data size
- **Bin-Packing**: Optimal file grouping for parallelism
- **Delete Deduplication**: Avoids redundant delete file reads

### New for Distributed
- **Serializable Tasks**: FileGroups and configs serialize for network transport
- **Worker Independence**: Each worker executes independently with full context
- **Inline Deletes**: Small delete data can be sent directly to workers
- **Coordinated Commits**: Planner collects results for atomic commit

## Delete Handling Strategy

```
Small Deletes (< 10MB):
  └── Planner reads once, sends data inline to workers

Large Deletes:
  └── Workers read from storage (consider caching layer)

Very Large Equality Deletes (100GB+):
  └── Shuffle-based join: partition both data and deletes by join key
```

## Usage

```rust
use compaction_planner::{FileSelector, SelectionStrategy, TaskBuilder};
use compaction_worker::WorkerExecutor;

// Planner side
let file_groups = FileSelector::select_files(
    &table,
    snapshot_id,
    SelectionStrategy::SmallFiles { threshold_bytes: 32 * 1024 * 1024 },
    &config,
).await?;

let task_builder = TaskBuilder::from_table(&table, snapshot_id, exec_config)?;
let tasks = task_builder.build_tasks(file_groups);

// Send tasks to workers...

// Worker side
let executor = WorkerExecutor::new();
let result = executor.execute(task).await;
```

## Configuration

### Planning Config
```rust
PlanningConfig {
    target_file_size_bytes: 1GB,
    small_file_threshold_bytes: 32MB,
    max_parallelism: 16,
    grouping_strategy: BinPack { target: 1GB },
}
```

### Execution Config
```rust
ExecutionConfig {
    target_file_size_bytes: 1GB,
    max_record_batch_rows: 1024,
    sort_columns: Some(vec![...]),  // Honor table's sort order
}
```

## TODO

- [ ] Complete DataFusion table registration (IcebergFileScanTaskTableProvider)
- [ ] Implement streaming writer integration
- [ ] Add gRPC service definitions
- [ ] Implement commit coordinator
- [ ] Add worker heartbeat/health checks
- [ ] Support shuffle-based equality delete handling
- [ ] Add sorting support (read from table metadata)

## Credits

Core execution logic adapted from [iceberg-compaction](https://github.com/user/iceberg-compaction).
