# Iceberg Compaction Service - Project Specification

## Project Vision

Build a **lightweight, cloud-native Iceberg compaction service** that combines Rust's performance with intelligent planning algorithms, filling the gap between library-based solutions (Nimtable) and heavyweight platforms (Amoro).

---

## MVP Scope (4 Weeks)

### Core Features

#### 1. Compaction Engine ✅ Priority: P0
- **Reuse from Nimtable**: DataFusion executor, file scanning
- **New Implementation**:
  - REST API wrapper
  - Configuration management
  - Basic monitoring

#### 2. Smart File Selection ✅ Priority: P0
- **Port from Amoro**:
  - Fragment ratio calculation
  - Partition scoring algorithm
- **Enhance with**:
  - Cost-based selection (minimize S3 API calls)
  - Time-series awareness (recent vs old data)

#### 3. REST API ✅ Priority: P0
```yaml
endpoints:
  - POST /compact        # Trigger compaction
  - GET /jobs           # List compaction jobs
  - GET /jobs/{id}      # Job status
  - GET /health         # Health check
  - GET /metrics        # Prometheus metrics
```

#### 4. Basic Scheduling ✅ Priority: P1
- Cron-based triggers
- Manual triggers via API
- Table-specific schedules

### Non-MVP Features (Defer)
- ❌ Distributed execution
- ❌ Serverless support
- ❌ ML-based planning
- ❌ Web UI
- ❌ Multi-catalog support

---

## Technical Architecture (MVP)

### System Design

```
┌────────────────────────────────────┐
│         REST API (Axum)            │
├────────────────────────────────────┤
│    • /compact endpoint             │
│    • /jobs monitoring              │
│    • /metrics Prometheus           │
└────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────┐
│      Compaction Service            │
├────────────────────────────────────┤
│    • Planner (from Amoro)          │
│    • Executor (from Nimtable)      │
│    • Scheduler (Tokio)             │
└────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────┐
│        Storage Layer               │
├────────────────────────────────────┤
│    • S3/GCS via object_store       │
│    • REST Catalog                  │
│    • Redis (job state)             │
└────────────────────────────────────┘
```

### Project Structure

```
iceberg-compaction/
├── Cargo.toml                 # Workspace root
├── Cargo.lock
├── README.md
├── LICENSE
├── .github/
│   └── workflows/
│       ├── ci.yml             # Tests and linting
│       └── release.yml        # Build releases
│
├── iceberg-compaction-core/   # Core library
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── planner/           # Planning algorithms
│       │   ├── mod.rs
│       │   ├── scoring.rs     # From Amoro
│       │   └── binpack.rs     # From Amoro
│       ├── executor/          # From Nimtable
│       │   ├── mod.rs
│       │   └── datafusion.rs
│       ├── catalog/           # Catalog abstraction
│       └── config.rs          # Configuration
│
├── iceberg-compaction-server/ # REST API server
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       ├── api/               # API handlers
│       │   ├── mod.rs
│       │   ├── compact.rs
│       │   └── jobs.rs
│       ├── scheduler.rs       # Cron scheduler
│       └── state.rs           # App state
│
├── examples/                  # Usage examples
│   ├── basic-compaction.rs
│   └── scheduled-compaction.rs
│
├── tests/                     # Integration tests
│   └── integration_test.rs
│
└── docker/
    ├── Dockerfile
    └── docker-compose.yml
```

---

## Implementation Plan

### Week 1: Foundation

#### Day 1-2: Project Setup
```bash
# Create workspace structure
cargo new iceberg-compaction --lib
cargo new iceberg-compaction-core --lib
cargo new iceberg-compaction-server --bin

# Setup dependencies in workspace Cargo.toml
[workspace]
members = ["iceberg-compaction-core", "iceberg-compaction-server"]

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
axum = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"
iceberg = { git = "https://github.com/risingwavelabs/iceberg-rust" }
datafusion = "45"
arrow = "54"
```

#### Day 3-4: Core Compaction Logic
Port from Nimtable:
- `CompactionExecutor` trait
- `DataFusionExecutor` implementation
- `FileGroup` and selection logic

#### Day 5: Catalog Integration
- REST catalog client
- Table metadata reading
- File listing and filtering

### Week 2: Intelligence

#### Day 6-7: Planning Algorithms
Port from Amoro:
```rust
// Fragment ratio calculation
pub fn calculate_fragment_ratio(
    data_files: usize,
    delete_files: usize,
) -> f64 {
    delete_files as f64 / data_files.max(1) as f64
}

// Partition scoring
pub struct PartitionScore {
    pub fragment_score: f64,
    pub small_file_score: f64,
    pub time_score: f64,
    pub total: f64,
}
```

#### Day 8-9: Bin Packing
```rust
pub fn bin_pack_files(
    files: Vec<DataFile>,
    target_size: ByteSize,
) -> Vec<FileGroup> {
    // Implement First Fit Decreasing algorithm
    // Group files to minimize number of output files
}
```

#### Day 10: Cost Model
```rust
pub struct CostModel {
    s3_get_cost: f64,      // $0.0004 per 1000 requests
    s3_put_cost: f64,      // $0.005 per 1000 requests
    compute_cost: f64,     // $0.0000166667 per GB-second
}
```

### Week 3: Service Layer

#### Day 11-12: REST API
```rust
// Using Axum
async fn compact_table(
    State(app): State<AppState>,
    Json(req): Json<CompactRequest>,
) -> Result<Json<CompactResponse>> {
    let job_id = Uuid::new_v4();

    // Queue compaction job
    app.queue.push(CompactionJob {
        id: job_id,
        table: req.table,
        strategy: req.strategy,
    })?;

    Ok(Json(CompactResponse { job_id }))
}
```

#### Day 13-14: Job Management
```rust
pub struct JobManager {
    active_jobs: Arc<RwLock<HashMap<Uuid, Job>>>,
    queue: Arc<RwLock<VecDeque<Job>>>,
    executor: Arc<CompactionExecutor>,
}
```

#### Day 15: Scheduler
```rust
pub struct Scheduler {
    cron: JobScheduler,
    tables: Vec<TableSchedule>,
}

pub struct TableSchedule {
    table_name: String,
    cron_expression: String,
    strategy: CompactionStrategy,
}
```

### Week 4: Production Readiness

#### Day 16-17: Monitoring
```rust
// Prometheus metrics
lazy_static! {
    static ref COMPACTION_DURATION: Histogram = register_histogram!(
        "compaction_duration_seconds",
        "Compaction job duration"
    ).unwrap();

    static ref FILES_COMPACTED: Counter = register_counter!(
        "files_compacted_total",
        "Total files compacted"
    ).unwrap();
}
```

#### Day 18-19: Configuration & Deployment
```yaml
# config.yaml
server:
  host: 0.0.0.0
  port: 8080

catalog:
  type: rest
  uri: http://catalog:8181

compaction:
  target_file_size: 1GB
  max_parallelism: 4

scheduler:
  enabled: true
  default_schedule: "0 */6 * * *"  # Every 6 hours
```

#### Day 20: Testing & Documentation
- Integration tests with LocalStack (S3)
- Load testing with vegeta
- API documentation with OpenAPI
- README with examples

---

## Configuration Examples

### 1. Basic Configuration
```yaml
# config.yaml
catalog:
  type: rest
  uri: ${CATALOG_URI}

storage:
  type: s3
  bucket: ${S3_BUCKET}
  region: ${AWS_REGION}

compaction:
  strategies:
    small_files:
      enabled: true
      threshold: 32MB
      target_size: 1GB

    delete_files:
      enabled: true
      fragment_ratio_threshold: 0.1
```

### 2. Table-Specific Configuration
```yaml
tables:
  - name: events
    schedule: "*/30 * * * *"  # Every 30 minutes
    strategy: time_series
    options:
      target_file_size: 512MB
      partition_filter: "date >= current_date - 7"

  - name: dimensions
    schedule: "0 2 * * *"     # Daily at 2 AM
    strategy: small_files
    options:
      target_file_size: 1GB
```

---

## API Specification

### POST /api/v1/compact
Trigger a compaction job.

**Request:**
```json
{
  "catalog": "production",
  "namespace": "analytics",
  "table": "events",
  "strategy": "small_files",
  "options": {
    "target_file_size": "1GB",
    "max_cost": 10.0,
    "partition_filter": "date = '2024-01-01'"
  }
}
```

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued",
  "created_at": "2024-01-15T10:00:00Z",
  "estimated_duration_seconds": 300
}
```

### GET /api/v1/jobs/{job_id}
Get job status.

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "progress": {
    "partitions_completed": 5,
    "partitions_total": 10,
    "files_processed": 150,
    "bytes_read": 5368709120,
    "bytes_written": 1073741824
  },
  "started_at": "2024-01-15T10:00:05Z",
  "estimated_completion": "2024-01-15T10:05:00Z"
}
```

### GET /api/v1/metrics
Prometheus metrics endpoint.

```
# HELP compaction_jobs_total Total number of compaction jobs
# TYPE compaction_jobs_total counter
compaction_jobs_total{status="success"} 142
compaction_jobs_total{status="failed"} 3

# HELP compaction_duration_seconds Compaction job duration
# TYPE compaction_duration_seconds histogram
compaction_duration_seconds_bucket{le="60"} 50
compaction_duration_seconds_bucket{le="300"} 130
compaction_duration_seconds_bucket{le="600"} 142
```

---

## Deployment

### Docker
```dockerfile
# Dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/iceberg-compaction-server /usr/local/bin/
EXPOSE 8080
CMD ["iceberg-compaction-server"]
```

### Docker Compose
```yaml
version: '3.8'
services:
  compaction:
    image: iceberg-compaction:latest
    ports:
      - "8080:8080"
    environment:
      - CATALOG_URI=http://rest-catalog:8181
      - REDIS_URL=redis://redis:6379
      - AWS_REGION=us-east-1
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
    depends_on:
      - redis
      - rest-catalog

  redis:
    image: redis:7-alpine

  rest-catalog:
    image: tabular/iceberg-rest:latest
```

### Kubernetes (Future)
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: iceberg-compaction
spec:
  replicas: 3
  selector:
    matchLabels:
      app: iceberg-compaction
  template:
    metadata:
      labels:
        app: iceberg-compaction
    spec:
      containers:
      - name: compaction
        image: iceberg-compaction:latest
        ports:
        - containerPort: 8080
        env:
        - name: CATALOG_URI
          valueFrom:
            secretKeyRef:
              name: catalog-secret
              key: uri
```

---

## Success Criteria (MVP)

### Performance
- [ ] Compact 100GB in < 5 minutes on single node
- [ ] Support tables with up to 10,000 files
- [ ] Planning latency < 1 second

### Reliability
- [ ] 99% success rate for compaction jobs
- [ ] Graceful handling of transient S3 errors
- [ ] No data loss or corruption

### Usability
- [ ] Simple REST API
- [ ] Clear error messages
- [ ] Comprehensive logging

### Cost
- [ ] < $0.05 per GB compacted (S3 costs)
- [ ] Minimize unnecessary S3 API calls
- [ ] Efficient memory usage (< 4GB for most workloads)

---

## Future Roadmap (Post-MVP)

### Phase 2: Scale (Months 2-3)
- Distributed execution with Ray/Ballista
- Kubernetes operator
- Web UI dashboard
- Multi-catalog support

### Phase 3: Intelligence (Months 4-5)
- ML-based compaction prediction
- Query-aware optimization
- Auto-tuning based on workload
- Cost optimization advisor

### Phase 4: Enterprise (Months 6+)
- Multi-tenancy with isolation
- RBAC and audit logging
- SLA guarantees
- Professional support

---

## Competitive Analysis

| Feature | Our Service | Nimtable | Amoro | Trino | Spark |
|---------|------------|----------|-------|-------|--------|
| Language | Rust | Rust | Java | Java | Scala |
| Deployment | Service/Library | Library | Platform | Platform | Platform |
| Cloud-Native | ✅ | ❌ | ❌ | ❌ | ❌ |
| Cost-Aware | ✅ | ❌ | ❌ | ❌ | ❌ |
| REST API | ✅ | ❌ | ✅ | ❌ | ❌ |
| Scheduling | ✅ | ❌ | ✅ | ❌ | External |
| Performance | High | High | Medium | Medium | Medium |
| Setup Complexity | Low | Low | High | High | High |

---

## Development Guidelines

### Code Style
- Use `rustfmt` and `clippy`
- Write unit tests for all public functions
- Document public APIs
- Use `thiserror` for errors
- Use `tracing` for logging

### Testing Strategy
- Unit tests: 80% coverage minimum
- Integration tests with LocalStack
- Property-based tests for algorithms
- Load tests before release

### CI/CD
- GitHub Actions for CI
- Automated releases on tag
- Docker images pushed to registry
- Changelog generation

---

## Getting Started

### Prerequisites
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install Docker
# Install LocalStack for testing
pip install localstack

# Clone the repository
git clone https://github.com/yourusername/iceberg-compaction.git
cd iceberg-compaction
```

### Build & Run
```bash
# Build the project
cargo build --release

# Run tests
cargo test

# Start the service
CATALOG_URI=http://localhost:8181 cargo run --bin iceberg-compaction-server

# Run with Docker
docker-compose up
```

### First Compaction
```bash
# Trigger compaction via API
curl -X POST http://localhost:8080/api/v1/compact \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "default",
    "namespace": "public",
    "table": "events",
    "strategy": "small_files"
  }'

# Check job status
curl http://localhost:8080/api/v1/jobs/{job_id}
```

---

This specification provides a clear, achievable path to building a production-ready Iceberg compaction service in 4 weeks, leveraging the best ideas from both Nimtable and Amoro while introducing novel cloud-native features.