/*
 * Observability - Metrics and Tracing
 *
 * Provides Prometheus-compatible metrics for monitoring compaction.
 */

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Global metrics registry for compaction.
#[derive(Debug, Default)]
pub struct CompactionMetrics {
    // Planner metrics
    pub tables_scanned: AtomicU64,
    pub files_selected: AtomicU64,
    pub tasks_created: AtomicU64,
    pub plans_created: AtomicU64,

    // Worker metrics
    pub tasks_started: AtomicU64,
    pub tasks_completed: AtomicU64,
    pub tasks_failed: AtomicU64,

    // Data metrics
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub rows_processed: AtomicU64,
    pub files_written: AtomicU64,

    // Timing metrics (in microseconds)
    pub total_execution_time_us: AtomicU64,
    pub total_read_time_us: AtomicU64,
    pub total_write_time_us: AtomicU64,

    // Commit metrics
    pub commits_attempted: AtomicU64,
    pub commits_succeeded: AtomicU64,
    pub commits_failed: AtomicU64,
}

impl CompactionMetrics {
    /// Creates a new metrics instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records bytes read.
    pub fn record_bytes_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records bytes written.
    pub fn record_bytes_written(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records rows processed.
    pub fn record_rows_processed(&self, rows: u64) {
        self.rows_processed.fetch_add(rows, Ordering::Relaxed);
    }

    /// Records a task start.
    pub fn record_task_start(&self) {
        self.tasks_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a task completion.
    pub fn record_task_complete(&self, success: bool) {
        if success {
            self.tasks_completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.tasks_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records execution time.
    pub fn record_execution_time(&self, duration: Duration) {
        self.total_execution_time_us.fetch_add(
            duration.as_micros() as u64,
            Ordering::Relaxed,
        );
    }

    /// Records a commit attempt.
    pub fn record_commit(&self, success: bool) {
        self.commits_attempted.fetch_add(1, Ordering::Relaxed);
        if success {
            self.commits_succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.commits_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Returns a snapshot of current metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            tables_scanned: self.tables_scanned.load(Ordering::Relaxed),
            files_selected: self.files_selected.load(Ordering::Relaxed),
            tasks_created: self.tasks_created.load(Ordering::Relaxed),
            tasks_started: self.tasks_started.load(Ordering::Relaxed),
            tasks_completed: self.tasks_completed.load(Ordering::Relaxed),
            tasks_failed: self.tasks_failed.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            rows_processed: self.rows_processed.load(Ordering::Relaxed),
            files_written: self.files_written.load(Ordering::Relaxed),
            total_execution_time_us: self.total_execution_time_us.load(Ordering::Relaxed),
            commits_attempted: self.commits_attempted.load(Ordering::Relaxed),
            commits_succeeded: self.commits_succeeded.load(Ordering::Relaxed),
            commits_failed: self.commits_failed.load(Ordering::Relaxed),
        }
    }

    /// Formats metrics as Prometheus exposition format.
    pub fn to_prometheus(&self) -> String {
        let snap = self.snapshot();
        format!(
            r#"# HELP compaction_tables_scanned Total tables scanned for compaction
# TYPE compaction_tables_scanned counter
compaction_tables_scanned {}

# HELP compaction_files_selected Total files selected for compaction
# TYPE compaction_files_selected counter
compaction_files_selected {}

# HELP compaction_tasks_total Total compaction tasks by status
# TYPE compaction_tasks_total counter
compaction_tasks_total{{status="created"}} {}
compaction_tasks_total{{status="started"}} {}
compaction_tasks_total{{status="completed"}} {}
compaction_tasks_total{{status="failed"}} {}

# HELP compaction_bytes_total Bytes processed
# TYPE compaction_bytes_total counter
compaction_bytes_total{{direction="read"}} {}
compaction_bytes_total{{direction="written"}} {}

# HELP compaction_rows_processed Total rows processed
# TYPE compaction_rows_processed counter
compaction_rows_processed {}

# HELP compaction_files_written Total output files written
# TYPE compaction_files_written counter
compaction_files_written {}

# HELP compaction_execution_time_seconds Total execution time
# TYPE compaction_execution_time_seconds counter
compaction_execution_time_seconds {}

# HELP compaction_commits_total Commit operations by status
# TYPE compaction_commits_total counter
compaction_commits_total{{status="attempted"}} {}
compaction_commits_total{{status="succeeded"}} {}
compaction_commits_total{{status="failed"}} {}
"#,
            snap.tables_scanned,
            snap.files_selected,
            snap.tasks_created,
            snap.tasks_started,
            snap.tasks_completed,
            snap.tasks_failed,
            snap.bytes_read,
            snap.bytes_written,
            snap.rows_processed,
            snap.files_written,
            snap.total_execution_time_us as f64 / 1_000_000.0,
            snap.commits_attempted,
            snap.commits_succeeded,
            snap.commits_failed,
        )
    }
}

/// A point-in-time snapshot of metrics.
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub tables_scanned: u64,
    pub files_selected: u64,
    pub tasks_created: u64,
    pub tasks_started: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub rows_processed: u64,
    pub files_written: u64,
    pub total_execution_time_us: u64,
    pub commits_attempted: u64,
    pub commits_succeeded: u64,
    pub commits_failed: u64,
}

impl MetricsSnapshot {
    /// Returns the task success rate.
    pub fn success_rate(&self) -> f64 {
        let total = self.tasks_completed + self.tasks_failed;
        if total == 0 {
            1.0
        } else {
            self.tasks_completed as f64 / total as f64
        }
    }

    /// Returns the compression ratio (bytes written / bytes read).
    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_read == 0 {
            1.0
        } else {
            self.bytes_written as f64 / self.bytes_read as f64
        }
    }

    /// Returns average execution time per task.
    pub fn avg_task_time(&self) -> Duration {
        if self.tasks_completed == 0 {
            Duration::ZERO
        } else {
            Duration::from_micros(self.total_execution_time_us / self.tasks_completed)
        }
    }
}

/// Timer guard for measuring operation duration.
pub struct Timer {
    start: Instant,
    metrics: Arc<CompactionMetrics>,
    record_fn: fn(&CompactionMetrics, Duration),
}

impl Timer {
    /// Starts a timer for execution time.
    pub fn execution(metrics: Arc<CompactionMetrics>) -> Self {
        Self {
            start: Instant::now(),
            metrics,
            record_fn: |m, d| m.record_execution_time(d),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        (self.record_fn)(&self.metrics, self.start.elapsed());
    }
}

/// Tracing span names for consistent instrumentation.
pub mod spans {
    pub const PLAN_COMPACTION: &str = "plan_compaction";
    pub const EXECUTE_TASK: &str = "execute_task";
    pub const READ_DATA: &str = "read_data";
    pub const WRITE_OUTPUT: &str = "write_output";
    pub const COMMIT_CHANGES: &str = "commit_changes";
    pub const BUILD_SQL: &str = "build_sql";
    pub const REGISTER_TABLES: &str = "register_tables";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_recording() {
        let metrics = CompactionMetrics::new();

        metrics.record_bytes_read(1000);
        metrics.record_bytes_written(500);
        metrics.record_task_complete(true);
        metrics.record_task_complete(false);

        let snap = metrics.snapshot();
        assert_eq!(snap.bytes_read, 1000);
        assert_eq!(snap.bytes_written, 500);
        assert_eq!(snap.tasks_completed, 1);
        assert_eq!(snap.tasks_failed, 1);
    }

    #[test]
    fn test_success_rate() {
        let snap = MetricsSnapshot {
            tasks_completed: 9,
            tasks_failed: 1,
            ..Default::default()
        };
        assert!((snap.success_rate() - 0.9).abs() < 0.001);
    }

    #[test]
    fn test_prometheus_format() {
        let metrics = CompactionMetrics::new();
        metrics.record_bytes_read(1000);

        let output = metrics.to_prometheus();
        assert!(output.contains("compaction_bytes_total{direction=\"read\"} 1000"));
    }
}
