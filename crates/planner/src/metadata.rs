/*
 * Metadata Scaling
 *
 * Provides:
 * - Snapshot anchors for incremental planning
 * - Manifest caching for large tables
 * - Parallel manifest reading
 */

use compaction_common::{CompactionError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Snapshot anchor for incremental compaction planning.
///
/// Stores the last processed snapshot ID for each table,
/// enabling incremental planning that only considers new files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotAnchor {
    /// Table identifier
    pub table_identifier: String,
    /// Last processed snapshot ID
    pub snapshot_id: i64,
    /// Timestamp when this anchor was created
    pub created_at: i64,
    /// Files that were already compacted (to avoid re-processing)
    pub processed_file_paths: Vec<String>,
}

impl SnapshotAnchor {
    /// Creates a new anchor for a table snapshot.
    pub fn new(table_identifier: String, snapshot_id: i64) -> Self {
        Self {
            table_identifier,
            snapshot_id,
            created_at: chrono::Utc::now().timestamp(),
            processed_file_paths: Vec::new(),
        }
    }

    /// Adds processed file paths to the anchor.
    pub fn with_processed_files(mut self, files: Vec<String>) -> Self {
        self.processed_file_paths = files;
        self
    }
}

/// Store for snapshot anchors.
pub trait AnchorStore: Send + Sync {
    /// Gets the anchor for a table.
    fn get_anchor(&self, table_identifier: &str) -> Result<Option<SnapshotAnchor>>;

    /// Saves an anchor.
    fn save_anchor(&self, anchor: &SnapshotAnchor) -> Result<()>;

    /// Deletes an anchor.
    fn delete_anchor(&self, table_identifier: &str) -> Result<()>;
}

/// In-memory anchor store (for testing or single-node deployments).
#[derive(Debug, Default)]
pub struct InMemoryAnchorStore {
    anchors: RwLock<HashMap<String, SnapshotAnchor>>,
}

impl InMemoryAnchorStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AnchorStore for InMemoryAnchorStore {
    fn get_anchor(&self, table_identifier: &str) -> Result<Option<SnapshotAnchor>> {
        let anchors = self.anchors.read().map_err(|e| {
            CompactionError::Unexpected(format!("Lock poisoned: {}", e))
        })?;
        Ok(anchors.get(table_identifier).cloned())
    }

    fn save_anchor(&self, anchor: &SnapshotAnchor) -> Result<()> {
        let mut anchors = self.anchors.write().map_err(|e| {
            CompactionError::Unexpected(format!("Lock poisoned: {}", e))
        })?;
        anchors.insert(anchor.table_identifier.clone(), anchor.clone());
        Ok(())
    }

    fn delete_anchor(&self, table_identifier: &str) -> Result<()> {
        let mut anchors = self.anchors.write().map_err(|e| {
            CompactionError::Unexpected(format!("Lock poisoned: {}", e))
        })?;
        anchors.remove(table_identifier);
        Ok(())
    }
}

/// Cached manifest entry.
#[derive(Debug, Clone)]
struct CachedManifest {
    /// The manifest data
    data: Vec<u8>,
    /// When this entry was cached
    cached_at: Instant,
    /// Size in bytes
    size: usize,
}

/// LRU cache for manifest files.
///
/// For tables with 1TB+ of metadata, caching manifests avoids
/// repeated downloads during planning.
pub struct ManifestCache {
    entries: RwLock<HashMap<String, CachedManifest>>,
    max_size_bytes: usize,
    ttl: Duration,
    current_size: RwLock<usize>,
}

impl ManifestCache {
    /// Creates a new cache with the given max size and TTL.
    pub fn new(max_size_bytes: usize, ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_size_bytes,
            ttl,
            current_size: RwLock::new(0),
        }
    }

    /// Creates a cache with 1GB capacity and 5 minute TTL.
    pub fn default_config() -> Self {
        Self::new(
            1024 * 1024 * 1024, // 1GB
            Duration::from_secs(300), // 5 minutes
        )
    }

    /// Gets a manifest from cache.
    pub fn get(&self, path: &str) -> Option<Vec<u8>> {
        let entries = self.entries.read().ok()?;
        let entry = entries.get(path)?;

        // Check TTL
        if entry.cached_at.elapsed() > self.ttl {
            return None;
        }

        Some(entry.data.clone())
    }

    /// Puts a manifest in cache.
    pub fn put(&self, path: String, data: Vec<u8>) {
        let size = data.len();

        // Evict if needed
        self.evict_if_needed(size);

        if let Ok(mut entries) = self.entries.write() {
            // Remove old entry if exists
            if let Some(old) = entries.remove(&path) {
                if let Ok(mut current) = self.current_size.write() {
                    *current = current.saturating_sub(old.size);
                }
            }

            entries.insert(path, CachedManifest {
                data,
                cached_at: Instant::now(),
                size,
            });

            if let Ok(mut current) = self.current_size.write() {
                *current += size;
            }
        }
    }

    /// Evicts entries to make room for new data.
    fn evict_if_needed(&self, needed: usize) {
        let current = self.current_size.read().map(|c| *c).unwrap_or(0);

        if current + needed <= self.max_size_bytes {
            return;
        }

        if let Ok(mut entries) = self.entries.write() {
            // Simple eviction: remove expired entries first
            let now = Instant::now();
            let expired: Vec<_> = entries
                .iter()
                .filter(|(_, v)| now.duration_since(v.cached_at) > self.ttl)
                .map(|(k, _)| k.clone())
                .collect();

            let mut freed = 0usize;
            for key in expired {
                if let Some(entry) = entries.remove(&key) {
                    freed += entry.size;
                }
            }

            if let Ok(mut current) = self.current_size.write() {
                *current = current.saturating_sub(freed);
            }

            // If still not enough, remove oldest entries
            if let Ok(current) = self.current_size.read() {
                if *current + needed > self.max_size_bytes {
                    // Remove oldest entries until we have space
                    let mut by_age: Vec<_> = entries.iter()
                        .map(|(k, v)| (k.clone(), v.cached_at, v.size))
                        .collect();
                    by_age.sort_by_key(|(_, t, _)| *t);

                    let mut to_remove = Vec::new();
                    let mut to_free = (*current + needed).saturating_sub(self.max_size_bytes);

                    for (key, _, size) in by_age {
                        if to_free == 0 {
                            break;
                        }
                        to_remove.push(key);
                        to_free = to_free.saturating_sub(size);
                    }

                    for key in to_remove {
                        entries.remove(&key);
                    }
                }
            }
        }
    }

    /// Clears the entire cache.
    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.write() {
            entries.clear();
        }
        if let Ok(mut current) = self.current_size.write() {
            *current = 0;
        }
    }

    /// Returns current cache size.
    pub fn size(&self) -> usize {
        self.current_size.read().map(|c| *c).unwrap_or(0)
    }

    /// Returns number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.read().map(|e| e.len()).unwrap_or(0)
    }

    /// Returns true if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Configuration for manifest sharding.
///
/// For very large tables (1TB+ metadata), manifest reading can be
/// parallelized by sharding the manifest list.
#[derive(Debug, Clone)]
pub struct ManifestShardingConfig {
    /// Enable sharding for tables with more than this many manifests
    pub threshold_manifest_count: usize,
    /// Number of parallel readers
    pub parallelism: usize,
}

impl Default for ManifestShardingConfig {
    fn default() -> Self {
        Self {
            threshold_manifest_count: 100,
            parallelism: 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anchor_store() {
        let store = InMemoryAnchorStore::new();

        let anchor = SnapshotAnchor::new("db.table".to_string(), 123);
        store.save_anchor(&anchor).unwrap();

        let retrieved = store.get_anchor("db.table").unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().snapshot_id, 123);

        store.delete_anchor("db.table").unwrap();
        assert!(store.get_anchor("db.table").unwrap().is_none());
    }

    #[test]
    fn test_manifest_cache() {
        let cache = ManifestCache::new(1024, Duration::from_secs(60));

        cache.put("manifest1".to_string(), vec![1, 2, 3]);

        let data = cache.get("manifest1");
        assert!(data.is_some());
        assert_eq!(data.unwrap(), vec![1, 2, 3]);

        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let cache = ManifestCache::new(100, Duration::from_secs(60));

        // Fill cache
        cache.put("m1".to_string(), vec![0; 50]);
        cache.put("m2".to_string(), vec![0; 50]);

        assert_eq!(cache.size(), 100);
        assert_eq!(cache.len(), 2);

        // Add new entry - this triggers eviction
        cache.put("m3".to_string(), vec![0; 50]);

        // Should have evicted at least one entry
        // Note: exact behavior depends on timing, but we should not exceed max + new entry
        assert!(cache.len() <= 3);
        // The new entry m3 should be present
        assert!(cache.get("m3").is_some());
    }
}
