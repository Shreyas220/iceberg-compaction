/*
 * Table Provider Abstraction
 *
 * Provides a clean interface for accessing Iceberg tables.
 * This abstraction allows the planner to work with tables from different catalogs.
 */

use async_trait::async_trait;
use compaction_common::{CompactionError, Result};
use iceberg::table::Table;
use iceberg::Catalog;
use std::sync::Arc;

/// Trait for providing access to Iceberg tables.
#[async_trait]
pub trait TableProvider: Send + Sync {
    /// Loads a table by its identifier.
    async fn load_table(&self, identifier: &str) -> Result<Table>;

    /// Lists all tables in a namespace.
    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>>;
}

/// Generic Iceberg table provider that works with any catalog.
pub struct IcebergTableProvider<C: Catalog> {
    catalog: Arc<C>,
}

impl<C: Catalog + 'static> IcebergTableProvider<C> {
    /// Creates a new provider with the given catalog.
    pub fn new(catalog: C) -> Self {
        Self {
            catalog: Arc::new(catalog),
        }
    }

    /// Creates a new provider with an Arc'd catalog.
    pub fn with_arc(catalog: Arc<C>) -> Self {
        Self { catalog }
    }

    /// Returns a reference to the underlying catalog.
    pub fn catalog(&self) -> &C {
        &self.catalog
    }
}

#[async_trait]
impl<C: Catalog + Send + Sync + 'static> TableProvider for IcebergTableProvider<C> {
    async fn load_table(&self, identifier: &str) -> Result<Table> {
        // Parse identifier: "namespace.table" or "ns1.ns2.table"
        let parts: Vec<&str> = identifier.split('.').collect();
        if parts.len() < 2 {
            return Err(CompactionError::Planning(format!(
                "Invalid table identifier: {}. Expected format: namespace.table",
                identifier
            )));
        }

        let table_name = parts.last().unwrap();
        let namespace_parts: Vec<String> = parts[..parts.len() - 1]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let namespace = iceberg::NamespaceIdent::from_vec(namespace_parts)
            .map_err(|e| CompactionError::Planning(e.to_string()))?;

        let table_ident = iceberg::TableIdent::new(namespace, table_name.to_string());

        self.catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| CompactionError::Iceberg(e))
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let parts: Vec<String> = namespace.split('.').map(|s| s.to_string()).collect();
        let namespace_ident = iceberg::NamespaceIdent::from_vec(parts)
            .map_err(|e| CompactionError::Planning(e.to_string()))?;

        let tables = self
            .catalog
            .list_tables(&namespace_ident)
            .await
            .map_err(|e| CompactionError::Iceberg(e))?;

        Ok(tables.iter().map(|t| t.to_string()).collect())
    }
}

/// In-memory table provider for testing.
#[cfg(test)]
pub struct MockTableProvider {
    tables: std::collections::HashMap<String, Table>,
}

#[cfg(test)]
impl MockTableProvider {
    pub fn new() -> Self {
        Self {
            tables: std::collections::HashMap::new(),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl TableProvider for MockTableProvider {
    async fn load_table(&self, identifier: &str) -> Result<Table> {
        self.tables
            .get(identifier)
            .cloned()
            .ok_or_else(|| CompactionError::Planning(format!("Table not found: {}", identifier)))
    }

    async fn list_tables(&self, _namespace: &str) -> Result<Vec<String>> {
        Ok(self.tables.keys().cloned().collect())
    }
}
