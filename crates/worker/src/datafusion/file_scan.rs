/*
 * File Scan Provider for DataFusion
 *
 * Registers Parquet files as DataFusion tables with hidden columns
 * for merge-on-read delete processing.
 */

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use compaction_common::{CompactionError, FileMetadata, Result};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

use super::{SYS_HIDDEN_FILE_PATH, SYS_HIDDEN_POS, SYS_HIDDEN_SEQ_NUM};

/// Registers data files as a DataFusion table.
pub struct FileScanProvider;

impl FileScanProvider {
    /// Registers data files as a table with optional hidden columns.
    ///
    /// Hidden columns added based on delete type:
    /// - Position deletes: sys_hidden_file_path, sys_hidden_pos
    /// - Equality deletes: sys_hidden_seq_num
    pub async fn register_data_table(
        ctx: &SessionContext,
        table_name: &str,
        files: &[FileMetadata],
        schema: SchemaRef,
        add_position_columns: bool,
        add_sequence_column: bool,
    ) -> Result<()> {
        if files.is_empty() {
            return Err(CompactionError::Execution("No files to register".to_string()));
        }

        // Build schema with hidden columns
        let extended_schema = Self::extend_schema_with_hidden_columns(
            &schema,
            add_position_columns,
            add_sequence_column,
        );

        // Get the directory containing the files
        // We'll use listing table pointed at the data directory
        let _file_paths: Vec<&str> = files.iter().map(|f| f.file_path.as_str()).collect();

        // For now, we use a simple approach: register each file
        // A more optimized approach would use a custom TableProvider
        let format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(format))
            .with_file_extension(".parquet");

        // Use the first file's directory as the table path
        let first_path = &files[0].file_path;
        let table_path = Self::get_directory_path(first_path)?;

        let table_url = ListingTableUrl::parse(&table_path)
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(extended_schema);

        let table = ListingTable::try_new(config)
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        ctx.register_table(table_name, Arc::new(table))
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        tracing::debug!(
            "Registered table '{}' with {} files",
            table_name,
            files.len()
        );

        Ok(())
    }

    /// Registers position delete files as a table.
    ///
    /// Position delete files have schema: (file_path: string, pos: long)
    pub async fn register_position_delete_table(
        ctx: &SessionContext,
        table_name: &str,
        files: &[FileMetadata],
    ) -> Result<()> {
        if files.is_empty() {
            return Ok(()); // No deletes to register
        }

        // Position delete schema
        let schema = Arc::new(Schema::new(vec![
            Field::new(SYS_HIDDEN_FILE_PATH, DataType::Utf8, false),
            Field::new(SYS_HIDDEN_POS, DataType::Int64, false),
        ]));

        let format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(format))
            .with_file_extension(".parquet");

        let first_path = &files[0].file_path;
        let table_path = Self::get_directory_path(first_path)?;

        let table_url = ListingTableUrl::parse(&table_path)
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table = ListingTable::try_new(config)
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        ctx.register_table(table_name, Arc::new(table))
            .map_err(|e| CompactionError::Execution(e.to_string()))?;

        tracing::debug!(
            "Registered position delete table '{}' with {} files",
            table_name,
            files.len()
        );

        Ok(())
    }

    /// Registers equality delete files as tables.
    ///
    /// Each equality delete file becomes its own table with the equality columns + seq_num.
    pub async fn register_equality_delete_tables(
        ctx: &SessionContext,
        table_name_prefix: &str,
        files: &[FileMetadata],
        base_schema: &Schema,
    ) -> Result<Vec<String>> {
        let mut table_names = Vec::new();

        for (idx, file) in files.iter().enumerate() {
            let table_name = format!("{}_{}", table_name_prefix, idx);

            // Get equality column ids
            let eq_ids = file.equality_ids.as_ref().ok_or_else(|| {
                CompactionError::Execution("Equality delete file missing equality_ids".to_string())
            })?;

            // Build schema with just the equality columns + seq_num
            let mut fields: Vec<Field> = eq_ids
                .iter()
                .filter_map(|id| {
                    base_schema.field_with_name(&format!("field_{}", id)).ok()
                        .or_else(|| {
                            // Try to find by index (field IDs in Iceberg)
                            base_schema.fields().get(*id as usize).map(|f| f.as_ref())
                        })
                })
                .cloned()
                .collect();

            fields.push(Field::new(SYS_HIDDEN_SEQ_NUM, DataType::Int64, false));

            let schema = Arc::new(Schema::new(fields));

            let format = ParquetFormat::default();
            let listing_options = ListingOptions::new(Arc::new(format))
                .with_file_extension(".parquet");

            let table_path = Self::get_directory_path(&file.file_path)?;

            let table_url = ListingTableUrl::parse(&table_path)
                .map_err(|e| CompactionError::Execution(e.to_string()))?;

            let config = ListingTableConfig::new(table_url)
                .with_listing_options(listing_options)
                .with_schema(schema);

            let table = ListingTable::try_new(config)
                .map_err(|e| CompactionError::Execution(e.to_string()))?;

            ctx.register_table(&table_name, Arc::new(table))
                .map_err(|e| CompactionError::Execution(e.to_string()))?;

            table_names.push(table_name);
        }

        tracing::debug!(
            "Registered {} equality delete tables",
            table_names.len()
        );

        Ok(table_names)
    }

    /// Extends a schema with hidden columns for delete processing.
    fn extend_schema_with_hidden_columns(
        schema: &Schema,
        add_position_columns: bool,
        add_sequence_column: bool,
    ) -> SchemaRef {
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();

        if add_sequence_column {
            fields.push(Field::new(SYS_HIDDEN_SEQ_NUM, DataType::Int64, false));
        }

        if add_position_columns {
            fields.push(Field::new(SYS_HIDDEN_FILE_PATH, DataType::Utf8, false));
            fields.push(Field::new(SYS_HIDDEN_POS, DataType::Int64, false));
        }

        Arc::new(Schema::new(fields))
    }

    /// Extracts the directory path from a file path.
    fn get_directory_path(file_path: &str) -> Result<String> {
        let path = std::path::Path::new(file_path);
        let parent = path.parent().ok_or_else(|| {
            CompactionError::Execution(format!("Cannot get parent directory of {}", file_path))
        })?;
        Ok(parent.to_string_lossy().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extend_schema_position() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let extended = FileScanProvider::extend_schema_with_hidden_columns(
            &schema,
            true,  // position columns
            false, // no seq column
        );

        assert_eq!(extended.fields().len(), 4);
        assert!(extended.field_with_name(SYS_HIDDEN_FILE_PATH).is_ok());
        assert!(extended.field_with_name(SYS_HIDDEN_POS).is_ok());
    }

    #[test]
    fn test_extend_schema_both() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]);

        let extended = FileScanProvider::extend_schema_with_hidden_columns(
            &schema,
            true, // position columns
            true, // seq column
        );

        assert_eq!(extended.fields().len(), 4); // id + seq + filepath + pos
    }
}
