/*
 * SQL Builder for merge-on-read queries.
 *
 * Generates SQL with RIGHT ANTI JOINs to apply deletes.
 * Adapted from iceberg-compaction datafusion_processor.rs
 */

use compaction_common::Result;

use super::{SYS_HIDDEN_FILE_PATH, SYS_HIDDEN_POS, SYS_HIDDEN_SEQ_NUM};

/// Builds merge-on-read SQL queries for compaction.
pub struct SqlBuilder {
    project_columns: Vec<String>,
    data_table_name: String,
    position_delete_table: Option<String>,
    equality_delete_tables: Vec<EqualityDeleteTable>,
}

struct EqualityDeleteTable {
    table_name: String,
    join_columns: Vec<String>,
}

impl SqlBuilder {
    pub fn new() -> Self {
        Self {
            project_columns: vec![],
            data_table_name: "data_table".to_string(),
            position_delete_table: None,
            equality_delete_tables: vec![],
        }
    }

    pub fn with_project_columns(mut self, columns: Vec<String>) -> Self {
        self.project_columns = columns;
        self
    }

    pub fn with_data_table(mut self, name: &str) -> Self {
        self.data_table_name = name.to_string();
        self
    }

    pub fn with_position_deletes(mut self, has_deletes: bool, table_name: &str) -> Self {
        if has_deletes {
            self.position_delete_table = Some(table_name.to_string());
        }
        self
    }

    pub fn with_equality_deletes(mut self, has_deletes: bool, column_sets: Vec<Vec<String>>) -> Self {
        if has_deletes {
            for (idx, columns) in column_sets.into_iter().enumerate() {
                self.equality_delete_tables.push(EqualityDeleteTable {
                    table_name: format!("equality_delete_table_{}", idx),
                    join_columns: columns,
                });
            }
        }
        self
    }

    /// Builds the merge-on-read SQL query.
    pub fn build(self) -> Result<String> {
        let need_seq_num = !self.equality_delete_tables.is_empty();
        let need_file_path_and_pos = self.position_delete_table.is_some();

        // Simple case: no deletes
        if !need_seq_num && !need_file_path_and_pos {
            return Ok(format!(
                "SELECT {} FROM {}",
                self.quote_columns(&self.project_columns),
                self.quote_identifier(&self.data_table_name)
            ));
        }

        // Build internal columns (project + hidden)
        let mut internal_columns = self.project_columns.clone();
        if need_seq_num {
            internal_columns.push(SYS_HIDDEN_SEQ_NUM.to_string());
        }
        if need_file_path_and_pos {
            internal_columns.push(SYS_HIDDEN_FILE_PATH.to_string());
            internal_columns.push(SYS_HIDDEN_POS.to_string());
        }

        // Start with base select
        let mut query = format!(
            "SELECT {} FROM {}",
            self.quote_columns(&internal_columns),
            self.quote_identifier(&self.data_table_name)
        );

        // Add position delete join
        if let Some(pos_table) = &self.position_delete_table {
            let join_condition = format!(
                "{}.{} = {}.{} AND {}.{} = {}.{}",
                self.quote_identifier(&self.data_table_name),
                self.quote_identifier(SYS_HIDDEN_FILE_PATH),
                self.quote_identifier(pos_table),
                self.quote_identifier(SYS_HIDDEN_FILE_PATH),
                self.quote_identifier(&self.data_table_name),
                self.quote_identifier(SYS_HIDDEN_POS),
                self.quote_identifier(pos_table),
                self.quote_identifier(SYS_HIDDEN_POS),
            );

            query = format!(
                "SELECT {} FROM {} RIGHT ANTI JOIN ({}) AS {} ON {}",
                self.quote_columns(&internal_columns),
                self.quote_identifier(pos_table),
                query,
                self.quote_identifier(&self.data_table_name),
                join_condition
            );
        }

        // Add equality delete joins
        for eq_table in &self.equality_delete_tables {
            let mut conditions: Vec<String> = eq_table
                .join_columns
                .iter()
                .map(|col| {
                    format!(
                        "{}.{} = {}.{}",
                        self.quote_identifier(&eq_table.table_name),
                        self.quote_identifier(col),
                        self.quote_identifier(&self.data_table_name),
                        self.quote_identifier(col)
                    )
                })
                .collect();

            // Add sequence number condition
            conditions.push(format!(
                "{}.{} < {}.{}",
                self.quote_identifier(&self.data_table_name),
                self.quote_identifier(SYS_HIDDEN_SEQ_NUM),
                self.quote_identifier(&eq_table.table_name),
                self.quote_identifier(SYS_HIDDEN_SEQ_NUM),
            ));

            query = format!(
                "SELECT {} FROM {} RIGHT ANTI JOIN ({}) AS {} ON {}",
                self.quote_columns(&internal_columns),
                self.quote_identifier(&eq_table.table_name),
                query,
                self.quote_identifier(&self.data_table_name),
                conditions.join(" AND ")
            );
        }

        // Final select to remove hidden columns
        if need_seq_num || need_file_path_and_pos {
            query = format!(
                "SELECT {} FROM ({}) AS {}",
                self.quote_columns(&self.project_columns),
                query,
                self.quote_identifier("final_result")
            );
        }

        Ok(query)
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn quote_columns(&self, columns: &[String]) -> String {
        columns
            .iter()
            .map(|c| self.quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Default for SqlBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_deletes() {
        let sql = SqlBuilder::new()
            .with_project_columns(vec!["id".into(), "name".into()])
            .with_data_table("data")
            .build()
            .unwrap();

        assert_eq!(sql, r#"SELECT "id", "name" FROM "data""#);
    }

    #[test]
    fn test_position_deletes() {
        let sql = SqlBuilder::new()
            .with_project_columns(vec!["id".into(), "name".into()])
            .with_data_table("data")
            .with_position_deletes(true, "pos_del")
            .build()
            .unwrap();

        assert!(sql.contains("RIGHT ANTI JOIN"));
        assert!(sql.contains("sys_hidden_file_path"));
        assert!(sql.contains("sys_hidden_pos"));
    }

    #[test]
    fn test_equality_deletes() {
        let sql = SqlBuilder::new()
            .with_project_columns(vec!["id".into(), "name".into()])
            .with_data_table("data")
            .with_equality_deletes(true, vec![vec!["id".into()]])
            .build()
            .unwrap();

        assert!(sql.contains("RIGHT ANTI JOIN"));
        assert!(sql.contains("sys_hidden_seq_num"));
    }
}
