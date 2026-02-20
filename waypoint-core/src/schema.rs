//! PostgreSQL schema introspection, diff, and DDL generation.
//!
//! Used by diff, drift, and snapshot commands.

use serde::Serialize;
use tokio_postgres::Client;

use crate::db::quote_ident;
use crate::error::Result;

/// Complete snapshot of a PostgreSQL schema.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SchemaSnapshot {
    /// All base tables in the schema.
    pub tables: Vec<TableDef>,
    /// All views (regular and materialized) in the schema.
    pub views: Vec<ViewDef>,
    /// All indexes in the schema.
    pub indexes: Vec<IndexDef>,
    /// All sequences in the schema.
    pub sequences: Vec<SequenceDef>,
    /// All functions and procedures in the schema.
    pub functions: Vec<FunctionDef>,
    /// All enum types in the schema.
    pub enums: Vec<EnumDef>,
    /// All table constraints in the schema.
    pub constraints: Vec<ConstraintDef>,
    /// All triggers in the schema.
    pub triggers: Vec<TriggerDef>,
    /// Names of installed extensions (excluding plpgsql).
    pub extensions: Vec<String>,
}

/// Definition of a database table.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TableDef {
    /// Schema the table belongs to.
    pub schema: String,
    /// Name of the table.
    pub name: String,
    /// Columns belonging to this table.
    pub columns: Vec<ColumnDef>,
}

/// Definition of a table column.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ColumnDef {
    /// Name of the column.
    pub name: String,
    /// SQL data type of the column.
    pub data_type: String,
    /// Whether the column allows NULL values.
    pub is_nullable: bool,
    /// Default value expression, if any.
    pub default: Option<String>,
    /// Position of the column within its table (1-based).
    pub ordinal_position: i32,
}

/// Definition of a database view.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ViewDef {
    /// Schema the view belongs to.
    pub schema: String,
    /// Name of the view.
    pub name: String,
    /// SQL definition body of the view.
    pub definition: String,
    /// Whether this is a materialized view.
    pub is_materialized: bool,
}

/// Definition of a database index.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct IndexDef {
    /// Schema the index belongs to.
    pub schema: String,
    /// Name of the index.
    pub name: String,
    /// Name of the table the index is built on.
    pub table_name: String,
    /// Full CREATE INDEX DDL statement.
    pub definition: String,
    /// Whether this is a unique index.
    pub is_unique: bool,
}

/// Definition of a database sequence.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SequenceDef {
    /// Schema the sequence belongs to.
    pub schema: String,
    /// Name of the sequence.
    pub name: String,
    /// Data type of the sequence (e.g. bigint).
    pub data_type: String,
}

/// Definition of a database function or procedure.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FunctionDef {
    /// Schema the function belongs to.
    pub schema: String,
    /// Name of the function.
    pub name: String,
    /// Function argument signature.
    pub arguments: String,
    /// Return type of the function.
    pub return_type: String,
    /// Implementation language (e.g. plpgsql, sql).
    pub language: String,
    /// Full function definition body.
    pub definition: String,
}

/// Definition of a PostgreSQL enum type.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EnumDef {
    /// Schema the enum belongs to.
    pub schema: String,
    /// Name of the enum type.
    pub name: String,
    /// Ordered list of enum label values.
    pub values: Vec<String>,
}

/// Definition of a table constraint.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConstraintDef {
    /// Schema the constraint belongs to.
    pub schema: String,
    /// Name of the table the constraint is on.
    pub table_name: String,
    /// Name of the constraint.
    pub name: String,
    /// Type of constraint (e.g. PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK).
    pub constraint_type: String,
    /// Full constraint definition expression.
    pub definition: String,
}

/// Definition of a database trigger.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TriggerDef {
    /// Schema the trigger belongs to.
    pub schema: String,
    /// Name of the table the trigger is attached to.
    pub table_name: String,
    /// Name of the trigger.
    pub name: String,
    /// Action statement executed by the trigger.
    pub definition: String,
}

/// Differences between two schema snapshots.
#[derive(Debug, Clone, Serialize)]
pub enum SchemaDiff {
    /// A table was added in the target schema.
    TableAdded(TableDef),
    /// A table was dropped from the target schema.
    TableDropped(String),
    /// A column was added to an existing table.
    ColumnAdded { table: String, column: ColumnDef },
    /// A column was dropped from an existing table.
    ColumnDropped { table: String, column: String },
    /// A column definition was altered in an existing table.
    ColumnAltered { table: String, column: String, from: ColumnDef, to: ColumnDef },
    /// An index was added in the target schema.
    IndexAdded(IndexDef),
    /// An index was dropped from the target schema.
    IndexDropped(String),
    /// A view was added in the target schema.
    ViewAdded(ViewDef),
    /// A view was dropped from the target schema.
    ViewDropped(String),
    /// A view definition was altered.
    ViewAltered { name: String, from: String, to: String },
    /// A sequence was added in the target schema.
    SequenceAdded(SequenceDef),
    /// A sequence was dropped from the target schema.
    SequenceDropped(String),
    /// A function was added in the target schema.
    FunctionAdded(FunctionDef),
    /// A function was dropped from the target schema.
    FunctionDropped(String),
    /// A function definition was altered.
    FunctionAltered { name: String },
    /// An enum type was added in the target schema.
    EnumAdded(EnumDef),
    /// An enum type was dropped from the target schema.
    EnumDropped(String),
    /// A constraint was added in the target schema.
    ConstraintAdded(ConstraintDef),
    /// A constraint was dropped from the target schema.
    ConstraintDropped { table: String, name: String },
    /// A trigger was added in the target schema.
    TriggerAdded(TriggerDef),
    /// A trigger was dropped from the target schema.
    TriggerDropped { table: String, name: String },
    /// A PostgreSQL extension was added.
    ExtensionAdded(String),
    /// A PostgreSQL extension was dropped.
    ExtensionDropped(String),
}

impl std::fmt::Display for SchemaDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaDiff::TableAdded(t) => write!(f, "+ TABLE {}", t.name),
            SchemaDiff::TableDropped(n) => write!(f, "- TABLE {}", n),
            SchemaDiff::ColumnAdded { table, column } => {
                write!(f, "+ COLUMN {}.{} ({})", table, column.name, column.data_type)
            }
            SchemaDiff::ColumnDropped { table, column } => {
                write!(f, "- COLUMN {}.{}", table, column)
            }
            SchemaDiff::ColumnAltered { table, column, .. } => {
                write!(f, "~ COLUMN {}.{}", table, column)
            }
            SchemaDiff::IndexAdded(idx) => write!(f, "+ INDEX {}", idx.name),
            SchemaDiff::IndexDropped(n) => write!(f, "- INDEX {}", n),
            SchemaDiff::ViewAdded(v) => write!(f, "+ VIEW {}", v.name),
            SchemaDiff::ViewDropped(n) => write!(f, "- VIEW {}", n),
            SchemaDiff::ViewAltered { name, .. } => write!(f, "~ VIEW {}", name),
            SchemaDiff::SequenceAdded(s) => write!(f, "+ SEQUENCE {}", s.name),
            SchemaDiff::SequenceDropped(n) => write!(f, "- SEQUENCE {}", n),
            SchemaDiff::FunctionAdded(func) => write!(f, "+ FUNCTION {}", func.name),
            SchemaDiff::FunctionDropped(n) => write!(f, "- FUNCTION {}", n),
            SchemaDiff::FunctionAltered { name } => write!(f, "~ FUNCTION {}", name),
            SchemaDiff::EnumAdded(e) => write!(f, "+ TYPE {} (enum)", e.name),
            SchemaDiff::EnumDropped(n) => write!(f, "- TYPE {} (enum)", n),
            SchemaDiff::ConstraintAdded(c) => {
                write!(f, "+ CONSTRAINT {} ON {}", c.name, c.table_name)
            }
            SchemaDiff::ConstraintDropped { table, name } => {
                write!(f, "- CONSTRAINT {} ON {}", name, table)
            }
            SchemaDiff::TriggerAdded(t) => write!(f, "+ TRIGGER {} ON {}", t.name, t.table_name),
            SchemaDiff::TriggerDropped { table, name } => {
                write!(f, "- TRIGGER {} ON {}", name, table)
            }
            SchemaDiff::ExtensionAdded(n) => write!(f, "+ EXTENSION {}", n),
            SchemaDiff::ExtensionDropped(n) => write!(f, "- EXTENSION {}", n),
        }
    }
}

/// Introspect the current state of a PostgreSQL schema.
pub async fn introspect(client: &Client, schema: &str) -> Result<SchemaSnapshot> {
    let tables = introspect_tables(client, schema).await?;
    let views = introspect_views(client, schema).await?;
    let indexes = introspect_indexes(client, schema).await?;
    let sequences = introspect_sequences(client, schema).await?;
    let functions = introspect_functions(client, schema).await?;
    let enums = introspect_enums(client, schema).await?;
    let constraints = introspect_constraints(client, schema).await?;
    let triggers = introspect_triggers(client, schema).await?;
    let extensions = introspect_extensions(client).await?;

    Ok(SchemaSnapshot {
        tables,
        views,
        indexes,
        sequences,
        functions,
        enums,
        constraints,
        triggers,
        extensions,
    })
}

async fn introspect_tables(client: &Client, schema: &str) -> Result<Vec<TableDef>> {
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables
             WHERE table_schema = $1 AND table_type = 'BASE TABLE'
             ORDER BY table_name",
            &[&schema],
        )
        .await?;

    let mut tables = Vec::new();
    for row in &rows {
        let table_name: String = row.get(0);

        let col_rows = client
            .query(
                "SELECT column_name, data_type, is_nullable, column_default, ordinal_position
                 FROM information_schema.columns
                 WHERE table_schema = $1 AND table_name = $2
                 ORDER BY ordinal_position",
                &[&schema, &table_name],
            )
            .await?;

        let columns = col_rows
            .iter()
            .map(|r| ColumnDef {
                name: r.get(0),
                data_type: r.get(1),
                is_nullable: r.get::<_, String>(2) == "YES",
                default: r.get(3),
                ordinal_position: r.get(4),
            })
            .collect();

        tables.push(TableDef {
            schema: schema.to_string(),
            name: table_name,
            columns,
        });
    }

    Ok(tables)
}

async fn introspect_views(client: &Client, schema: &str) -> Result<Vec<ViewDef>> {
    // Regular views
    let rows = client
        .query(
            "SELECT table_name, view_definition
             FROM information_schema.views
             WHERE table_schema = $1
             ORDER BY table_name",
            &[&schema],
        )
        .await?;

    let mut views: Vec<ViewDef> = rows
        .iter()
        .map(|r| ViewDef {
            schema: schema.to_string(),
            name: r.get(0),
            definition: r.get::<_, Option<String>>(1).unwrap_or_default(),
            is_materialized: false,
        })
        .collect();

    // Materialized views
    let mat_rows = client
        .query(
            "SELECT c.relname, pg_get_viewdef(c.oid)
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relkind = 'm'
             ORDER BY c.relname",
            &[&schema],
        )
        .await?;

    for r in &mat_rows {
        views.push(ViewDef {
            schema: schema.to_string(),
            name: r.get(0),
            definition: r.get::<_, Option<String>>(1).unwrap_or_default(),
            is_materialized: true,
        });
    }

    Ok(views)
}

async fn introspect_indexes(client: &Client, schema: &str) -> Result<Vec<IndexDef>> {
    let rows = client
        .query(
            "SELECT indexname, tablename, indexdef
             FROM pg_indexes
             WHERE schemaname = $1
             ORDER BY indexname",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| {
            let definition: String = r.get(2);
            IndexDef {
                schema: schema.to_string(),
                name: r.get(0),
                table_name: r.get(1),
                is_unique: definition.to_uppercase().contains("UNIQUE"),
                definition,
            }
        })
        .collect())
}

async fn introspect_sequences(client: &Client, schema: &str) -> Result<Vec<SequenceDef>> {
    let rows = client
        .query(
            "SELECT sequence_name, data_type
             FROM information_schema.sequences
             WHERE sequence_schema = $1
             ORDER BY sequence_name",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| SequenceDef {
            schema: schema.to_string(),
            name: r.get(0),
            data_type: r.get(1),
        })
        .collect())
}

async fn introspect_functions(client: &Client, schema: &str) -> Result<Vec<FunctionDef>> {
    let rows = client
        .query(
            "SELECT p.proname,
                    pg_get_function_arguments(p.oid),
                    pg_get_function_result(p.oid),
                    l.lanname,
                    pg_get_functiondef(p.oid)
             FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace
             JOIN pg_language l ON l.oid = p.prolang
             WHERE n.nspname = $1
               AND p.prokind IN ('f', 'p')
             ORDER BY p.proname",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| FunctionDef {
            schema: schema.to_string(),
            name: r.get(0),
            arguments: r.get(1),
            return_type: r.get::<_, Option<String>>(2).unwrap_or_default(),
            language: r.get(3),
            definition: r.get::<_, Option<String>>(4).unwrap_or_default(),
        })
        .collect())
}

async fn introspect_enums(client: &Client, schema: &str) -> Result<Vec<EnumDef>> {
    let rows = client
        .query(
            "SELECT t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder)::text[]
             FROM pg_type t
             JOIN pg_enum e ON e.enumtypid = t.oid
             JOIN pg_namespace n ON n.oid = t.typnamespace
             WHERE n.nspname = $1
             GROUP BY t.typname
             ORDER BY t.typname",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| EnumDef {
            schema: schema.to_string(),
            name: r.get(0),
            values: r.get(1),
        })
        .collect())
}

async fn introspect_constraints(client: &Client, schema: &str) -> Result<Vec<ConstraintDef>> {
    let rows = client
        .query(
            "SELECT tc.table_name, tc.constraint_name, tc.constraint_type,
                    pg_get_constraintdef(c.oid)
             FROM information_schema.table_constraints tc
             JOIN pg_constraint c ON c.conname = tc.constraint_name
             JOIN pg_namespace n ON n.oid = c.connamespace
             WHERE tc.constraint_schema = $1 AND n.nspname = $1
             ORDER BY tc.table_name, tc.constraint_name",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| ConstraintDef {
            schema: schema.to_string(),
            table_name: r.get(0),
            name: r.get(1),
            constraint_type: r.get(2),
            definition: r.get::<_, Option<String>>(3).unwrap_or_default(),
        })
        .collect())
}

async fn introspect_triggers(client: &Client, schema: &str) -> Result<Vec<TriggerDef>> {
    let rows = client
        .query(
            "SELECT event_object_table, trigger_name, action_statement
             FROM information_schema.triggers
             WHERE trigger_schema = $1
             ORDER BY event_object_table, trigger_name",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| TriggerDef {
            schema: schema.to_string(),
            table_name: r.get(0),
            name: r.get(1),
            definition: r.get(2),
        })
        .collect())
}

async fn introspect_extensions(client: &Client) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname",
            &[],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Compare two schema snapshots and return the differences.
pub fn diff(before: &SchemaSnapshot, after: &SchemaSnapshot) -> Vec<SchemaDiff> {
    let mut diffs = Vec::new();

    // Tables
    for bt in &before.tables {
        if let Some(at) = after.tables.iter().find(|t| t.name == bt.name) {
            diff_columns(&mut diffs, &bt.name, &bt.columns, &at.columns);
        } else {
            diffs.push(SchemaDiff::TableDropped(bt.name.clone()));
        }
    }
    for at in &after.tables {
        if !before.tables.iter().any(|t| t.name == at.name) {
            diffs.push(SchemaDiff::TableAdded(at.clone()));
        }
    }

    // Views
    for bv in &before.views {
        if let Some(av) = after.views.iter().find(|v| v.name == bv.name) {
            if bv.definition != av.definition {
                diffs.push(SchemaDiff::ViewAltered {
                    name: bv.name.clone(),
                    from: bv.definition.clone(),
                    to: av.definition.clone(),
                });
            }
        } else {
            diffs.push(SchemaDiff::ViewDropped(bv.name.clone()));
        }
    }
    for av in &after.views {
        if !before.views.iter().any(|v| v.name == av.name) {
            diffs.push(SchemaDiff::ViewAdded(av.clone()));
        }
    }

    // Indexes
    for bi in &before.indexes {
        if !after.indexes.iter().any(|i| i.name == bi.name) {
            diffs.push(SchemaDiff::IndexDropped(bi.name.clone()));
        }
    }
    for ai in &after.indexes {
        if !before.indexes.iter().any(|i| i.name == ai.name) {
            diffs.push(SchemaDiff::IndexAdded(ai.clone()));
        }
    }

    // Sequences
    for bs in &before.sequences {
        if !after.sequences.iter().any(|s| s.name == bs.name) {
            diffs.push(SchemaDiff::SequenceDropped(bs.name.clone()));
        }
    }
    for a_s in &after.sequences {
        if !before.sequences.iter().any(|s| s.name == a_s.name) {
            diffs.push(SchemaDiff::SequenceAdded(a_s.clone()));
        }
    }

    // Functions
    for bf in &before.functions {
        if let Some(af) = after.functions.iter().find(|f| f.name == bf.name) {
            if bf.definition != af.definition {
                diffs.push(SchemaDiff::FunctionAltered {
                    name: bf.name.clone(),
                });
            }
        } else {
            diffs.push(SchemaDiff::FunctionDropped(bf.name.clone()));
        }
    }
    for af in &after.functions {
        if !before.functions.iter().any(|f| f.name == af.name) {
            diffs.push(SchemaDiff::FunctionAdded(af.clone()));
        }
    }

    // Enums
    for be in &before.enums {
        if !after.enums.iter().any(|e| e.name == be.name) {
            diffs.push(SchemaDiff::EnumDropped(be.name.clone()));
        }
    }
    for ae in &after.enums {
        if !before.enums.iter().any(|e| e.name == ae.name) {
            diffs.push(SchemaDiff::EnumAdded(ae.clone()));
        }
    }

    // Constraints
    for bc in &before.constraints {
        let key = (&bc.table_name, &bc.name);
        if !after.constraints.iter().any(|ac| (&ac.table_name, &ac.name) == key) {
            diffs.push(SchemaDiff::ConstraintDropped {
                table: bc.table_name.clone(),
                name: bc.name.clone(),
            });
        }
    }
    for ac in &after.constraints {
        let key = (&ac.table_name, &ac.name);
        if !before.constraints.iter().any(|bc| (&bc.table_name, &bc.name) == key) {
            diffs.push(SchemaDiff::ConstraintAdded(ac.clone()));
        }
    }

    // Triggers
    for bt in &before.triggers {
        let key = (&bt.table_name, &bt.name);
        if !after.triggers.iter().any(|at| (&at.table_name, &at.name) == key) {
            diffs.push(SchemaDiff::TriggerDropped {
                table: bt.table_name.clone(),
                name: bt.name.clone(),
            });
        }
    }
    for at in &after.triggers {
        let key = (&at.table_name, &at.name);
        if !before.triggers.iter().any(|bt| (&bt.table_name, &bt.name) == key) {
            diffs.push(SchemaDiff::TriggerAdded(at.clone()));
        }
    }

    // Extensions
    for ext in &before.extensions {
        if !after.extensions.contains(ext) {
            diffs.push(SchemaDiff::ExtensionDropped(ext.clone()));
        }
    }
    for ext in &after.extensions {
        if !before.extensions.contains(ext) {
            diffs.push(SchemaDiff::ExtensionAdded(ext.clone()));
        }
    }

    diffs
}

fn diff_columns(diffs: &mut Vec<SchemaDiff>, table: &str, before: &[ColumnDef], after: &[ColumnDef]) {
    for bc in before {
        if let Some(ac) = after.iter().find(|c| c.name == bc.name) {
            if bc != ac {
                diffs.push(SchemaDiff::ColumnAltered {
                    table: table.to_string(),
                    column: bc.name.clone(),
                    from: bc.clone(),
                    to: ac.clone(),
                });
            }
        } else {
            diffs.push(SchemaDiff::ColumnDropped {
                table: table.to_string(),
                column: bc.name.clone(),
            });
        }
    }
    for ac in after {
        if !before.iter().any(|bc| bc.name == ac.name) {
            diffs.push(SchemaDiff::ColumnAdded {
                table: table.to_string(),
                column: ac.clone(),
            });
        }
    }
}

/// Generate DDL statements from schema diffs.
pub fn generate_ddl(diffs: &[SchemaDiff]) -> String {
    let mut statements = Vec::new();

    for d in diffs {
        match d {
            SchemaDiff::TableAdded(t) => {
                let cols: Vec<String> = t
                    .columns
                    .iter()
                    .map(|c| {
                        let mut col = format!("    {} {}", quote_ident(&c.name), c.data_type);
                        if !c.is_nullable {
                            col.push_str(" NOT NULL");
                        }
                        if let Some(ref default) = c.default {
                            col.push_str(&format!(" DEFAULT {}", default));
                        }
                        col
                    })
                    .collect();
                statements.push(format!(
                    "CREATE TABLE {} (\n{}\n);",
                    quote_ident(&t.name),
                    cols.join(",\n")
                ));
            }
            SchemaDiff::TableDropped(name) => {
                statements.push(format!("DROP TABLE IF EXISTS {} CASCADE;", quote_ident(name)));
            }
            SchemaDiff::ColumnAdded { table, column } => {
                let mut stmt = format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    quote_ident(table),
                    quote_ident(&column.name),
                    column.data_type
                );
                if !column.is_nullable {
                    stmt.push_str(" NOT NULL");
                }
                if let Some(ref default) = column.default {
                    stmt.push_str(&format!(" DEFAULT {}", default));
                }
                stmt.push(';');
                statements.push(stmt);
            }
            SchemaDiff::ColumnDropped { table, column } => {
                statements.push(format!(
                    "ALTER TABLE {} DROP COLUMN {};",
                    quote_ident(table),
                    quote_ident(column)
                ));
            }
            SchemaDiff::ColumnAltered { table, column, to, .. } => {
                statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
                    quote_ident(table),
                    quote_ident(column),
                    to.data_type
                ));
                if to.is_nullable {
                    statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;",
                        quote_ident(table),
                        quote_ident(column)
                    ));
                } else {
                    statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                        quote_ident(table),
                        quote_ident(column)
                    ));
                }
                match &to.default {
                    Some(default) => {
                        statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                            quote_ident(table),
                            quote_ident(column),
                            default
                        ));
                    }
                    None => {
                        statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                            quote_ident(table),
                            quote_ident(column)
                        ));
                    }
                }
            }
            SchemaDiff::IndexAdded(idx) => {
                statements.push(format!("{};", idx.definition));
            }
            SchemaDiff::IndexDropped(name) => {
                statements.push(format!("DROP INDEX IF EXISTS {};", quote_ident(name)));
            }
            SchemaDiff::ViewAdded(v) => {
                let keyword = if v.is_materialized {
                    "MATERIALIZED VIEW"
                } else {
                    "VIEW"
                };
                statements.push(format!(
                    "CREATE {} {} AS {};",
                    keyword,
                    quote_ident(&v.name),
                    v.definition.trim_end_matches(';').trim()
                ));
            }
            SchemaDiff::ViewDropped(name) => {
                statements.push(format!("DROP VIEW IF EXISTS {} CASCADE;", quote_ident(name)));
            }
            SchemaDiff::ViewAltered { name, to, .. } => {
                statements.push(format!(
                    "CREATE OR REPLACE VIEW {} AS {};",
                    quote_ident(name),
                    to.trim_end_matches(';').trim()
                ));
            }
            SchemaDiff::SequenceAdded(s) => {
                statements.push(format!("CREATE SEQUENCE {};", quote_ident(&s.name)));
            }
            SchemaDiff::SequenceDropped(name) => {
                statements.push(format!("DROP SEQUENCE IF EXISTS {};", quote_ident(name)));
            }
            SchemaDiff::FunctionAdded(func) => {
                statements.push(format!("{};", func.definition.trim_end_matches(';')));
            }
            SchemaDiff::FunctionDropped(name) => {
                statements.push(format!("DROP FUNCTION IF EXISTS {} CASCADE;", quote_ident(name)));
            }
            SchemaDiff::FunctionAltered { name } => {
                // For altered functions we'd need the full definition; leave a comment
                statements.push(format!("-- Function {} was altered; manual review needed", name));
            }
            SchemaDiff::EnumAdded(e) => {
                let values: Vec<String> = e.values.iter().map(|v| format!("'{}'", v)).collect();
                statements.push(format!(
                    "CREATE TYPE {} AS ENUM ({});",
                    quote_ident(&e.name),
                    values.join(", ")
                ));
            }
            SchemaDiff::EnumDropped(name) => {
                statements.push(format!("DROP TYPE IF EXISTS {} CASCADE;", quote_ident(name)));
            }
            SchemaDiff::ConstraintAdded(c) => {
                statements.push(format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} {};",
                    quote_ident(&c.table_name),
                    quote_ident(&c.name),
                    c.definition
                ));
            }
            SchemaDiff::ConstraintDropped { table, name } => {
                statements.push(format!(
                    "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};",
                    quote_ident(table),
                    quote_ident(name)
                ));
            }
            SchemaDiff::TriggerAdded(t) => {
                statements.push(format!(
                    "-- Trigger {} on {} needs manual creation",
                    t.name, t.table_name
                ));
            }
            SchemaDiff::TriggerDropped { table, name } => {
                statements.push(format!(
                    "DROP TRIGGER IF EXISTS {} ON {};",
                    quote_ident(name),
                    quote_ident(table)
                ));
            }
            SchemaDiff::ExtensionAdded(name) => {
                statements.push(format!("CREATE EXTENSION IF NOT EXISTS {};", quote_ident(name)));
            }
            SchemaDiff::ExtensionDropped(name) => {
                statements.push(format!("DROP EXTENSION IF EXISTS {};", quote_ident(name)));
            }
        }
    }

    statements.join("\n\n")
}

/// Generate full DDL to recreate a schema from a snapshot.
pub fn to_ddl(snapshot: &SchemaSnapshot) -> String {
    let mut statements = Vec::new();

    // Extensions first
    for ext in &snapshot.extensions {
        statements.push(format!("CREATE EXTENSION IF NOT EXISTS {};", quote_ident(ext)));
    }

    // Enums before tables (types must exist for columns)
    for e in &snapshot.enums {
        let values: Vec<String> = e.values.iter().map(|v| format!("'{}'", v)).collect();
        statements.push(format!(
            "CREATE TYPE {} AS ENUM ({});",
            quote_ident(&e.name),
            values.join(", ")
        ));
    }

    // Sequences
    for s in &snapshot.sequences {
        statements.push(format!("CREATE SEQUENCE {};", quote_ident(&s.name)));
    }

    // Tables
    for t in &snapshot.tables {
        let cols: Vec<String> = t
            .columns
            .iter()
            .map(|c| {
                let mut col = format!("    {} {}", quote_ident(&c.name), c.data_type);
                if !c.is_nullable {
                    col.push_str(" NOT NULL");
                }
                if let Some(ref default) = c.default {
                    col.push_str(&format!(" DEFAULT {}", default));
                }
                col
            })
            .collect();
        statements.push(format!(
            "CREATE TABLE {} (\n{}\n);",
            quote_ident(&t.name),
            cols.join(",\n")
        ));
    }

    // Constraints
    for c in &snapshot.constraints {
        statements.push(format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {};",
            quote_ident(&c.table_name),
            quote_ident(&c.name),
            c.definition
        ));
    }

    // Indexes
    for idx in &snapshot.indexes {
        statements.push(format!("{};", idx.definition));
    }

    // Views
    for v in &snapshot.views {
        let keyword = if v.is_materialized {
            "MATERIALIZED VIEW"
        } else {
            "VIEW"
        };
        statements.push(format!(
            "CREATE {} {} AS {};",
            keyword,
            quote_ident(&v.name),
            v.definition.trim_end_matches(';').trim()
        ));
    }

    // Functions
    for func in &snapshot.functions {
        statements.push(format!("{};", func.definition.trim_end_matches(';')));
    }

    // Triggers
    for t in &snapshot.triggers {
        statements.push(format!(
            "-- Trigger {} on {}: {}",
            t.name, t.table_name, t.definition
        ));
    }

    statements.join("\n\n")
}
