//! Migration dependency graph with topological sort.
//!
//! Supports `-- waypoint:depends V3,V5` directives for non-linear
//! migration ordering using Kahn's algorithm.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::{Result, WaypointError};
use crate::migration::ResolvedMigration;

/// A directed acyclic graph of migration dependencies.
pub struct DependencyGraph {
    /// version -> set of versions it depends on
    edges: HashMap<String, HashSet<String>>,
    /// version -> set of versions that depend on it
    reverse_edges: HashMap<String, HashSet<String>>,
    /// All known versions
    all_versions: Vec<String>,
}

impl DependencyGraph {
    /// Build a dependency graph from resolved migrations.
    ///
    /// If `implicit_chain` is true, each versioned migration implicitly depends
    /// on the previous version in sort order (backward-compatible default).
    pub fn build(migrations: &[&ResolvedMigration], implicit_chain: bool) -> Result<Self> {
        let mut edges: HashMap<String, HashSet<String>> = HashMap::new();
        let mut reverse_edges: HashMap<String, HashSet<String>> = HashMap::new();
        let mut all_versions: Vec<String> = Vec::new();

        // Collect all versioned migrations sorted by version
        let mut versioned: Vec<&ResolvedMigration> = migrations
            .iter()
            .filter(|m| m.is_versioned())
            .copied()
            .collect();
        versioned.sort_by(|a, b| a.version().unwrap().cmp(b.version().unwrap()));

        for m in &versioned {
            let version = m.version().unwrap().raw.clone();
            all_versions.push(version.clone());
            edges.entry(version.clone()).or_default();
            reverse_edges.entry(version.clone()).or_default();
        }

        // Add explicit dependencies from directives
        for m in &versioned {
            let version = m.version().unwrap().raw.clone();
            for dep in &m.directives.depends {
                if !edges.contains_key(dep) {
                    return Err(WaypointError::MissingDependency {
                        version: version.clone(),
                        dependency: dep.clone(),
                    });
                }
                edges.get_mut(&version).unwrap().insert(dep.clone());
                reverse_edges.get_mut(dep).unwrap().insert(version.clone());
            }
        }

        // Add implicit chain dependencies (each version depends on previous)
        if implicit_chain {
            for i in 1..all_versions.len() {
                let current = &all_versions[i];
                let previous = &all_versions[i - 1];
                // Only add implicit dependency if no explicit dependencies are set
                if edges.get(current).is_none_or(|deps| deps.is_empty()) {
                    edges
                        .get_mut(current)
                        .unwrap()
                        .insert(previous.clone());
                    reverse_edges
                        .get_mut(previous)
                        .unwrap()
                        .insert(current.clone());
                }
            }
        }

        Ok(DependencyGraph {
            edges,
            reverse_edges,
            all_versions,
        })
    }

    /// Produce a topologically sorted order of versions using Kahn's algorithm.
    ///
    /// Returns an error if there is a cycle.
    pub fn topological_sort(&self) -> Result<Vec<String>> {
        // Compute in-degree for each node
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        for v in &self.all_versions {
            in_degree.insert(v.clone(), self.edges.get(v).map_or(0, |deps| deps.len()));
        }

        // Start with nodes that have no dependencies
        let mut queue: VecDeque<String> = VecDeque::new();
        for v in &self.all_versions {
            if *in_degree.get(v).unwrap_or(&0) == 0 {
                queue.push_back(v.clone());
            }
        }

        let mut sorted = Vec::new();

        while let Some(node) = queue.pop_front() {
            sorted.push(node.clone());

            // For each node that depends on this one, decrement in-degree
            if let Some(dependents) = self.reverse_edges.get(&node) {
                for dep in dependents {
                    let deg = in_degree.get_mut(dep).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(dep.clone());
                    }
                }
            }
        }

        if sorted.len() != self.all_versions.len() {
            // Find the cycle for error reporting
            let in_cycle: Vec<String> = self
                .all_versions
                .iter()
                .filter(|v| *in_degree.get(*v).unwrap_or(&0) > 0)
                .cloned()
                .collect();
            return Err(WaypointError::DependencyCycle {
                path: in_cycle.join(" -> "),
            });
        }

        Ok(sorted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directive::MigrationDirectives;
    use crate::migration::{MigrationKind, MigrationVersion, ResolvedMigration};

    fn make_migration(version: &str, depends: Vec<&str>) -> ResolvedMigration {
        ResolvedMigration {
            kind: MigrationKind::Versioned(MigrationVersion::parse(version).unwrap()),
            description: format!("V{}", version),
            script: format!("V{}__test.sql", version),
            checksum: 0,
            sql: String::new(),
            directives: MigrationDirectives {
                depends: depends.into_iter().map(String::from).collect(),
                env: vec![],
            },
        }
    }

    #[test]
    fn test_simple_chain() {
        let m1 = make_migration("1", vec![]);
        let m2 = make_migration("2", vec![]);
        let m3 = make_migration("3", vec![]);
        let migrations: Vec<&ResolvedMigration> = vec![&m1, &m2, &m3];

        let graph = DependencyGraph::build(&migrations, true).unwrap();
        let order = graph.topological_sort().unwrap();
        assert_eq!(order, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_explicit_dependency() {
        let m1 = make_migration("1", vec![]);
        let m2 = make_migration("2", vec![]);
        let m3 = make_migration("3", vec!["1"]); // V3 depends on V1, skipping V2
        let migrations: Vec<&ResolvedMigration> = vec![&m1, &m2, &m3];

        let graph = DependencyGraph::build(&migrations, false).unwrap();
        let order = graph.topological_sort().unwrap();
        // V1 must come before V3, V2 has no deps so can be anywhere
        let pos1 = order.iter().position(|v| v == "1").unwrap();
        let pos3 = order.iter().position(|v| v == "3").unwrap();
        assert!(pos1 < pos3);
    }

    #[test]
    fn test_cycle_detection() {
        let m1 = make_migration("1", vec!["2"]);
        let m2 = make_migration("2", vec!["1"]);
        let migrations: Vec<&ResolvedMigration> = vec![&m1, &m2];

        let graph = DependencyGraph::build(&migrations, false).unwrap();
        assert!(graph.topological_sort().is_err());
    }

    #[test]
    fn test_missing_dependency() {
        let m1 = make_migration("1", vec!["99"]);
        let migrations: Vec<&ResolvedMigration> = vec![&m1];

        assert!(DependencyGraph::build(&migrations, false).is_err());
    }
}
