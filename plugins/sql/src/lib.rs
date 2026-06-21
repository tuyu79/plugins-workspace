// Copyright 2019-2023 Tauri Programme within The Commons Conservancy
// SPDX-License-Identifier: Apache-2.0
// SPDX-License-Identifier: MIT

//! Interface with SQL databases through [sqlx](https://github.com/launchbadge/sqlx). It supports the `sqlite`, `mysql` and `postgres` drivers, enabled by a Cargo feature.

#![doc(
    html_logo_url = "https://github.com/tauri-apps/tauri/raw/dev/app-icon.png",
    html_favicon_url = "https://github.com/tauri-apps/tauri/raw/dev/app-icon.png"
)]

mod commands;
mod decode;
mod error;
mod wrapper;

pub use error::Error;
pub use wrapper::DbPool;
pub use commands::*;

use futures_core::future::BoxFuture;
use serde::{Deserialize, Serialize};
use sqlx::{
    error::BoxDynError,
    migrate::{Migration as SqlxMigration, MigrationSource, MigrationType, Migrator},
};
use tauri::{
    plugin::{Builder as PluginBuilder, TauriPlugin},
    Manager, RunEvent, Runtime,
};
use tokio::sync::{Mutex, RwLock};

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

#[derive(Default)]
pub struct DbInstances(pub RwLock<HashMap<String, DbPool>>);

#[derive(Serialize)]
#[serde(untagged)]
pub enum LastInsertId {
    #[cfg(feature = "sqlite")]
    Sqlite(i64),
    #[cfg(feature = "mysql")]
    MySql(u64),
    #[cfg(feature = "postgres")]
    Postgres(()),
    #[cfg(not(any(feature = "sqlite", feature = "mysql", feature = "postgres")))]
    None,
}

struct Migrations(Mutex<HashMap<String, MigrationList>>);

#[derive(Default, Clone, Deserialize)]
pub struct PluginConfig {
    #[serde(default)]
    preload: Vec<String>,
}

#[derive(Debug)]
pub enum MigrationKind {
    Up,
    Down,
}

impl From<MigrationKind> for MigrationType {
    fn from(kind: MigrationKind) -> Self {
        match kind {
            MigrationKind::Up => Self::ReversibleUp,
            MigrationKind::Down => Self::ReversibleDown,
        }
    }
}

/// A migration definition.
#[derive(Debug)]
pub struct Migration {
    pub version: i64,
    pub description: &'static str,
    pub sql: &'static str,
    pub kind: MigrationKind,
}

#[derive(Debug)]
struct MigrationList(Vec<Migration>);

impl MigrationSource<'static> for MigrationList {
    fn resolve(self) -> BoxFuture<'static, std::result::Result<Vec<SqlxMigration>, BoxDynError>> {
        Box::pin(async move {
            let mut migrations = Vec::new();
            for migration in self.0 {
                if matches!(migration.kind, MigrationKind::Up) {
                    migrations.push(SqlxMigration::new(
                        migration.version,
                        migration.description.into(),
                        migration.kind.into(),
                        migration.sql.into(),
                        false,
                    ));
                }
            }
            Ok(migrations)
        })
    }
}

/// Allows blocking on async code without creating a nested runtime.
fn run_async_command<F: std::future::Future>(cmd: F) -> F::Output {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(cmd))
    } else {
        tauri::async_runtime::block_on(cmd)
    }
}

fn expand_tilde(path: &str) -> String {
    if let Some(stripped) = path.strip_prefix('~') {
        let home = env::var("HOME").expect("HOME 环境变量不存在");
        let stripped = stripped.strip_prefix('/').unwrap_or(stripped);
        PathBuf::from(home).join(stripped).to_string_lossy().to_string()
    } else {
        PathBuf::from(path).to_string_lossy().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_tilde_with_prefix() {
        let home = std::env::var("HOME").unwrap();
        let result = expand_tilde("~/some/path");
        assert_eq!(result, format!("{}/some/path", home));
    }

    #[test]
    fn test_expand_tilde_alone() {
        let home = std::env::var("HOME").unwrap();
        let result = expand_tilde("~");
        assert_eq!(result, format!("{}/", home));
    }

    #[test]
    fn test_expand_tilde_no_tilde() {
        let result = expand_tilde("/absolute/path");
        assert_eq!(result, "/absolute/path");
    }

    #[test]
    fn test_expand_tilde_relative_path() {
        let result = expand_tilde("relative/path");
        assert_eq!(result, "relative/path");
    }

    #[test]
    fn test_expand_tilde_tilde_in_middle() {
        let result = expand_tilde("/path/~user/file");
        assert_eq!(result, "/path/~user/file");
    }
}

/// Tauri SQL plugin builder.
#[derive(Default)]
pub struct Builder {
    migrations: Option<HashMap<String, MigrationList>>,
}

impl Builder {
    pub fn new() -> Self {
        #[cfg(not(any(feature = "sqlite", feature = "mysql", feature = "postgres")))]
        eprintln!("No sql driver enabled. Please set at least one of the \"sqlite\", \"mysql\", \"postgres\" feature flags.");

        Self::default()
    }

    /// Add migrations to a database.
    #[must_use]
    pub fn add_migrations(mut self, db_url: &str, migrations: Vec<Migration>) -> Self {
        self.migrations
            .get_or_insert(Default::default())
            .insert(db_url.to_string(), MigrationList(migrations));
        self
    }

    pub fn build<R: Runtime>(mut self) -> TauriPlugin<R, Option<PluginConfig>> {
        PluginBuilder::<R, Option<PluginConfig>>::new("sql")
            .invoke_handler(tauri::generate_handler![
                commands::load,
                commands::execute,
                commands::select,
                commands::close
            ])
            .setup(|app, api| {
                let config = api.config().clone().unwrap_or_default();

                run_async_command(async move {
                    let instances = DbInstances::default();
                    let mut lock = instances.0.write().await;

                    for db in config.preload {
                        let db = expand_tilde(&db);
                        let pool = DbPool::connect(&db, app).await?;

                        if let Some(migrations) =
                            self.migrations.as_mut().and_then(|mm| mm.remove(&db))
                        {
                            let migrator = Migrator::new(migrations).await?;
                            pool.migrate(&migrator).await?;
                        }

                        lock.insert(db, pool);
                    }
                    drop(lock);

                    app.manage(instances);
                    app.manage(Migrations(Mutex::new(
                        self.migrations.take().unwrap_or_default(),
                    )));

                    Ok(())
                })
            })
            .on_event(|app, event| {
                if let RunEvent::Exit = event {
                    run_async_command(async move {
                        let instances = &*app.state::<DbInstances>();
                        let instances = instances.0.read().await;
                        for value in instances.values() {
                            value.close().await;
                        }
                    });
                }
            })
            .build()
    }
}
