pub use sea_orm_migration::prelude::*;

mod m20220101_000001_create_table;
mod m20240915_080718_add_index;
mod m20250117_025901_add_size_index;
mod m20250508_043601_add_meta;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20220101_000001_create_table::Migration),
            Box::new(m20240915_080718_add_index::Migration),
            Box::new(m20250117_025901_add_size_index::Migration),
            Box::new(m20250508_043601_add_meta::Migration),
        ]
    }
}
