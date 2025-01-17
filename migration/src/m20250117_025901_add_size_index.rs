use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Replace the sample below with your own migration scripts
        manager
            .create_index(
                Index::create()
                    .name("idx-indexes-size")
                    .table(Cache::Table)
                    .if_not_exists()
                    .col(Cache::Size)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx-indexes-size")
                    .table(Cache::Table)
                    .to_owned(),
            )
            .await
    }
}

#[derive(Iden)]
enum Cache {
    Table,
    Size,
}
