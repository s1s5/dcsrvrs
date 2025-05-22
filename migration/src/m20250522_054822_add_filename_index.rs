use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_index(
                Index::create()
                    .name("idx-indexes-filename")
                    .table(Cache::Table)
                    .if_not_exists()
                    .col(Cache::Filename)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx-indexes-filename")
                    .table(Cache::Table)
                    .to_owned(),
            )
            .await
    }
}

#[derive(Iden)]
enum Cache {
    Table,
    Filename,
}
