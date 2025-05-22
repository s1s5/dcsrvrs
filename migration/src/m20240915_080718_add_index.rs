use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_index(
                Index::create()
                    .name("idx-indexes-access_time")
                    .table(Cache::Table)
                    .if_not_exists()
                    .col(Cache::AccessTime)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx-indexes-store_time-key")
                    .table(Cache::Table)
                    .if_not_exists()
                    .col(Cache::StoreTime)
                    .col(Cache::Key)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx-indexes-access_time")
                    .table(Cache::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_index(
                Index::drop()
                    .name("idx-indexes-store_time-key")
                    .table(Cache::Table)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(Iden)]
enum Cache {
    Table,
    Key,
    StoreTime,
    AccessTime,
}
