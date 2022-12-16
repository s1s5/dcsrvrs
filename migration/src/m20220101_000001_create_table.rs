use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Cache::Table)
                    .if_not_exists()
                    // .col(
                    //     ColumnDef::new(Cache::Id)
                    //         .integer()
                    //         .not_null()
                    //         .auto_increment()
                    //         .primary_key(),
                    // )
                    // .col(ColumnDef::new(Cache::Key).string().not_null())
                    .col(ColumnDef::new(Cache::Key).string().not_null().primary_key())
                    .col(ColumnDef::new(Cache::StoreTime).date_time().not_null())
                    .col(ColumnDef::new(Cache::ExpireTime).date_time())
                    .col(ColumnDef::new(Cache::AccessTime).date_time().not_null())
                    .col(ColumnDef::new(Cache::Size).integer().not_null())
                    .col(ColumnDef::new(Cache::Filename).string())
                    .col(ColumnDef::new(Cache::Value).blob(BlobSize::Medium))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx-indexes-expire_time")
                    .table(Cache::Table)
                    .if_not_exists()
                    .col(Cache::ExpireTime)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Cache::Table).to_owned())
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
enum Cache {
    Table,
    // Id,
    Key,
    StoreTime,
    ExpireTime,
    AccessTime,
    Size,
    Filename,
    Value,
}
