use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // メタ情報テーブル作成
        manager
            .create_table(
                Table::create()
                    .table(Meta::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Meta::Id).string().not_null().primary_key())
                    .col(ColumnDef::new(Meta::Value).big_integer().not_null())
                    .to_owned(),
            )
            .await?;

        // INSERT ... SELECT で集計して初期データ挿入
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                INSERT INTO meta (id, value)
                SELECT 'num_of_entries', COUNT(*) FROM cache
                UNION ALL
                SELECT 'total_size', COALESCE(SUM(size), 0) FROM cache
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Meta::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
enum Meta {
    Table,
    Id,
    Value,
}
