use std::collections::HashMap;

use sea_orm::{entity::prelude::*, FromQueryResult, IntoActiveModel as _, QuerySelect as _, Set};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "meta")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: String,
    pub value: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Debug, Clone)]
pub struct Stat {
    pub num_of_entries: i64,
    pub total_size: i64,
}

pub async fn get_stat(conn: &impl ConnectionTrait) -> Result<Stat, DbErr> {
    let m = Entity::find()
        .filter(Column::Id.is_in(["num_of_entries", "total_size"]))
        .all(conn)
        .await?
        .into_iter()
        .map(|x| (x.id, x.value))
        .collect::<HashMap<_, _>>();

    Ok(Stat {
        num_of_entries: m.get("num_of_entries").copied().unwrap_or(0),
        total_size: m.get("total_size").copied().unwrap_or(0),
    })
}

#[derive(FromQueryResult)]
struct SumOfSizeCacheRow {
    count: i64,
    sum_of_size: Option<i64>,
}

pub async fn reset_stat(conn: &impl ConnectionTrait) -> Result<Stat, DbErr> {
    use super::cache;
    let stat = cache::Entity::find()
        .select_only()
        .column_as(cache::Column::Key.count(), "count")
        .column_as(cache::Column::Size.sum(), "sum_of_size")
        .into_model::<SumOfSizeCacheRow>()
        .all(conn)
        .await?;
    if stat.len() != 1 {
        Err(DbErr::Custom("invalid stat".into()))?;
    }
    let entries = stat[0].count;
    let size = stat[0].sum_of_size.unwrap_or(0);
    update_or_create(conn, "num_of_entries", entries).await?;
    update_or_create(conn, "total_size", size).await?;
    Ok(Stat {
        num_of_entries: entries,
        total_size: size,
    })
}

pub async fn update_or_create(
    conn: &impl ConnectionTrait,
    key: &str,
    new_value: i64,
) -> Result<Model, DbErr> {
    match Entity::find_by_id(key).one(conn).await? {
        Some(mut model) => {
            model.value = new_value;
            model.into_active_model().update(conn).await
        }
        None => {
            ActiveModel {
                id: Set(key.to_string()),
                value: Set(new_value),
            }
            .insert(conn)
            .await
        }
    }
}

pub async fn insert_or_delete_entries(
    conn: &impl ConnectionTrait,
    stat: Stat,
) -> Result<Stat, DbErr> {
    // Entity::update_many()
    //     .col_expr(
    //         Column::Value,
    //         Expr::col(Column::Value).add(Expr::value(entries)),
    //     )
    //     .filter(Column::Id.eq("num_of_entries"))
    //     .exec(conn)
    //     .await?;

    // Entity::update_many()
    //     .col_expr(
    //         Column::Value,
    //         Expr::col(Column::Value).add(Expr::value(bytes)),
    //     )
    //     .filter(Column::Id.eq("total_size"))
    //     .exec(conn)
    //     .await?;

    ActiveModel {
        id: Set("num_of_entries".to_string()),
        value: Set(stat.num_of_entries),
    }
    .update(conn)
    .await?;

    ActiveModel {
        id: Set("total_size".to_string()),
        value: Set(stat.total_size),
    }
    .update(conn)
    .await?;

    Ok(stat)
}
