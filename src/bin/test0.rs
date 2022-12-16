use std::time::SystemTime;

use chrono::{DateTime, Utc};
use dcsrvrs::establish_connection;
use entity::cache;
use migration::DbErr;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};

#[tokio::main]
async fn main() -> Result<(), DbErr> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .init();

    let db = establish_connection().await?;
    let now: DateTime<Utc> = Utc::now();

    // Find a cake model first
    let c: Option<cache::Model> = cache::Entity::find()
        .filter(cache::Column::Key.eq("test"))
        .one(&db)
        .await?;
    println!("c => {:?}", c);
    // let c: cache::Model = c.unwrap();

    let c = cache::ActiveModel {
        key: Set(String::from("test")),
        store_time: Set(now.to_rfc3339().into()),
        expire_time: Set(None),
        access_time: Set(now.to_rfc3339().into()),
        size: Set(10),
        filename: Set(None),
        value: Set(None),
        ..Default::default()
    };

    let c: cache::Model = c.insert(&db).await?;

    println!(
        "Cache created with ID: {}, key: {}, size: {}",
        c.key, c.key, c.size
    );

    let mut u: cache::ActiveModel = c.into();
    u.size = Set(100);
    let c = u.update(&db).await?;
    println!(
        "Cache created with ID: {}, key: {}, size: {}",
        c.key, c.key, c.size
    );

    Ok(())
}
