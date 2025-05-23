use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "cache")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub key: String,
    pub store_time: i64,
    pub expire_time: Option<i64>,
    pub access_time: i64,
    pub size: i64,
    pub sha256sum: Vec<u8>,
    pub filename: Option<String>,
    pub value: Option<Vec<u8>>,
    pub attr: Option<Vec<u8>>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
