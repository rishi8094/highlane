pub mod avantis;
pub mod lighter;

use diesel_derive_enum::DbEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, DbEnum)]
#[ExistingTypePath = "crate::schema::sql_types::Dex"]
pub enum Dex {
    Avantis,
    Lighter,
}
