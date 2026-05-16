pub mod avantis;
pub mod lighter;

use diesel_derive_enum::DbEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, DbEnum)]
#[ExistingTypePath = "crate::schema::sql_types::Dex"]
pub enum Dex {
    Avantis,
    Lighter,
}

impl std::str::FromStr for Dex {
    type Err = eyre::Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "avantis" => Ok(Dex::Avantis),
            "lighter" => Ok(Dex::Lighter),
            other => Err(eyre::eyre!("unknown DEX: {other:?}")),
        }
    }
}
