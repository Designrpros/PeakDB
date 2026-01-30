use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeakRecord {
    pub id: String,
    pub collection: String,
    pub data: serde_json::Value,
    pub metadata: Option<serde_json::Value>,
}

#[async_trait]
pub trait DataRouter {
    async fn save(&self, record: PeakRecord) -> Result<()>;
    async fn find(&self, collection: &str, query: serde_json::Value) -> Result<Vec<PeakRecord>>;
    async fn delete(&self, id: &str) -> Result<()>;
}

pub struct PeakDBRouter {
    // We will add providers (Postgres, Local, Memory) here
    _name: String,
}

impl PeakDBRouter {
    pub fn new(name: impl Into<String>) -> Self {
        Self { _name: name.into() }
    }
}

#[async_trait]
impl DataRouter for PeakDBRouter {
    async fn save(&self, record: PeakRecord) -> Result<()> {
        log::info!("Routing save for {} to providers...", record.id);
        // TODO: Parallel save to local disk and remote cloud/postgres
        Ok(())
    }

    async fn find(&self, collection: &str, query: serde_json::Value) -> Result<Vec<PeakRecord>> {
        log::info!(
            "Routing query for collection: {} with {:?}",
            collection,
            query
        );
        Ok(vec![])
    }

    async fn delete(&self, id: &str) -> Result<()> {
        log::info!("Routing delete for {}", id);
        Ok(())
    }
}
