use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeakRecord {
    pub id: String,
    pub collection: String,
    pub content: String,
    pub vector: Option<Vec<f32>>,
    pub metadata: serde_json::Value,
    pub timestamp: u64,
}

#[async_trait]
pub trait DataProvider: Send + Sync {
    async fn save(&self, record: PeakRecord) -> Result<()>;
    async fn find_semantic(&self, query_vector: &[f32], limit: usize) -> Result<Vec<PeakRecord>>;
    async fn find_keyword(&self, query: &str) -> Result<Vec<PeakRecord>>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[derive(Debug)]
pub struct PeakDB {
    pool: SqlitePool,
}

impl PeakDB {
    pub async fn connect(url: &str) -> Result<Self> {
        let mut options: sqlx::sqlite::SqliteConnectOptions = url.parse()?;
        options = options.create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;

        // Initialize schema for Neural Memory
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS neural_records (
                id TEXT PRIMARY KEY,
                collection TEXT,
                content TEXT,
                vector BLOB,
                metadata TEXT,
                timestamp INTEGER
            )",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    pub async fn save_record(&self, record: PeakRecord) -> Result<()> {
        let vector_blob = record.vector.as_ref().map(|v| {
            let mut bytes = Vec::with_capacity(v.len() * 4);
            for f in v {
                bytes.extend_from_slice(&f.to_le_bytes());
            }
            bytes
        });

        let metadata_str = serde_json::to_string(&record.metadata)?;

        sqlx::query(
            "INSERT OR REPLACE INTO neural_records (id, collection, content, vector, metadata, timestamp)
             VALUES (?, ?, ?, ?, ?, ?)"
        )
        .bind(&record.id)
        .bind(&record.collection)
        .bind(&record.content)
        .bind(vector_blob)
        .bind(metadata_str)
        .bind(record.timestamp as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn find_semantic(&self, vector: &[f32], limit: usize) -> Result<Vec<PeakRecord>> {
        let rows = sqlx::query(
            "SELECT id, collection, content, vector, metadata, timestamp FROM neural_records",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut scored_results = Vec::new();
        for row in rows {
            use sqlx::Row;
            let id: String = row.get(0);
            let collection: String = row.get(1);
            let content: String = row.get(2);
            let vector_blob: Option<Vec<u8>> = row.get(3);
            let metadata_str: String = row.get(4);
            let timestamp: i64 = row.get(5);

            let record_vector = vector_blob.map(|b| {
                b.chunks_exact(4)
                    .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
                    .collect::<Vec<f32>>()
            });

            if let Some(rv) = &record_vector {
                let score = cosine_similarity(vector, rv);
                scored_results.push((
                    score,
                    PeakRecord {
                        id,
                        collection,
                        content,
                        vector: record_vector,
                        metadata: serde_json::from_str(&metadata_str).unwrap_or_default(),
                        timestamp: timestamp as u64,
                    },
                ));
            }
        }

        scored_results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        Ok(scored_results
            .into_iter()
            .take(limit)
            .map(|(_, r)| r)
            .collect())
    }

    pub async fn find_keyword(&self, query: &str) -> Result<Vec<PeakRecord>> {
        let query_lower = format!("%{}%", query.to_lowercase());
        let rows = sqlx::query(
            "SELECT id, collection, content, vector, metadata, timestamp 
             FROM neural_records 
             WHERE LOWER(content) LIKE ?",
        )
        .bind(query_lower)
        .fetch_all(&self.pool)
        .await?;

        let mut results = Vec::new();
        for row in rows {
            use sqlx::Row;
            let id: String = row.get(0);
            let collection: String = row.get(1);
            let content: String = row.get(2);
            let vector_blob: Option<Vec<u8>> = row.get(3);
            let metadata_str: String = row.get(4);
            let timestamp: i64 = row.get(5);

            let vector = vector_blob.map(|b| {
                b.chunks_exact(4)
                    .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
                    .collect()
            });

            results.push(PeakRecord {
                id,
                collection,
                content,
                vector,
                metadata: serde_json::from_str(&metadata_str).unwrap_or_default(),
                timestamp: timestamp as u64,
            });
        }
        Ok(results)
    }
}

fn cosine_similarity(v1: &[f32], v2: &[f32]) -> f32 {
    if v1.len() != v2.len() || v1.is_empty() {
        return 0.0;
    }
    let dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    let n1: f32 = v1.iter().map(|a| a * a).sum::<f32>().sqrt();
    let n2: f32 = v2.iter().map(|a| a * a).sum::<f32>().sqrt();
    if n1 == 0.0 || n2 == 0.0 {
        return 0.0;
    }
    dot / (n1 * n2)
}
