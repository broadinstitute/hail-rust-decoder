//! SQLite-backed metrics storage for persistent dashboard data.

use crate::distributed::message::TelemetrySnapshot;
use rusqlite::{params, Connection, Result as SqliteResult};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Database handle for metrics storage.
pub struct MetricsDb {
    conn: Arc<Mutex<Connection>>,
}

impl MetricsDb {
    /// Open or create a metrics database at the given path.
    /// Use ":memory:" for in-memory database or a file path for persistence.
    pub fn open<P: AsRef<Path>>(path: P) -> SqliteResult<Self> {
        let conn = Connection::open(path)?;

        // Create tables if they don't exist
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                cpu_percent REAL,
                memory_used_bytes INTEGER,
                memory_total_bytes INTEGER,
                rows_per_sec REAL NOT NULL,
                total_rows INTEGER NOT NULL,
                active_partition INTEGER,
                partitions_completed INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_telemetry_worker_time
                ON telemetry(worker_id, timestamp_ms);

            CREATE INDEX IF NOT EXISTS idx_telemetry_time
                ON telemetry(timestamp_ms);
            "#,
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open an in-memory database (useful for testing or when persistence isn't needed).
    pub fn in_memory() -> SqliteResult<Self> {
        Self::open(":memory:")
    }

    /// Insert a telemetry snapshot for a worker.
    pub fn insert_snapshot(&self, worker_id: &str, snapshot: &TelemetrySnapshot) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO telemetry (
                worker_id, timestamp_ms, cpu_percent, memory_used_bytes,
                memory_total_bytes, rows_per_sec, total_rows,
                active_partition, partitions_completed
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            params![
                worker_id,
                snapshot.timestamp_ms as i64,
                snapshot.cpu_percent,
                snapshot.memory_used_bytes.map(|v| v as i64),
                snapshot.memory_total_bytes.map(|v| v as i64),
                snapshot.rows_per_sec,
                snapshot.total_rows as i64,
                snapshot.active_partition.map(|v| v as i64),
                snapshot.partitions_completed as i64,
            ],
        )?;
        Ok(())
    }

    /// Get all snapshots for a worker, ordered by timestamp.
    pub fn get_worker_snapshots(&self, worker_id: &str) -> SqliteResult<Vec<TelemetrySnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT timestamp_ms, cpu_percent, memory_used_bytes, memory_total_bytes,
                   rows_per_sec, total_rows, active_partition, partitions_completed
            FROM telemetry
            WHERE worker_id = ?1
            ORDER BY timestamp_ms ASC
            "#,
        )?;

        let snapshots = stmt
            .query_map([worker_id], |row| {
                Ok(TelemetrySnapshot {
                    timestamp_ms: row.get::<_, i64>(0)? as u64,
                    cpu_percent: row.get(1)?,
                    memory_used_bytes: row.get::<_, Option<i64>>(2)?.map(|v| v as u64),
                    memory_total_bytes: row.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                    rows_per_sec: row.get(4)?,
                    total_rows: row.get::<_, i64>(5)? as usize,
                    active_partition: row.get::<_, Option<i64>>(6)?.map(|v| v as usize),
                    partitions_completed: row.get::<_, i64>(7)? as usize,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(snapshots)
    }

    /// Get recent snapshots for a worker (last N entries).
    pub fn get_worker_snapshots_recent(
        &self,
        worker_id: &str,
        limit: usize,
    ) -> SqliteResult<Vec<TelemetrySnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT timestamp_ms, cpu_percent, memory_used_bytes, memory_total_bytes,
                   rows_per_sec, total_rows, active_partition, partitions_completed
            FROM telemetry
            WHERE worker_id = ?1
            ORDER BY timestamp_ms DESC
            LIMIT ?2
            "#,
        )?;

        let mut snapshots: Vec<TelemetrySnapshot> = stmt
            .query_map(params![worker_id, limit as i64], |row| {
                Ok(TelemetrySnapshot {
                    timestamp_ms: row.get::<_, i64>(0)? as u64,
                    cpu_percent: row.get(1)?,
                    memory_used_bytes: row.get::<_, Option<i64>>(2)?.map(|v| v as u64),
                    memory_total_bytes: row.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                    rows_per_sec: row.get(4)?,
                    total_rows: row.get::<_, i64>(5)? as usize,
                    active_partition: row.get::<_, Option<i64>>(6)?.map(|v| v as usize),
                    partitions_completed: row.get::<_, i64>(7)? as usize,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        // Reverse to get chronological order
        snapshots.reverse();
        Ok(snapshots)
    }

    /// Get list of all worker IDs that have telemetry data.
    pub fn get_worker_ids(&self) -> SqliteResult<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT DISTINCT worker_id FROM telemetry ORDER BY worker_id")?;
        let ids = stmt
            .query_map([], |row| row.get(0))?
            .collect::<SqliteResult<Vec<_>>>()?;
        Ok(ids)
    }

    /// Clear all telemetry data (useful when starting a new job).
    pub fn clear(&self) -> SqliteResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM telemetry", [])?;
        Ok(())
    }

    /// Get total count of snapshots.
    pub fn count(&self) -> SqliteResult<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM telemetry", [], |row| row.get(0))?;
        Ok(count as usize)
    }
}

impl Clone for MetricsDb {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_db_basic() {
        let db = MetricsDb::in_memory().unwrap();

        let snapshot = TelemetrySnapshot {
            timestamp_ms: 1000,
            cpu_percent: Some(50.0),
            memory_used_bytes: Some(1024),
            memory_total_bytes: Some(2048),
            rows_per_sec: 1000.0,
            total_rows: 5000,
            active_partition: Some(5),
            partitions_completed: 10,
        };

        db.insert_snapshot("worker-1", &snapshot).unwrap();
        db.insert_snapshot("worker-1", &snapshot).unwrap();
        db.insert_snapshot("worker-2", &snapshot).unwrap();

        assert_eq!(db.count().unwrap(), 3);

        let worker1_snaps = db.get_worker_snapshots("worker-1").unwrap();
        assert_eq!(worker1_snaps.len(), 2);

        let ids = db.get_worker_ids().unwrap();
        assert_eq!(ids, vec!["worker-1", "worker-2"]);

        db.clear().unwrap();
        assert_eq!(db.count().unwrap(), 0);
    }
}
