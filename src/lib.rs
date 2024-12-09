mod config;
mod constants;
mod errors;
mod models;
mod readchart;
pub mod storage;
mod writechart;

pub use models::{Aggregation, ChartRequest, Datapoint, Storage};

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use models::*;

    use super::*;

    #[tokio::test]
    async fn basic_storage_creation_write_and_read_sync() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            data_storage_dir: tempdir.into_path(),
            ..Default::default()
        };
        let storage = Storage::new(config).await.unwrap();

        let now = Utc::now().timestamp();
        let num_pts = 100;
        let m = 2.3;
        let b = 12.3;
        let stream_id = 69420;
        let stream = (0..num_pts)
            .map(|x| StreamPoint {
                timestamp: now + x * 10,
                stream_id,
                value: m * x as f32 + b,
            })
            .collect();
        storage.import_stream(stream).await.unwrap();

        let req = ChartRequest {
            stream_ids: vec![stream_id],
            start_unix_s: now,
            stop_unix_s: now + num_pts * 10,
            step_s: 10,
        };

        let res = storage.get_agg_chart(req, Aggregation::Sum).await;

        for x in res.into_iter() {
            dbg!(x.to_owned());
        }
    }
}
