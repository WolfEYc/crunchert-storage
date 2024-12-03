use crate::constants::*;
use crate::models::*;
use chrono::Utc;
use memmap2::Mmap;
use pco::standalone::simple_decompress;
use std::cmp::{max, min};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;

impl ChartRequest {
    fn resolution(&self) -> usize {
        let duration_s = (self.stop_unix_s - self.start_unix_s) as u32;
        let resolution = duration_s / self.step_s;
        return resolution as usize;
    }
}

impl ChartReqMetadata {
    #[inline]
    fn iter_search_ts(&self) -> impl Iterator<Item = i64> {
        (self.start_unix_s..self.stop_unix_s)
            .rev()
            .step_by(self.step_s as usize)
    }
}

impl ValueTracker {
    #[inline]
    fn agg<F>(self, rhs: Self, agg_fn: &F) -> Self
    where
        F: Fn(f32, f32) -> f32,
    {
        Self {
            value: agg_fn(self.value, rhs.value),
            count: self.count + rhs.count,
        }
    }
    #[inline]
    fn apply<F>(self, rhs: f32, agg_fn: &F) -> Self
    where
        F: Fn(f32, f32) -> f32,
    {
        Self {
            value: agg_fn(self.value, rhs),
            count: self.count + 1,
        }
    }

    #[inline]
    fn default_final_agg_fn(self) -> f32 {
        self.value
    }
    #[inline]
    fn avg_final_agg(self) -> f32 {
        self.value / self.count as f32
    }
}

impl Aggregation {
    fn to_agg_fn(&self) -> impl Fn(f32, f32) -> f32 {
        match self {
            Aggregation::Sum => std::ops::Add::add,
            Aggregation::Avg => std::ops::Add::add,
            Aggregation::Min => f32::min,
            Aggregation::Max => f32::max,
        }
    }
    fn to_final_agg(self) -> impl Fn(ValueTracker) -> f32 {
        match self {
            Aggregation::Avg => ValueTracker::avg_final_agg,
            _ => ValueTracker::default_final_agg_fn,
        }
    }
}

impl DatapointVec {
    fn get_chart_values(&self, req: ChartReqMetadata) -> impl Iterator<Item = Option<f32>> + '_ {
        let mut prev_index = 0;
        let max_idx = self
            .unix_s
            .binary_search(&req.stop_unix_s)
            .map_or_else(|e| e, |x| x + 1);
        req.iter_search_ts().map(move |x| {
            let found_ts_idx = self.unix_s[prev_index..max_idx]
                .binary_search(&x)
                .unwrap_or_else(|x| x);
            let found_ts_idx = min(found_ts_idx, self.unix_s.len() - 1);
            let found_ts = self.unix_s[found_ts_idx];
            let diff = x.abs_diff(found_ts);
            prev_index = found_ts_idx;
            if diff < req.step_s as u64 {
                let value = self.value[found_ts_idx];
                Some(value)
            } else {
                None
            }
        })
    }
    fn add_stream_to_chart(
        &self,
        req: ChartReqMetadata,
        aggregated_result: Vec<ValueTracker>,
        agg: Aggregation,
    ) -> Vec<ValueTracker> {
        let chart_values = self.get_chart_values(req);
        let agg_fn = agg.to_agg_fn();
        aggregated_result
            .into_iter()
            .zip(chart_values)
            .filter_map(|(x, y)| match y {
                Some(y) => Some((x, y)),
                None => None,
            })
            .map(|(x, y)| x.apply(y, &agg_fn))
            .collect()
    }
}
impl From<&ChartRequest> for ChartReqMetadata {
    fn from(value: &ChartRequest) -> Self {
        Self {
            start_unix_s: value.start_unix_s,
            stop_unix_s: value.stop_unix_s,
            step_s: value.step_s,
            resolution: value.resolution(),
        }
    }
}

impl ReadOnlyDiskStreamFileHeader {
    fn read_stream_from_compressed(&self, mmap: &Mmap) -> DatapointVec {
        let unix_s_bytes = &mmap[self.unix_s_byte_start..self.unix_s_byte_stop];
        let value_bytes = &mmap[self.unix_s_byte_stop..self.values_byte_stop];

        let Ok(unix_s_decompressed) = simple_decompress(unix_s_bytes) else {
            return DatapointVec::default();
        };
        let Ok(values_decompressed) = simple_decompress(value_bytes) else {
            return DatapointVec::default();
        };

        DatapointVec {
            unix_s: unix_s_decompressed,
            value: values_decompressed,
        }
    }
}
impl ReadOnlyStream {
    async fn get_chart_aggregated(
        &self,
        req: ChartReqMetadata,
        mmap: &Mmap,
        aggregated_result: Vec<ValueTracker>,
        agg: Aggregation,
    ) -> Vec<ValueTracker> {
        {
            let mut last_accessed_lock = self.last_accessed.lock().await;
            *last_accessed_lock = Some(Utc::now().timestamp());
        }
        {
            let hot_stream_option = self.hot_stream.read().await;
            if let Some(ref x) = *hot_stream_option {
                return x.add_stream_to_chart(req, aggregated_result, agg);
            }
        }

        let mut writable_hot_stream = self.hot_stream.write().await;

        if let Some(ref x) = *writable_hot_stream {
            return x.add_stream_to_chart(req, aggregated_result, agg);
        }

        let hot_stream = self.disk_header.read_stream_from_compressed(mmap);

        let res = hot_stream.add_stream_to_chart(req, aggregated_result, agg);
        *writable_hot_stream = Some(hot_stream);
        res
    }
}

impl ReadOnlyTimePartition {
    async fn read_only_get_chart_aggregated_batched(
        self: Arc<Self>,
        req: Arc<ChartRequest>,
        agg: Aggregation,
        meta: ChartReqMetadata,
        thread_idx: usize,
        num_threads: usize,
    ) -> Vec<ValueTracker> {
        let streams_per_thread = req.stream_ids.len() / num_threads;
        let start_idx = streams_per_thread * thread_idx;
        let stop_idx = if thread_idx == num_threads - 1 {
            req.stream_ids.len()
        } else {
            streams_per_thread * (thread_idx + 1)
        };
        // pretty confident with this math, if it goes wrong, then well shit

        let streams = req.stream_ids[start_idx..stop_idx]
            .iter()
            .filter_map(|x| self.streams.get(x));

        let mut aggregated_batch = vec![ValueTracker::default(); meta.resolution];
        for stream in streams {
            aggregated_batch = stream
                .get_chart_aggregated(meta, &self.mmap, aggregated_batch, agg)
                .await;
        }
        return aggregated_batch;
    }

    pub async fn read_only_time_partition_get_agg_chart(
        self: Arc<Self>,
        req: Arc<ChartRequest>,
        agg: Aggregation,
        num_threads: usize,
    ) -> Vec<Datapoint> {
        let meta: ChartReqMetadata = req.as_ref().into();

        let threads_requested = req.stream_ids.len() / MIN_STREAMS_PER_THREAD;
        let threads_capped = min(threads_requested, num_threads);
        let num_threads = max(threads_capped, 1);

        let batches = (0..num_threads).map(|x| {
            self.clone().read_only_get_chart_aggregated_batched(
                req.clone(),
                agg,
                meta,
                x,
                num_threads,
            )
        });
        let agg_fn = agg.to_agg_fn();
        let batched_agg_chart = JoinSet::from_iter(batches).join_all().await;
        let reduced = batched_agg_chart.into_iter().reduce(|acc, x| {
            acc.into_iter()
                .zip(x)
                .map(|(x, y)| x.agg(y, &agg_fn))
                .collect()
        });

        let Some(reduced) = reduced else {
            panic!("tried to aggregate nothing");
        };

        let final_agg = agg.to_final_agg();

        reduced
            .into_iter()
            .zip(meta.iter_search_ts())
            .filter(|(x, _)| x.count > 0)
            .map(|(x, ts)| Datapoint {
                unix_s: ts,
                value: final_agg(x),
            })
            .collect()
    }
}
impl<T> ResizableMmapMut<T> {
    fn align_to(&self, len: usize) -> &[T] {
        unsafe {
            let (_, res, _) = self.mmap[..len].align_to();
            res
        }
    }
}
impl WritableTimePartition {
    #[inline]
    pub fn cap(&self) -> usize {
        self.timestamps_mmap.cap
    }
    #[inline]
    pub fn pts_free(&self) -> usize {
        self.cap() - self.len
    }
    #[inline]
    pub fn pct_full(&self) -> f32 {
        self.len as f32 / self.timestamps_mmap.cap as f32
    }
    pub fn stream(&self) -> StreamPointSlice {
        StreamPointSlice {
            timestamp: self.timestamps_mmap.align_to(self.len),
            stream_id: self.streams_mmap.align_to(self.len),
            value: self.values_mmap.align_to(self.len),
        }
    }
    pub fn get_stream_from_wal(&self, stream_id: u64) -> DatapointVec {
        self.stream()
            .iter()
            .filter(|x| *x.stream_id == stream_id)
            .map(|x| Datapoint {
                unix_s: *x.timestamp,
                value: *x.value,
            })
            .collect()
    }
}
pub async fn writable_get_chart_aggregated_batched(
    writable_partition: Arc<RwLock<WritableTimePartition>>,
    req: Arc<ChartRequest>,
    agg: Aggregation,
    meta: ChartReqMetadata,
    thread_idx: usize,
    num_threads: usize,
) -> Vec<ValueTracker> {
    let streams_per_thread = req.stream_ids.len() / num_threads;
    let start_idx = streams_per_thread * thread_idx;
    let stop_idx = if thread_idx == num_threads - 1 {
        req.stream_ids.len()
    } else {
        streams_per_thread * (thread_idx + 1)
    };
    // pretty confident with this math, if it goes wrong, then well shit

    let partition_read_lock = writable_partition.read().await;

    let mut aggregated_batch = vec![ValueTracker::default(); meta.resolution];
    for stream_id in &req.stream_ids[start_idx..stop_idx] {
        let Some(from_cache) = partition_read_lock.streams.get(stream_id) else {
            continue;
        };

        if let Some(ref hs) = *from_cache.hs.read().await {
            aggregated_batch = hs.add_stream_to_chart(meta, aggregated_batch, agg);
            continue;
        }
        let hs = partition_read_lock.get_stream_from_wal(*stream_id);
        aggregated_batch = hs.add_stream_to_chart(meta, aggregated_batch, agg);
        let mut writetable_hs = from_cache.hs.write().await;
        *writetable_hs = Some(hs)
    }
    return aggregated_batch;
}

pub async fn writable_time_partition_get_agg_chart(
    time_partition: Arc<RwLock<WritableTimePartition>>,
    req: Arc<ChartRequest>,
    agg: Aggregation,
    num_threads: usize,
) -> Vec<Datapoint> {
    let meta: ChartReqMetadata = req.as_ref().into();

    let threads_requested = req.stream_ids.len() / MIN_STREAMS_PER_THREAD;
    let threads_capped = min(threads_requested, num_threads);
    let num_threads = max(threads_capped, 1);

    let batches = (0..num_threads).map(|x| {
        writable_get_chart_aggregated_batched(
            time_partition.clone(),
            req.clone(),
            agg,
            meta,
            x,
            num_threads,
        )
    });
    let agg_fn = agg.to_agg_fn();
    let batched_agg_chart = JoinSet::from_iter(batches).join_all().await;
    let reduced = batched_agg_chart.into_iter().reduce(|acc, x| {
        acc.into_iter()
            .zip(x)
            .map(|(x, y)| x.agg(y, &agg_fn))
            .collect()
    });

    let Some(reduced) = reduced else {
        panic!("tried to aggregate nothing");
    };

    let final_agg = agg.to_final_agg();

    reduced
        .into_iter()
        .zip(meta.iter_search_ts())
        .filter(|(x, _)| x.count > 0)
        .map(|(x, ts)| Datapoint {
            unix_s: ts,
            value: final_agg(x),
        })
        .collect()
}
