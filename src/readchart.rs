use crate::models::*;
use memmap2::Mmap;
use pco::standalone::simple_decompress;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

impl ChartRequest {
    fn resolution(&self) -> usize {
        let duration_s = (self.stop_unix_s - self.start_unix_s) as u32;
        let resolution = duration_s / self.step_s;
        return resolution as usize;
    }
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

impl ReadOnlyTimePartition {
    pub async fn get_chart_aggregated(
        self: Arc<Self>,
        req: ChartRequest,
        agg: Aggregation,
    ) -> DatapointVec {
        let datapoint_vecs = req
            .stream_ids
            .iter()
            .filter_map(|x| self.streams.get(x))
            .map(|x| x.read_stream_from_compressed(&self.mmap));

        // aggregate them all!
        todo!()
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
pub async fn get_writable_chart_aggregated(
    time_partition: Arc<RwLock<WritableTimePartition>>,
    req: ChartRequest,
    agg: Aggregation,
) -> DatapointVec {
    let mut stream_counter: HashMap<u64, Option<f32>> =
        req.stream_ids.iter().map(|&x| (x, None)).collect();

    let tp_readlock = time_partition.read().await;

    let stream = tp_readlock.stream();
    let first_idx = stream.timestamp.partition_point(|&x| x < req.start_unix_s);
    let mut last_idx = stream.len();

    req.iter_search_ts()
        .filter_map(|unix_s| {
            let start_idx =
                stream.timestamp[first_idx..last_idx].partition_point(|&p| p < unix_s) + first_idx;
            let offset_ts = unix_s + req.step_s as i64;
            let stop_idx = stream.timestamp[start_idx..last_idx]
                .partition_point(|&p| p < offset_ts)
                + start_idx;
            last_idx = start_idx;

            for (i, stream_id) in stream.stream_id[start_idx..stop_idx].iter().enumerate() {
                match stream_counter.get_mut(stream_id) {
                    None => continue,
                    Some(Some(_)) => continue,
                    Some(none_ref) => *none_ref = Some(stream.value[i + start_idx]),
                }
            }
            let maybe_value = match agg {
                Aggregation::Sum => stream_counter.values().cloned().sum(),
                Aggregation::Avg => {
                    let (sum, counter) = stream_counter
                        .values()
                        .filter_map(|&x| x)
                        .fold((0_f32, 0), |(sum, count), x| (sum + x, count + 1));
                    match counter {
                        0 => None,
                        len => Some(sum / len as f32),
                    }
                }
                Aggregation::Min => stream_counter.values().filter_map(|&x| x).reduce(f32::min),
                Aggregation::Max => stream_counter.values().filter_map(|&x| x).reduce(f32::max),
            };
            for v in stream_counter.values_mut() {
                *v = None
            }

            match maybe_value {
                None => None,
                Some(value) => Some(Datapoint { unix_s, value }),
            }
        })
        .collect()
}
