use crate::models::*;
use itertools::Itertools;
use memmap2::Mmap;
use pco::standalone::simple_decompress;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

impl ChartRequest {
    pub fn resolution(&self) -> usize {
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

#[inline]
fn idx_closematch(
    mut idx: usize,
    timestamps: &[i64],
    requested_ts: i64,
    allowed_offset: u32,
) -> Option<usize> {
    if idx == timestamps.len() {
        idx -= 1
    }
    if requested_ts.abs_diff(timestamps[idx]) > allowed_offset as u64 {
        None
    } else {
        Some(idx)
    }
}

impl ReadOnlyTimePartition {
    pub async fn get_chart_aggregated(
        self: Arc<Self>,
        req: ChartRequest,
        agg: Aggregation,
    ) -> DatapointVec {
        let datapoint_vecs: Vec<DatapointVec> = req
            .stream_ids
            .iter()
            .filter_map(|x| self.streams.get(x))
            .map(|x| x.read_stream_from_compressed(&self.mmap))
            .collect();

        req.iter_search_ts()
            .filter_map(|unix_s| {
                let values = datapoint_vecs.iter().filter_map(|pts| {
                    let idx_res = pts.unix_s.binary_search(&unix_s);
                    match idx_res {
                        Ok(i) => Some(pts.value[i]),
                        Err(i) => match idx_closematch(i, &pts.unix_s, unix_s, req.step_s) {
                            None => None,
                            Some(idx) => Some(pts.value[idx]),
                        },
                    }
                });

                let maybe_agged_value = match agg {
                    Aggregation::Sum => values.sum1(),
                    Aggregation::Avg => {
                        let (sum, counter) =
                            values.fold((0_f32, 0), |(sum, count), x| (sum + x, count + 1));
                        match counter {
                            0 => None,
                            len => Some(sum / len as f32),
                        }
                    }
                    Aggregation::Min => values.reduce(f32::min),
                    Aggregation::Max => values.reduce(f32::max),
                };

                match maybe_agged_value {
                    None => None,
                    Some(value) => Some(Datapoint { unix_s, value }),
                }
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
                if let Some(x) = stream_counter.get_mut(stream_id) {
                    *x = Some(stream.value[i + start_idx])
                }
            }
            let values = stream_counter.values().filter_map(|&x| x);
            let maybe_agged_value = match agg {
                Aggregation::Sum => values.sum1(),
                Aggregation::Avg => {
                    let (sum, counter) =
                        values.fold((0_f32, 0), |(sum, count), x| (sum + x, count + 1));
                    match counter {
                        0 => None,
                        len => Some(sum / len as f32),
                    }
                }
                Aggregation::Min => values.reduce(f32::min),
                Aggregation::Max => values.reduce(f32::max),
            };
            for v in stream_counter.values_mut() {
                *v = None
            }

            match maybe_agged_value {
                None => None,
                Some(value) => Some(Datapoint { unix_s, value }),
            }
        })
        .collect()
}
