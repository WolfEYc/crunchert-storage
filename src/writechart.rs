use crate::errors::ImportStreamError;
use crate::models::*;
use std::io;

impl ResizableMmapMut {
    fn align_to<T>(&self, len: usize) -> &[T] {
        unsafe {
            let (_, res, _) = self.mmap[..len].align_to();
            res
        }
    }

    fn align_to_mut<T>(&mut self, len: usize) -> &mut [T] {
        unsafe {
            let (_, res, _) = self.mmap[..len].align_to_mut();
            res
        }
    }
}

impl WritableTimePartition {
    fn timestamps(&self) -> &[i64] {
        self.timestamps_mmap.align_to(self.len)
    }
    fn timestamps_mut(&mut self) -> &mut [i64] {
        self.timestamps_mmap.align_to_mut(self.len)
    }
    fn stream_ids_mut(&mut self) -> &mut [u64] {
        self.streams_mmap.align_to_mut(self.len)
    }
    fn values_mut(&mut self) -> &mut [f32] {
        self.values_mmap.align_to_mut(self.len)
    }

    pub async fn import_stream(
        &mut self,
        mut stream: ImportStream,
    ) -> Result<(), ImportStreamError> {
        if stream.pts.is_empty() {
            return Err(ImportStreamError::EmptyInputError);
        }

        stream.pts.sort_unstable_by_key(|x| x.timestamp);

        let start_ts = stream.pts.first().unwrap().timestamp;
        let end_ts = stream.pts.last().unwrap().timestamp;

        let r_offset_count: usize;
        let first_idx: usize;
        let last_idx: usize;

        {
            let wal_timestamps = self.timestamps();
            first_idx = wal_timestamps
                .iter()
                .cloned()
                .rposition(|x| x < start_ts)
                .unwrap_or(0);
            last_idx = wal_timestamps
                .iter()
                .cloned()
                .rposition(|x| x < end_ts)
                .unwrap_or(0);

            r_offset_count = self.len - last_idx - 1;
        }

        let prev_len = self.len;
        self.len += stream.pts.len();

        if r_offset_count > 0 {
            // need to memmove the pts to the right of the insertion block
            let dest = self.len - r_offset_count;
            let src = last_idx..prev_len;
            {
                let timestamps = self.timestamps_mut();
                timestamps.copy_within(src.clone(), dest);
            }
            {
                let stream_ids = self.stream_ids_mut();
                stream_ids.copy_within(src.clone(), dest);
            }
            {
                let values = self.values_mut();
                values.copy_within(src, dest);
            }
        }
        // TODO need to insert overlapping points into the insertion block
        // and then memcpy them into the WAL

        // self.timestamps_wal[first_idx..last_idx].copy_from_slice()

        todo!()
    }
}
