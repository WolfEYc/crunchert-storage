use crate::errors::ImportStreamError;
use crate::models::*;
use memmap2::RemapOptions;
use std::io;

impl ResizableMmapMut {
    fn remap(&mut self, new_len: usize, opts: RemapOptions) -> Result<(), io::Error> {
        if new_len < self.mmap.len() {
            return Ok(());
        }

        let new_capacity = self.mmap.len() * 2;
        self.file.set_len(new_capacity as u64)?;
        unsafe {
            self.mmap.remap(new_capacity, opts)?;
        }
        Ok(())
    }

    fn align_to<T>(&self, len: usize) -> &[T] {
        unsafe {
            let (_, res, _) = self.mmap[..len].align_to();
            res
        }
    }
}

impl WritableTimePartition {
    fn remap(&mut self, new_len: usize) -> Result<(), io::Error> {
        let remap_opts = RemapOptions::new().may_move(true);

        self.timestamps_mmap.remap(new_len, remap_opts)?;
        self.streams_mmap.remap(new_len, remap_opts)?;
        self.values_mmap.remap(new_len, remap_opts)?;

        self.len = new_len;
        Ok(())
    }

    fn timestamps(&self) -> &[i64] {
        self.timestamps_mmap.align_to(self.len)
    }

    pub async fn import_stream(
        &mut self,
        mut stream: ImportStream,
    ) -> Result<(), ImportStreamError> {
        if stream.pts.is_empty() {
            return Err(ImportStreamError::EmptyInputError);
        }

        let new_mmap_size = self.len + stream.pts.len();
        self.remap(new_mmap_size)?;

        stream.pts.sort_unstable_by_key(|x| x.timestamp);

        let start_ts = stream.pts.first().unwrap().timestamp;
        let end_ts = stream.pts.last().unwrap().timestamp;

        let wal_timestamps = self.timestamps();
        let first_idx = wal_timestamps
            .iter()
            .cloned()
            .rposition(|x| x < start_ts)
            .unwrap_or(0);
        let last_idx = wal_timestamps
            .iter()
            .cloned()
            .rposition(|x| x < end_ts)
            .unwrap_or(0);
        // self.timestamps_wal[first_idx..last_idx].copy_from_slice()

        todo!()
    }
}
