use std::cmp::min;
use crate::chunk_getter::{blocking_chunk_getter};
use crate::chunk_streamer::ChunkGetter;
use bytes::Bytes;
use log::{debug, error, info, warn};
use self_encryption::{streaming_decrypt, DataMap, Error};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

pub struct ChunkSender<T> {
    sender: Sender<JoinHandle<Result<Bytes, Error>>>,
    id: String,
    chunk_getter: T,
    data_map: DataMap
}

const CHUNK_SIZE: u64 = 4194304;

impl<T: ChunkGetter> ChunkSender<T> {
    pub fn new(sender: Sender<JoinHandle<Result<Bytes, Error>>>, id: String, chunk_getter: T, data_map: DataMap) -> ChunkSender<T> {
        ChunkSender { sender, id, chunk_getter, data_map }
    }

    pub async fn send(&self, mut range_from: u64, range_to: u64) {
        let mut chunk_count = 1;
        let range_to_inclusive = range_to + 1;
        let len = min(CHUNK_SIZE, range_to_inclusive - range_from);

        while range_from < range_to_inclusive {
            info!("Async fetch chunk [{}] at range_from [{}] to range_to [{}] using len [{}] for ID [{}], channel capacity [{}] of [{}]", chunk_count, range_from, range_to_inclusive, len, self.id, self.sender.capacity(), self.sender.max_capacity());

            let local_data_map = self.data_map.clone();
            let local_chunk_getter = self.chunk_getter.clone();

            let join_handle = tokio::task::spawn_blocking(move || {
                let get_chunk_functor = blocking_chunk_getter(local_chunk_getter);
                let stream = streaming_decrypt(&local_data_map, &get_chunk_functor)
                    .expect("failed to execute streaming_decrypt");

                let usize_range_from = usize::try_from(range_from).expect("failed range_from conversion");
                let usize_len = usize::try_from(len).expect("failed len conversion");
                let bytes = stream.get_range(
                    usize_range_from,
                    usize_len
                ).expect("failed to get bytes in range");
                debug!("get_range({}, {}) returned [{}] bytes of total [{}]", usize_range_from, usize_len, bytes.len(), stream.file_size());
                Ok(bytes)
            });
            let result = self.sender.send(join_handle).await;
            if result.is_err() {
                warn!("Send aborted: {}", result.unwrap_err().to_string());
                break;
            };

            range_from += len;
            chunk_count += 1;
        };
    }
}