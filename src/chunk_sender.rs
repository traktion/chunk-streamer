use bytes::Bytes;
use log::info;
use self_encryption::{DataMap, Error};
use tokio::sync::mpsc::{Sender};
use tokio::task::JoinHandle;
use crate::chunk_fetcher::ChunkFetcher;
use crate::chunk_streamer::ChunkGetter;

pub struct ChunkSender<T> {
    sender: Sender<JoinHandle<Result<Bytes, Error>>>,
    id: String,
    chunk_getter: T,
    data_map: DataMap
}

impl<T: ChunkGetter> ChunkSender<T> {
    pub fn new(sender: Sender<JoinHandle<Result<Bytes, Error>>>, id: String, chunk_getter: T, data_map: DataMap) -> ChunkSender<T> {
        ChunkSender { sender, id, chunk_getter, data_map }
    }
    
    pub async fn send(&self, mut range_from: u64, range_to: u64) {
        let mut chunk_count = 1;
        while range_from < range_to {
            info!("Async fetch chunk [{}] at file position [{}] for ID [{}], channel capacity [{}] of [{}]", chunk_count, range_from, self.id, self.sender.capacity(), self.sender.max_capacity());
            let chunk_fetcher = ChunkFetcher::new(self.chunk_getter.clone());
            let data_map_clone = self.data_map.clone();

            let join_handle = tokio::spawn(async move {
                chunk_fetcher.fetch_from_data_map_chunk(data_map_clone, range_from, range_to).await
            });
            let result = self.sender.send(join_handle).await;
            if result.is_err() {
                info!("Send aborted: {}", result.unwrap_err().to_string());
                break;
            };

            range_from += if chunk_count == 1 {
                self.get_first_chunk_limit(range_from) as u64
            } else {
                self.data_map.infos().get(0).unwrap().src_size as u64
            };
            chunk_count += 1;
        };
    }

    fn get_first_chunk_limit(&self, range_from: u64) -> usize {
        let stream_chunk_size = self.data_map.infos().get(0).unwrap().src_size;
        let first_chunk_remainder = range_from % stream_chunk_size as u64;
        if first_chunk_remainder > 0 {
            (stream_chunk_size as u64 - first_chunk_remainder) as usize
        } else {
            stream_chunk_size
        }
    }
}