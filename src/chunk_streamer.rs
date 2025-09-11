use crate::chunk_receiver::ChunkReceiver;
use crate::chunk_sender::ChunkSender;
use async_trait::async_trait;
use autonomi::client::GetError;
use autonomi::{Chunk, ChunkAddress};
use bytes::Bytes;
use self_encryption::streaming_decrypt;
use tokio::sync::mpsc::channel;
use crate::chunk_getter::blocking_chunk_getter;
use crate::data_map_builder::DataMapBuilder;

#[async_trait]
pub trait ChunkGetter: Clone + Send + Sync + 'static {
    async fn chunk_get(&self, address: &ChunkAddress) -> Result<Chunk, GetError>;
}

pub struct ChunkStreamer<T> {
    id: String,
    data_map_chunk_bytes: Bytes,
    chunk_getter: T,
    download_threads: usize,
}

impl<T: ChunkGetter> ChunkStreamer<T> {
    pub fn new(id: String, data_map_chunk_bytes: Bytes, chunk_getter: T, download_threads: usize) -> ChunkStreamer<T> {
        ChunkStreamer { id, data_map_chunk_bytes, chunk_getter, download_threads }
    }
    
    pub async fn open(&self, range_from: u64, range_to: u64) -> Result<ChunkReceiver, GetError> {
        let data_map_builder = DataMapBuilder::new(self.chunk_getter.clone(), self.download_threads);
        match data_map_builder.get_data_map_from_bytes(&self.data_map_chunk_bytes).await {
            Ok(data_map) => {
                let (sender, receiver) = channel(self.download_threads);
                let chunk_sender = ChunkSender::new(sender, self.id.clone(), self.chunk_getter.clone(), data_map);
                tokio::spawn(Box::pin(async move { chunk_sender.send(range_from, range_to).await; }));
                Ok(ChunkReceiver::new(receiver, self.id.clone()))
            },
            Err(error) => Err(error)
        }
    }

    pub async fn get_stream_size(&self) -> usize {
        let data_map_builder = DataMapBuilder::new(self.chunk_getter.clone(), self.download_threads);
        let data_map = data_map_builder.get_data_map_from_bytes(&self.data_map_chunk_bytes).await.expect("failed to build data map from chunk");
        let local_chunk_getter = self.chunk_getter.clone();

        let join_handle = tokio::task::spawn_blocking(move || {
            let get_chunk_functor = blocking_chunk_getter(local_chunk_getter);
            let stream = streaming_decrypt(&data_map, get_chunk_functor)
                .expect("failed to execute streaming_decrypt");
            stream.file_size()
        });
        join_handle.await.unwrap_or(0)
    }
}