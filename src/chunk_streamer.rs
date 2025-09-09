use async_trait::async_trait;
use autonomi_old::{Chunk, ChunkAddress};
use autonomi_old::client::GetError;
use self_encryption_old::DataMap;
use tokio::sync::mpsc::channel;
use crate::chunk_receiver::ChunkReceiver;
use crate::chunk_sender::ChunkSender;

#[async_trait]
pub trait ChunkGetter: Clone + Send + Sync + 'static {
    async fn chunk_get(&self, address: &ChunkAddress) -> Result<Chunk, GetError>;
}

pub struct ChunkStreamer<T> {
    id: String,
    data_map: DataMap,
    chunk_getter: T,
    download_threads: usize,
}

impl<T: ChunkGetter> ChunkStreamer<T> {
    pub fn new(id: String, data_map: DataMap, chunk_getter: T, download_threads: usize) -> ChunkStreamer<T> {
        ChunkStreamer { id, data_map, chunk_getter, download_threads }
    }
    
    pub fn open(&self, range_from: u64, range_to: u64) -> ChunkReceiver {
        let (sender, receiver) = channel(self.download_threads);
        let chunk_sender = ChunkSender::new(sender, self.id.clone(), self.chunk_getter.clone(), self.data_map.clone());
        tokio::spawn( Box::pin(async move { chunk_sender.send(range_from, range_to).await; }));
        ChunkReceiver::new(receiver, self.id.clone())
    }
}