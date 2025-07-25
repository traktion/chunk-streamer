use async_trait::async_trait;
use autonomi::{Chunk, ChunkAddress};
use autonomi::client::GetError;
use self_encryption::DataMap;
use tokio::sync::mpsc::channel;
use crate::chunk_receiver::ChunkReceiver;
use crate::chunk_sender::ChunkSender;

#[async_trait]
pub trait ChunkGetter: Clone + Send + Sync + 'static {
    async fn chunk_get(&self, address: &ChunkAddress) -> Result<Chunk, GetError>;
}

pub struct ChunkStreamer {
    id: String,
    data_map: DataMap,
    download_threads: usize,
}

impl ChunkStreamer {
    pub fn new(id: String, data_map: DataMap, download_threads: usize) -> ChunkStreamer {
        ChunkStreamer { id, data_map, download_threads }
    }
    
    pub fn open(&self, chunk_getter: impl ChunkGetter, range_from: u64, range_to: u64) -> ChunkReceiver {
        let (sender, receiver) = channel(self.download_threads);
        let chunk_sender = ChunkSender::new(sender, self.id.clone(), self.data_map.clone());
        tokio::spawn( Box::pin(async move { chunk_sender.send(chunk_getter, range_from, range_to).await; }));
        ChunkReceiver::new(receiver, self.id.clone())
    }
}