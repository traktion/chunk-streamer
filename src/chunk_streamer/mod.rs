use autonomi::Client;
use chunk_receiver::ChunkReceiver;
use chunk_sender::ChunkSender;
use self_encryption::DataMap;
use tokio::sync::mpsc::channel;

pub mod chunk_receiver;
pub mod chunk_sender;
pub mod chunk_fetcher;

pub struct ChunkStreamer {
    id: String,
    data_map: DataMap,
    autonomi_client: Client,
    download_threads: usize,
}

impl ChunkStreamer {
    pub fn new(id: String, data_map: DataMap, autonomi_client: Client, download_threads: usize) -> ChunkStreamer {
        ChunkStreamer { id, data_map, autonomi_client, download_threads }
    }
    
    pub fn open(&self, range_from: u64, range_to: u64) -> ChunkReceiver {
        let (sender, receiver) = channel(self.download_threads);
        let chunk_sender = ChunkSender::new(sender, self.id.clone(), self.data_map.clone(), self.autonomi_client.clone());
        tokio::spawn(async move { chunk_sender.send(range_from, range_to).await; });
        ChunkReceiver::new(receiver, self.id.clone())
    }
}