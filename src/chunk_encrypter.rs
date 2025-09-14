use std::time::Instant;
use autonomi::Chunk;
use autonomi::chunk::DataMapChunk;
use autonomi::client::PutError;
use autonomi::self_encryption::encrypt;
use bytes::Bytes;
use log::debug;

pub struct ChunkEncrypter {
}

impl ChunkEncrypter {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn encrypt(
        &self,
        is_public: bool,
        bytes: Bytes
    ) -> Result<(Vec<Chunk>, DataMapChunk), PutError> {
        let start = Instant::now();
        let (data_map_chunk, mut chunks) = encrypt(bytes)?;

        if is_public {
            chunks.push(data_map_chunk.clone());
        }

        debug!("Encryption took: {:.2?}", start.elapsed());
        Ok((chunks, DataMapChunk(data_map_chunk)))
    }
}