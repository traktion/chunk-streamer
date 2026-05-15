use std::time::Instant;
use ant_core::data::error::{Error, Result};
use ant_core::data::{DataMap};
use bytes::Bytes;
use log::debug;
use self_encryption::{encrypt, EncryptedChunk};

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
    ) -> Result<(Vec<EncryptedChunk>, DataMap)> {
        let start = Instant::now();
        let (data_map_chunk, chunks) = match encrypt(bytes) {
            Ok((data_map_chunk, chunks)) => (data_map_chunk, chunks),
            Err(error) => return Err(Error::Encryption(error.to_string())),
        };

        // still needed?
        /*if is_public {
            chunks.push(data_map_chunk.clone());
        }*/

        debug!("Encryption took: {:.2?}", start.elapsed());
        Ok((chunks, data_map_chunk))
    }
}