use autonomi::{ChunkAddress};
use bytes::{Bytes};
use log::{info};
use self_encryption::{DataMap, EncryptedChunk, Error};
use crate::chunk_streamer::ChunkGetter;

#[derive(Clone)]
pub struct ChunkFetcher {
}

impl ChunkFetcher {
    
    pub fn new() -> Self {
        ChunkFetcher { }
    }

    pub async fn fetch_from_data_map_chunk(
        &self,
        chunk_getter: impl ChunkGetter,
        data_map: DataMap,
        position_start: u64,
        position_end: u64
    ) -> Result<Bytes, Error> {
        info!("fetch from data map chunk");
        
        // note: range queries can be u64, but autonomi only expects usize, so safely convert
        let position_start_usize = usize::try_from(position_start).ok().unwrap_or_else(|| usize::MAX);
        let position_end_usize = usize::try_from(position_end).ok().unwrap_or_else(|| usize::MAX);
        
        let stream_chunk_size = data_map.infos().get(0).unwrap().src_size;
        let chunk_position = position_start_usize / stream_chunk_size; // chunk_info.src_size needed for exact size, as last chunk size varies
        let chunk_start_offset = position_start_usize % stream_chunk_size;
        
        info!("decrypt chunk in position=[{}] of [{}], offset=[{}], total_size=[{}]", chunk_position+1, data_map.infos().len(), chunk_start_offset, data_map.file_size());
        match data_map.infos().get(chunk_position) {
            Some(chunk_info) => {
                info!("get chunk from data map with hash {:?} and size {}", chunk_info.dst_hash, chunk_info.src_size);
                let derived_chunk_size = self.get_chunk_size(position_start_usize, position_end_usize, chunk_info.src_size, chunk_start_offset);
                let chunk = match chunk_getter.chunk_get(&ChunkAddress::new(chunk_info.dst_hash)).await {
                    Ok(chunk) => chunk,
                    Err(e) => return Err(Error::Generic(format!("get chunk failed [{}]", e.to_string()))),
                };

                info!("self decrypt chunk: {:?}", chunk_info.dst_hash);
                let encrypted_chunks = &[EncryptedChunk { index: chunk_position, content: chunk.clone().value }];
                self_encryption::decrypt_range(&data_map, encrypted_chunks, chunk_start_offset, derived_chunk_size)
            }
            None => {
                Ok(Bytes::new())
            }
        }
    }

    fn get_chunk_size(&self, position_start: usize, position_end: usize, stream_chunk_size: usize, chunk_start_offset: usize) -> usize {
        // total bytes requested in this range
        let total_requested = position_end
            .checked_sub(position_start)
            .map(|d| d + 1)
            .unwrap_or(0);
        // bytes available in this chunk after the start offset
        let avail_in_chunk = stream_chunk_size.saturating_sub(chunk_start_offset);
        // use the smaller of requested vs available
        total_requested.min(avail_in_chunk)
    }
}