use crate::chunk_streamer::ChunkGetter;
use autonomi::{ChunkAddress, XorName};
use bytes::Bytes;

pub fn blocking_chunk_getter<T: ChunkGetter>(chunk_getter: T) -> impl Fn(&[(usize, XorName)]) -> self_encryption::Result<Vec<(usize, Bytes)>> {
    move |hashes| {
        // Create chunk retrieval function
        let mut results = Vec::new();
        for &(index, hash) in hashes {
            let chunk_address = ChunkAddress::new(hash);

            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(async { chunk_getter.chunk_get(&chunk_address).await })
            });

            match result {
                Ok(chunk) => {
                    results.push((index, Bytes::from(chunk.value.clone())));
                },
                Err(error) => {
                    return Err(self_encryption::Error::Generic(
                        error.to_string()
                    ));
                }
            }
        }
        Ok(results)
    }
}