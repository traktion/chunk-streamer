use crate::chunk_streamer::ChunkGetter;
use bytes::Bytes;
use self_encryption::XorName;

pub fn blocking_chunk_getter<T: ChunkGetter>(chunk_getter: T) -> impl Fn(&[(usize, XorName)]) -> self_encryption::Result<Vec<(usize, Bytes)>> {
    move |hashes| {
        // Create chunk retrieval function
        let mut results = Vec::new();
        for &(index, hash) in hashes {

            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(async { chunk_getter.chunk_get(&hash.0).await })
            });

            match result {
                Ok(Some(chunk)) => {
                    results.push((index, chunk.content));
                },
                Ok(None) => {
                    return Err(self_encryption::Error::Generic(
                        "chunk not found".to_string()
                    ));
                }
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