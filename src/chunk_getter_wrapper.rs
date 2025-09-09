use crate::chunk_streamer::ChunkGetter;
use autonomi::{ChunkAddress, XorName};
use bytes::Bytes;

#[derive(Clone)]
pub struct ChunkGetterWrapper<T> {
    chunk_getter: T
}

impl<T: ChunkGetter> ChunkGetterWrapper<T> {
    pub fn new(chunk_getter: T) -> ChunkGetterWrapper<T> {
        ChunkGetterWrapper { chunk_getter }
    }

    pub fn get_chunk_functor(&self) -> impl Fn(&[(usize, XorName)]) -> self_encryption::Result<Vec<(usize, Bytes)>> {
        let local_chunk_getter = self.chunk_getter.clone();
        move |hashes| {
            // Create chunk retrieval function
            let mut results = Vec::new();
            for &(index, hash) in hashes {
                let chunk_address = ChunkAddress::new(hash);

                let result = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(async { local_chunk_getter.chunk_get(&chunk_address).await })
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
}