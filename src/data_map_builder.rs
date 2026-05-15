use ant_core::data::error::{Error, Result};
use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use log::{debug, error, info};
use self_encryption::{DataMap, EncryptedChunk};
use crate::chunk_streamer::ChunkGetter;

pub struct DataMapBuilder<T> {
    chunk_getter: T,
    download_threads: usize,
}

impl<T: ChunkGetter> DataMapBuilder<T> {
    pub fn new(chunk_getter: T, download_threads: usize) -> DataMapBuilder<T> {
        DataMapBuilder {chunk_getter, download_threads}
    }

    pub async fn get_data_map_from_bytes(&self, data_map_bytes: &Bytes) -> Result<DataMap> {
        match rmp_serde::from_slice::<DataMap>(&data_map_bytes) {
            Ok(data_map) => {
                debug!("Attempting to deserialize NEW format data map chunk");
                Ok(data_map)
            },
            Err(e) => {
                info!("Failed to deserialize data_map_bytes: {e:?}");
                Err(Error::InvalidData("Failed to deserialize data_map_bytes".to_string()))
            }
        }
    }

    async fn fetch_from_data_map(&self, data_map: &DataMap) -> Result<Bytes> {
        let total_chunks = data_map.infos().len();
        debug!("Fetching {total_chunks} encrypted data chunks from datamap {data_map:?}");

        let mut download_tasks = vec![];
        for (i, info) in data_map.infos().into_iter().enumerate() {
            download_tasks.push(async move {
                let idx = i + 1;
                let chunk_addr = info.dst_hash;

                info!("Fetching chunk {idx}/{total_chunks}({chunk_addr:?})");

                match self.chunk_getter.chunk_get(&chunk_addr.0).await {
                    Ok(Some(chunk)) => {
                        info!("Successfully fetched chunk {idx}/{total_chunks}({chunk_addr:?})");
                        Ok(EncryptedChunk {
                            content: chunk.content,
                        })
                    }
                    Ok(None) => {
                        error!("Chunk not found: {chunk_addr:?}");
                        Err(Error::Network("Chunk not found".to_string()))
                    }
                    Err(err) => {
                        error!(
                            "Error fetching chunk {idx}/{total_chunks}({chunk_addr:?}): {err:?}"
                        );
                        Err(err)
                    }
                }
            });
        }
        let encrypted_chunks =
            Self::process_tasks_with_max_concurrency(download_tasks, self.download_threads)
                .await
                .into_iter()
                .collect::<Result<Vec<EncryptedChunk>>>()?;
        debug!("Successfully fetched all {total_chunks} encrypted chunks");

        let data = self_encryption::decrypt(data_map, &encrypted_chunks).map_err(|e| {
            error!("Error decrypting encrypted_chunks: {e:?}");
            Error::InvalidData("Error decrypting encrypted_chunks".to_string())
        })?;
        debug!("Successfully decrypted all {total_chunks} chunks");

        //self.cleanup_cached_chunks(&chunk_addrs);

        Ok(data)
    }

    async fn process_tasks_with_max_concurrency<I, R>(tasks: I, batch_size: usize) -> Vec<R>
    where
        I: IntoIterator,
        I::Item: Future<Output = R> + Send,
        R: Send,
    {
        let mut futures = FuturesUnordered::new();
        let mut results = Vec::new();

        for task in tasks.into_iter() {
            futures.push(task);

            if futures.len() >= batch_size
                && let Some(result) = futures.next().await
            {
                results.push(result);
            }
        }

        // Process remaining tasks
        while let Some(result) = futures.next().await {
            results.push(result);
        }

        results
    }
}