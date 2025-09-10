use autonomi::ChunkAddress;
use autonomi::client::GetError;
use autonomi::self_encryption::DataMapLevel;
use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use log::{debug, error, info};
use self_encryption::{ChunkInfo, DataMap, EncryptedChunk};
use crate::chunk_streamer::ChunkGetter;

pub struct DataMapBuilder<T> {
    chunk_getter: T,
    download_threads: usize,
}

impl<T: ChunkGetter> DataMapBuilder<T> {
    pub fn new(chunk_getter: T, download_threads: usize) -> DataMapBuilder<T> {
        DataMapBuilder {chunk_getter, download_threads}
    }

    pub async fn get_data_map_from_bytes(&self, data_map_bytes: &Bytes) -> Result<DataMap, GetError> {
        match rmp_serde::from_slice::<DataMap>(&data_map_bytes) {
            Ok(data_map) => {
                debug!("Attempting to deserialize NEW format data map chunk");
                Ok(data_map)
            },
            Err(_) => {
                debug!("Attempting to deserialize OLD format data map chunk");

                let mut data_map_bytes = data_map_bytes.clone();

                loop {
                    // The data_map_bytes could be an Archive, we shall return earlier for that case
                    match Self::get_raw_data_map(&data_map_bytes) {
                        Ok(mut data_map) => {
                            debug!("Restoring from data_map:\n{data_map:?}");
                            if !data_map.is_child() {
                                return Ok(data_map);
                            }
                            data_map.child = None;
                            data_map_bytes = self.fetch_from_data_map(&data_map).await.expect("fetch_from_data_map failed");
                        }
                        Err(e) => {
                            info!("Failed to deserialize data_map_bytes: {e:?}");
                            return Err(GetError::UnrecognizedDataMap("Failed to deserialize data_map_bytes".to_string()));
                        }
                    }
                }
            }
        }
    }

    fn get_raw_data_map(data_map_bytes: &Bytes) -> Result<DataMap, GetError> {
        // Fall back to old format and convert
        let data_map_level = rmp_serde::from_slice::<DataMapLevel>(data_map_bytes)
            .map_err(GetError::InvalidDataMap).expect("Failed to deserialize OLD data map level");

        let (old_data_map, child) = match &data_map_level {
            DataMapLevel::First(map) => (map, None),
            DataMapLevel::Additional(map) => (map, Some(0)),
        };

        // Convert to new format
        let chunk_identifiers: Vec<ChunkInfo> = old_data_map
            .infos()
            .iter()
            .map(|ck_info| ChunkInfo {
                index: ck_info.index,
                dst_hash: ck_info.dst_hash,
                src_hash: ck_info.src_hash,
                src_size: ck_info.src_size,
            })
            .collect();

        Ok(DataMap {
            chunk_identifiers,
            child,
        })
    }

    async fn fetch_from_data_map(&self, data_map: &DataMap) -> Result<Bytes, GetError> {
        let total_chunks = data_map.infos().len();
        debug!("Fetching {total_chunks} encrypted data chunks from datamap {data_map:?}");

        let mut download_tasks = vec![];
        for (i, info) in data_map.infos().into_iter().enumerate() {
            download_tasks.push(async move {
                let idx = i + 1;
                let chunk_addr = ChunkAddress::new(info.dst_hash);

                info!("Fetching chunk {idx}/{total_chunks}({chunk_addr:?})");

                match self.chunk_getter.chunk_get(&chunk_addr).await {
                    Ok(chunk) => {
                        info!("Successfully fetched chunk {idx}/{total_chunks}({chunk_addr:?})");
                        Ok(EncryptedChunk {
                            content: chunk.value,
                        })
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
                .collect::<Result<Vec<EncryptedChunk>, GetError>>()?;
        debug!("Successfully fetched all {total_chunks} encrypted chunks");

        let data = self_encryption::decrypt(data_map, &encrypted_chunks).map_err(|e| {
            error!("Error decrypting encrypted_chunks: {e:?}");
            GetError::UnrecognizedDataMap("Error decrypting encrypted_chunks".to_string())
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