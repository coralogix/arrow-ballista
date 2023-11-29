use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use object_store::{path::Path, ObjectStore};
use tokio::io::AsyncWriteExt;
use tokio::{fs::File, sync::mpsc};
use tracing::warn;

use crate::client::AsyncStreamReader;
use crate::error::BallistaError;
use tokio_util::compat::TokioAsyncReadCompatExt;

pub enum Command {
    Replicate { path: String },
}

pub async fn start_replication(
    base_path: String,
    object_store: Arc<dyn ObjectStore>,
    mut receiver: mpsc::Receiver<Command>,
) -> Result<(), BallistaError> {
    while let Some(Command::Replicate { path }) = receiver.recv().await {
        match Path::parse(base_path.as_str()) {
            Ok(location) => match File::open(path.as_str()).await {
                Ok(file) => match AsyncStreamReader::try_new(file.compat(), None).await {
                    Ok(mut reader) => match object_store.put_multipart(&location).await {
                        Ok((_, mut multipart_writer)) => {
                            while let Some(batch) = reader.maybe_next().await.transpose()
                            {
                                match batch {
                                    Ok(batch) => {
                                        let mut serialized_data = Vec::with_capacity(
                                            batch.get_array_memory_size(),
                                        );
                                        // let options = match IpcWriteOptions::default()
                                        //     .try_with_compression(Some(
                                        //         CompressionType::LZ4_FRAME,
                                        //     )) {
                                        //     Err(error) => {
                                        //         warn!(
                                        //             ?error,
                                        //             "Failed to enable compression"
                                        //         );
                                        //         IpcWriteOptions::default()
                                        //     }
                                        //     Ok(opt) => opt,
                                        // };

                                        let options = IpcWriteOptions::default();

                                        {
                                            match StreamWriter::try_new_with_options(
                                                &mut serialized_data,
                                                batch.schema().as_ref(),
                                                options,
                                            ) {
                                                Ok(mut writer) => {
                                                    if let Err(error) =
                                                        writer.write(&batch)
                                                    {
                                                        warn!(?error, ?path, "Failed to write batch to a stream writer");
                                                        break;
                                                    }

                                                    if let Err(error) = writer.finish() {
                                                        warn!(
                                                            ?error,
                                                            ?path,
                                                            "Failed to finish writing to a stream writer");
                                                        break;
                                                    }
                                                }
                                                Err(error) => {
                                                    warn!(
                                                        ?error,
                                                        ?path,
                                                        "Failed to build a stream writer"
                                                    );
                                                    break;
                                                }
                                            }
                                        }

                                        let bytes = Bytes::from(serialized_data);
                                        if let Err(error) =
                                            multipart_writer.write_all(&bytes).await
                                        {
                                            warn!(
                                                ?error,
                                                ?path,
                                                "Failed to write batch to object store"
                                            );
                                            break;
                                        }
                                    }
                                    Err(error) => {
                                        warn!(
                                            ?error,
                                            ?path,
                                            "Failed to write retrieve batch from stream"
                                        );
                                        break;
                                    }
                                }
                            }

                            if let Err(error) = multipart_writer.flush().await {
                                warn!(?error, ?path, "Failed to flush writer")
                            }
                            if let Err(error) = multipart_writer.shutdown().await {
                                warn!(?error, ?path, "Failed to shutdown writer")
                            }
                        }
                        Err(error) => {
                            warn!(
                                ?error,
                                ?path,
                                "Failed to create obejct store multipart writer"
                            );
                        }
                    },
                    Err(error) => {
                        warn!(?error, ?path, "Failed to create async reader");
                    }
                },
                Err(error) => {
                    warn!(?error, ?path, "Failed to open file for replication");
                }
            },
            Err(error) => {
                warn!(?error, ?path, "Failed to parse replication path");
            }
        }
    }

    Ok(())
}
