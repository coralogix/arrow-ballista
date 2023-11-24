use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use tokio::io::AsyncReadExt;
use tokio::{fs::File, sync::mpsc};
use tracing::warn;

use crate::error::BallistaError;

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
                Ok(mut file) => {
                    let size = file.metadata().await.map_or(1024, |m| m.len());
                    let mut content = Vec::with_capacity(size as usize);

                    match file.read_to_end(&mut content).await {
                        Err(error) => {
                            warn!(
                                ?error,
                                ?path,
                                "Failed to read content of the file for replication"
                            );
                        }
                        _ => match Path::parse(format!("{}/{}", base_path, location)) {
                            Ok(final_path) => {
                                if let Err(error) = object_store
                                    .put(&final_path, Bytes::from(content))
                                    .await
                                {
                                    warn!(
                                        ?error,
                                        ?final_path,
                                        "Failed to replicate file to object store"
                                    );
                                }
                            }
                            Err(error) => {
                                warn!(
                                    ?error,
                                    ?path,
                                    "Failed to parse final replication path"
                                );
                            }
                        },
                    }
                }
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
