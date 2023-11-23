use std::sync::Arc;

use bytes::Bytes;
use log::warn;
use object_store::{path::Path, ObjectStore};
use tokio::io::AsyncReadExt;
use tokio::{fs::File, sync::mpsc};

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

                    if file.read_to_end(&mut content).await.is_err() {
                        warn!(
                            "Failed to read content of the file for replication: {}",
                            path
                        );
                    } else {
                        match Path::parse(format!("{}/{}", base_path, location)) {
                            Ok(final_path) => {
                                if let Err(err) = object_store
                                    .put(&final_path, Bytes::from(content))
                                    .await
                                {
                                    warn!(
                                        "Failed to replicate file to object store - {}",
                                        err
                                    );
                                }
                            }
                            Err(error) => {
                                warn!(
                                    ?path,
                                    ?error,
                                    "Failed to parse final replication path"
                                );
                            }
                        }
                    }
                }
                Err(_) => {
                    warn!("Failed to open file for replication: {}", path);
                }
            },
            Err(error) => {
                warn!(?error, ?path, "Failed to parse replication path");
            }
        }
    }

    Ok(())
}
