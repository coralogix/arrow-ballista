use ballista_core::client::BallistaClient;
use ballista_core::serde::scheduler::PartitionLocation;
use log::error;
use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;

pub struct ShuffleReaper {
    receiver: Receiver<Vec<PartitionLocation>>,
}

impl ShuffleReaper {
    pub fn new(receiver: Receiver<Vec<PartitionLocation>>) -> Self {
        Self { receiver }
    }

    pub fn start(self) {
        tokio::task::spawn(async move {
            let mut clients: HashMap<String, BallistaClient> = Default::default();
            let mut receiver = self.receiver;
            while let Some(locations) = receiver.recv().await {
                for location in locations {
                    if let Some(client) = clients.get_mut(&location.executor_meta.id) {
                        client
                            .delete_partition(&location.path)
                            .await
                            .unwrap_or_else(|e| {
                                error!(
                                "Error deleting partition {:?} from executor {:?}: {:?}",
                                location.path, location.executor_meta.id, e
                            )
                            });
                    } else {
                        let executor_id = location.executor_meta.id.clone();
                        if let Ok(mut client) = BallistaClient::try_new(
                            &location.executor_meta.host,
                            location.executor_meta.grpc_port,
                        )
                        .await
                        {
                            clients.insert(executor_id, client.clone());
                            client.delete_partition(&location.path).await.unwrap_or_else(|e| {
                                error!("Error deleting partition {:?} from executor {:?}: {:?}", location.path, location.executor_meta.id,e)
                            });
                        } else {
                            error!("Failed to connect to executor {}", executor_id);
                        }
                    }
                }
            }
        });
    }
}
