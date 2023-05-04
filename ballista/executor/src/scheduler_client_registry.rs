use async_trait::async_trait;
use dashmap::DashMap;

use ballista_core::utils::create_grpc_client_connection;

use ballista_core::error::BallistaError;

use tonic::transport::Channel;

use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;

use tokio::sync::Mutex;

use std::sync::Arc;

#[async_trait]
pub trait SchedulerClientRegistry
where
    Self: Send + Sync,
{
    async fn get_scheduler_client(
        &self,
        scheduler_id: &str,
    ) -> Result<Option<Arc<Mutex<SchedulerGrpcClient<Channel>>>>, BallistaError>;

    async fn insert_scheduler_client(
        &self,
        scheduler_id: &str,
        client: Arc<Mutex<SchedulerGrpcClient<Channel>>>,
    );

    async fn get_or_create_scheduler_client(
        &self,
        scheduler_id: &str,
    ) -> Result<Arc<Mutex<SchedulerGrpcClient<Channel>>>, BallistaError> {
        let scheduler = self.get_scheduler_client(scheduler_id).await?;
        // If channel does not exist, create a new one
        if let Some(scheduler) = scheduler {
            Ok(scheduler)
        } else {
            let scheduler_url = format!("http://{scheduler_id}");
            let connection = create_grpc_client_connection(scheduler_url).await?;

            let scheduler = Arc::new(Mutex::new(SchedulerGrpcClient::new(connection)));

            Ok(scheduler)
        }
    }
}

#[async_trait]
impl SchedulerClientRegistry
    for DashMap<String, Arc<Mutex<SchedulerGrpcClient<Channel>>>>
{
    async fn get_scheduler_client(
        &self,
        scheduler_id: &str,
    ) -> Result<Option<Arc<Mutex<SchedulerGrpcClient<Channel>>>>, BallistaError> {
        Ok(self.get(scheduler_id).map(|v| v.clone()))
    }

    async fn insert_scheduler_client(
        &self,
        scheduler_id: &str,
        client: Arc<Mutex<SchedulerGrpcClient<Channel>>>,
    ) {
        self.insert(scheduler_id.to_string(), client);
    }
}
