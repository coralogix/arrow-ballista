#[cfg(test)]
mod proto;
#[cfg(test)]
mod test_logical_codec;
#[cfg(test)]
mod test_physical_codec;
#[cfg(test)]
mod test_table;
#[cfg(test)]
mod test_table_exec;

#[cfg(test)]
mod tests {

    use std::{net::SocketAddr, path::PathBuf, sync::Arc, vec};

    use anyhow::Context;
    use ballista_core::{
        serde::{
            protobuf::{
                execute_query_params::Query, executor_resource::Resource,
                scheduler_grpc_client::SchedulerGrpcClient, ExecuteQueryParams,
                ExecutorRegistration, ExecutorResource, ExecutorSpecification,
                GetJobStatusParams,
            },
            BallistaCodec,
        },
        utils::create_grpc_client_connection,
    };
    use ballista_executor::{
        executor::Executor,
        executor_server::{self, ServerHandle},
        metrics::LoggingMetricsCollector,
        shutdown::ShutdownNotifier,
    };
    use ballista_scheduler::standalone::new_standalone_scheduler_with_codec;
    use datafusion::{
        common::DFSchema,
        config::Extensions,
        datasource::{provider_as_source, TableProvider},
        execution::runtime_env::{RuntimeConfig, RuntimeEnv},
        logical_expr::{LogicalPlan, TableScan},
        sql::TableReference,
    };
    use datafusion_proto::{
        logical_plan::{AsLogicalPlan, LogicalExtensionCodec},
        physical_plan::PhysicalExtensionCodec,
        protobuf::LogicalPlanNode,
    };
    use tokio::sync::mpsc;
    use tonic::transport::Channel;

    use crate::{
        test_logical_codec::TestLogicalCodec, test_physical_codec::TestPhysicalCodec,
        test_table::TestTable,
    };

    lazy_static::lazy_static! {
        pub static ref WORKSPACE_DIR: PathBuf =
                PathBuf::from(std::env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    }

    #[tokio::test]
    async fn test_global_limit() {
        let logical_codec = Arc::new(TestLogicalCodec::new());
        let physical_codec = Arc::new(TestPhysicalCodec::new());

        let scheduler_socket =
            start_scheduler_local(logical_codec.clone(), physical_codec.clone()).await;

        // Wait for scheduler to start
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        let scheduler_url = format!("http://localhost:{}", scheduler_socket.port());

        println!("### SCHEDULER URL: {}", scheduler_url);

        let connection = create_grpc_client_connection(scheduler_url.clone())
            .await
            .context("Connecting to scheduler")
            .unwrap();

        let mut scheduler_client = SchedulerGrpcClient::new(connection);

        let codec = BallistaCodec::new(logical_codec.clone(), physical_codec.clone());

        let executors = start_executors_local(2, scheduler_client.clone(), codec).await;

        // Wait for executors to start
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        let test_table = Arc::new(TestTable::new(1));

        let reference: TableReference = "test_table".into();
        let schema = DFSchema::try_from_qualified_schema(
            reference.clone(),
            test_table.schema().as_ref(),
        )
        .context("Creating schema")
        .unwrap();

        let test_data = TableScan {
            table_name: reference,
            source: provider_as_source(test_table),
            fetch: None,
            filters: vec![],
            projection: None,
            projected_schema: Arc::new(schema),
        };

        let node = LogicalPlan::TableScan(test_data);

        let mut buf = vec![];

        LogicalPlanNode::try_from_logical_plan(&node, logical_codec.as_ref())
            .context("Converting to protobuf")
            .unwrap()
            .try_encode(&mut buf)
            .context("Encoding protobuf")
            .unwrap();

        let request = ExecuteQueryParams {
            settings: vec![],
            optional_session_id: None,
            query: Some(Query::LogicalPlan(buf)),
        };

        let result = scheduler_client
            .execute_query(request)
            .await
            .context("Executing query")
            .unwrap();

        println!("### RESULT: {:?}", result);

        // sleep
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        let x = scheduler_client
            .get_job_status(GetJobStatusParams {
                job_id: result.into_inner().job_id,
            })
            .await
            .unwrap();

        println!("### STATUS: {:?}", x);

        for ex in executors {
            ex.abort();
        }
    }

    async fn start_scheduler_local(
        logical_codec: Arc<dyn LogicalExtensionCodec>,
        physical_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> SocketAddr {
        new_standalone_scheduler_with_codec(physical_codec, logical_codec)
            .await
            .context("Starting scheduler process")
            .unwrap()
    }

    async fn start_executors_local(
        n: usize,
        scheduler: SchedulerGrpcClient<Channel>,
        codec: BallistaCodec,
    ) -> Vec<ServerHandle> {
        let work_dir = "/tmp";
        let cfg = RuntimeConfig::new();
        let runtime = Arc::new(RuntimeEnv::new(cfg).unwrap());

        let mut handles = Vec::with_capacity(n);

        for i in 0..n {
            let specification = ExecutorSpecification {
                resources: vec![ExecutorResource {
                    resource: Some(Resource::TaskSlots(1)),
                }],
            };

            let metadata = ExecutorRegistration {
                id: format!("executor-{}", i),
                port: 50051 + i as u32,
                grpc_port: (50052 + n + i) as u32,
                specification: Some(specification),
                optional_host: None,
            };

            let metrics_collector = Arc::new(LoggingMetricsCollector {});

            let concurrent_tasks = 1;

            let execution_engine = None;

            let executor = Executor::new(
                metadata,
                work_dir,
                runtime.clone(),
                metrics_collector,
                concurrent_tasks,
                execution_engine,
            );

            let stop_send = mpsc::channel(1).0;

            let shutdown_not = Arc::new(ShutdownNotifier::new());

            let handle = executor_server::startup(
                scheduler.clone(),
                "0.0.0.0".to_owned(),
                Arc::new(executor),
                codec.clone(),
                stop_send,
                shutdown_not.as_ref(),
                Extensions::new(),
            )
            .await
            .context(format!("Starting executor {}", i))
            .unwrap();

            handles.push(handle);
        }

        handles
    }
}
