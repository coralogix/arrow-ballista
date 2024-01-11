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

    use std::{convert::TryInto, net::SocketAddr, path::PathBuf, sync::Arc, vec};

    use anyhow::Context;
    use ballista_core::{
        serde::{
            protobuf::{
                execute_query_params::Query, executor_registration::OptionalHost,
                executor_resource::Resource, scheduler_grpc_client::SchedulerGrpcClient,
                ExecuteQueryParams, ExecutorRegistration, ExecutorResource,
                ExecutorSpecification, GetJobStatusParams, JobStatus, SuccessfulJob,
            },
            BallistaCodec,
        },
        utils::create_grpc_client_connection,
    };
    use ballista_executor::{
        circuit_breaker::client::CircuitBreakerClientConfig, execution_loop,
        executor::Executor, executor_server, metrics::LoggingMetricsCollector,
        shutdown::ShutdownNotifier,
    };
    use ballista_scheduler::standalone::new_standalone_scheduler_with_codec;
    use datafusion::{
        common::DFSchema,
        config::Extensions,
        datasource::{provider_as_source, TableProvider},
        execution::runtime_env::{RuntimeConfig, RuntimeEnv},
        logical_expr::{LogicalPlan, LogicalPlanBuilder, TableScan},
        sql::TableReference,
    };
    use datafusion_proto::{
        logical_plan::{AsLogicalPlan, LogicalExtensionCodec},
        physical_plan::PhysicalExtensionCodec,
        protobuf::LogicalPlanNode,
    };
    use tokio::{
        sync::mpsc,
        time::{sleep, Duration},
    };
    use tonic::transport::Channel;

    use crate::{
        test_logical_codec::TestLogicalCodec, test_physical_codec::TestPhysicalCodec,
        test_table::TestTable,
    };

    use ballista_core::serde::protobuf::job_status::Status;

    lazy_static::lazy_static! {
        pub static ref WORKSPACE_DIR: PathBuf =
                PathBuf::from(std::env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        let row_limit = 100;

        let mut test_env = TestEnvironment::new(
            2,
            Arc::new(TestLogicalCodec::new()),
            Arc::new(TestPhysicalCodec::default()),
        )
        .await;

        let test_table = Arc::new(TestTable::new(
            2,
            row_limit,
            "test-table".to_owned(),
            111,
            true,
        ));

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

        LogicalPlanNode::try_from_logical_plan(&node, test_env.logical_codec.as_ref())
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

        let result = test_env
            .scheduler_client
            .execute_query(request)
            .await
            .context("Executing query")
            .unwrap();

        let job_id = result.into_inner().job_id;

        let mut successful_job =
            await_job_completion(&mut test_env.scheduler_client, &job_id)
                .await
                .unwrap();

        assert!(successful_job.circuit_breaker_tripped);

        successful_job.circuit_breaker_tripped_labels.sort();

        assert_eq!(
            successful_job.circuit_breaker_tripped_labels,
            vec![
                "partition-0".to_owned(),
                "partition-1".to_owned(),
                "test".to_owned(),
                "test-table".to_owned()
            ]
        );

        let num_rows = get_num_rows(successful_job);
        assert!(num_rows.len() == 2);
        let sum_num_rows = num_rows[0] + num_rows[1];

        assert!(sum_num_rows > row_limit.try_into().unwrap());
        assert!(sum_num_rows < row_limit as i64 * 2);
    }

    #[tokio::test]
    async fn test_circuit_breaker_union() {
        let row_limit_1 = 15;
        let row_limit_2 = 100;

        let mut test_env = TestEnvironment::new(
            2,
            Arc::new(TestLogicalCodec::new()),
            Arc::new(TestPhysicalCodec::default()),
        )
        .await;

        let test_table1 = Arc::new(TestTable::new(
            2,
            row_limit_1,
            "first-test-table".to_owned(),
            111,
            false, // <- this is important so the stage doesn't get preempted here
        ));

        let test_table2 = Arc::new(TestTable::new(
            2,
            row_limit_2,
            "second-test-table".to_owned(),
            222,
            false, // <- this is important so the stage doesn't get preempted here
        ));

        let reference1: TableReference = "test_table1".into();
        let schema1 = DFSchema::try_from_qualified_schema(
            reference1.clone(),
            test_table1.schema().as_ref(),
        )
        .context("Creating schema")
        .unwrap();

        let reference2: TableReference = "test_table2".into();
        let schema2 = DFSchema::try_from_qualified_schema(
            reference2.clone(),
            test_table2.schema().as_ref(),
        )
        .context("Creating schema")
        .unwrap();

        let test_data1 = TableScan {
            table_name: reference1,
            source: provider_as_source(test_table1),
            fetch: None,
            filters: vec![],
            projection: None,
            projected_schema: Arc::new(schema1),
        };

        let test_data2 = TableScan {
            table_name: reference2,
            source: provider_as_source(test_table2),
            fetch: None,
            filters: vec![],
            projection: None,
            projected_schema: Arc::new(schema2),
        };

        let builder = LogicalPlanBuilder::from(LogicalPlan::TableScan(test_data1));

        let plan = builder
            .union(LogicalPlan::TableScan(test_data2))
            .context("Building union")
            .unwrap()
            .build()
            .context("Building logical plan")
            .unwrap();

        let mut buf = vec![];

        LogicalPlanNode::try_from_logical_plan(&plan, test_env.logical_codec.as_ref())
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

        let result = test_env
            .scheduler_client
            .execute_query(request)
            .await
            .context("Executing query")
            .unwrap();

        let job_id = result.into_inner().job_id;

        let mut successful_job =
            await_job_completion(&mut test_env.scheduler_client, &job_id)
                .await
                .unwrap();

        assert!(successful_job.circuit_breaker_tripped);

        println!(
            "circuit_breaker_tripped_labels: {:?}",
            successful_job.circuit_breaker_tripped_labels
        );

        successful_job.circuit_breaker_tripped_labels.sort();

        assert_eq!(
            successful_job.circuit_breaker_tripped_labels,
            vec![
                "first-test-table".to_owned(),
                "partition-0".to_owned(),
                "partition-1".to_owned(),
                "second-test-table".to_owned(),
                "test".to_owned()
            ]
        );

        let num_rows = get_num_rows(successful_job);
        assert_eq!(num_rows.len(), 4);

        // Not sure why this is inverted, I assume it's because of how UnionExec works
        let num_rows_table1 = num_rows[2] + num_rows[3];
        let num_rows_table2 = num_rows[0] + num_rows[1];

        println!("num_rows table 1: {}", num_rows_table1);
        println!("num rows table 2: {}", num_rows_table2);

        assert!(num_rows_table1 > row_limit_1 as i64);
        assert!(num_rows_table2 > row_limit_2 as i64);
    }

    fn get_num_rows(job: SuccessfulJob) -> Vec<i64> {
        let partitions = job.partition_location;

        let mut result = vec![];

        for partition in partitions {
            let num_rows = partition
                .partition_stats
                .clone()
                .context("Get partition stats")
                .unwrap()
                .num_rows;

            result.push(num_rows);
        }

        result
    }

    struct TestEnvironment {
        pub scheduler_client: SchedulerGrpcClient<Channel>,
        pub logical_codec: Arc<dyn LogicalExtensionCodec>,
    }

    impl TestEnvironment {
        async fn new(
            num_executors: usize,
            logical_codec: Arc<dyn LogicalExtensionCodec>,
            physical_codec: Arc<dyn PhysicalExtensionCodec>,
        ) -> Self {
            let scheduler_socket =
                start_scheduler_local(logical_codec.clone(), physical_codec.clone())
                    .await;

            let scheduler_url = format!("http://localhost:{}", scheduler_socket.port());

            let connection = create_grpc_client_connection(scheduler_url.clone())
                .await
                .context("Connecting to scheduler")
                .unwrap();

            let scheduler_client = SchedulerGrpcClient::new(connection);

            let codec = BallistaCodec::new(logical_codec.clone(), physical_codec.clone());

            let shutdown_not = Arc::new(ShutdownNotifier::new());

            start_executors_local(
                num_executors,
                scheduler_client.clone(),
                codec,
                shutdown_not.clone(),
            )
            .await;

            Self {
                scheduler_client,
                logical_codec,
            }
        }
    }

    async fn await_job_completion(
        scheduler_client: &mut SchedulerGrpcClient<Channel>,
        job_id: &str,
    ) -> Result<SuccessfulJob, String> {
        let mut job_status = scheduler_client
            .get_job_status(GetJobStatusParams {
                job_id: job_id.to_owned(),
            })
            .await
            .map_err(|e| format!("Job status request for job {} failed: {}", job_id, e))?
            .into_inner();

        let mut attempts = 60;

        while attempts > 0 {
            sleep(Duration::from_millis(1000)).await;

            if let Some(JobStatus {
                job_id: _,
                job_name: _,
                status: Some(status),
            }) = &job_status.status
            {
                match status {
                    Status::Failed(failed_job) => {
                        return Err(format!(
                            "Job {} failed: {:?}",
                            job_id, failed_job.error
                        ));
                    }
                    Status::Successful(s) => {
                        return Ok(s.clone());
                    }
                    _ => {}
                }
            }

            attempts -= 1;

            job_status = scheduler_client
                .get_job_status(GetJobStatusParams {
                    job_id: job_id.to_owned(),
                })
                .await
                .map_err(|e| {
                    format!("Job status request for job {} failed: {}", job_id, e)
                })?
                .into_inner();
        }

        Err(format!(
            "Job {} did not complete, last status: {:?}",
            job_id, job_status.status
        ))
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
        shutdown_not: Arc<ShutdownNotifier>,
    ) {
        let work_dir = "/tmp";
        let cfg = RuntimeConfig::new();
        let runtime = Arc::new(RuntimeEnv::new(cfg).unwrap());

        for i in 0..n {
            let specification = ExecutorSpecification {
                resources: vec![
                    ExecutorResource {
                        resource: Some(Resource::TaskSlots(1)),
                    },
                    ExecutorResource {
                        resource: Some(Resource::Version("test".to_string())),
                    },
                ],
            };

            let port = 50051 + i as u32;
            let grpc_port = (50052 + n + i) as u32;

            let metadata = ExecutorRegistration {
                id: format!("executor-{}", i),
                port,
                grpc_port,
                specification: Some(specification),
                optional_host: Some(OptionalHost::Host("localhost".to_owned())),
            };

            let metrics_collector = Arc::new(LoggingMetricsCollector {});

            let concurrent_tasks = 1;

            let execution_engine = None;

            let executor = Executor::new(
                metadata,
                work_dir,
                None,
                runtime.clone(),
                metrics_collector,
                concurrent_tasks,
                execution_engine,
            );

            let stop_send = mpsc::channel(1).0;

            let executor = Arc::new(executor);

            executor_server::startup(
                scheduler.clone(),
                "0.0.0.0".to_owned(),
                executor.clone(),
                codec.clone(),
                stop_send,
                shutdown_not.as_ref(),
                Extensions::new(),
                CircuitBreakerClientConfig::default(),
            )
            .await
            .context(format!("Starting executor {}", i))
            .unwrap();

            // Don't save the handle as this one cannot be interrupted and just has to be dropped
            tokio::spawn(execution_loop::poll_loop(
                scheduler.clone(),
                executor,
                codec.clone(),
                Extensions::default(),
            ));
        }
    }
}
