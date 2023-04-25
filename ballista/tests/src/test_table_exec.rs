use crate::proto;
use crate::test_table::TestTable;
use ballista_executor::global_limit_daemon::GlobalLimitDaemon;
use ballista_executor::global_limit_stream::GlobalLimitStream;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use std::any::Any;
use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct TestTableExec {
    pub(crate) table: Arc<TestTable>,
    pub(crate) limit: Option<usize>,
    pub(crate) projection: Option<Vec<usize>>,
}

impl TestTableExec {
    pub(crate) fn new(
        table: Arc<TestTable>,
        limit: Option<usize>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            table,
            limit,
            projection,
        }
    }
}

impl ExecutionPlan for TestTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.table.parallelism)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        panic!("Can't add children to TestTableExec")
    }

    fn execute(
        &self,
        // Each partition behaves exactly the same
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let record_batch = RecordBatch::try_new(
            self.schema(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )?;

        let mut data = vec![record_batch; self.limit.unwrap_or(1000) / 5];

        if let Some(limit) = self.limit {
            data.truncate(limit);
        }

        let stream = MemoryStream::try_new(data, self.schema(), self.projection.clone())?;

        if let Some(daemon) = context
            .session_config()
            .get_extension::<GlobalLimitDaemon>()
        {
            if let Some(task_id) = context.task_id() {
                let config = daemon.register_limit(task_id.clone())?;
                let boxed: Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(stream);
                let limited_steam = GlobalLimitStream::new(boxed, config, task_id);
                println!("##### RUNNING WITH LIMIT ####");
                return Ok(Box::pin(limited_steam));
            }
        }

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl TryFrom<proto::TestTableExec> for TestTableExec {
    type Error = DataFusionError;

    fn try_from(value: proto::TestTableExec) -> Result<Self> {
        let table = Arc::new(TestTable::from(value.table.unwrap()));
        let limit = value.limit.map(|x| x as usize);
        let projection = value
            .projection
            .into_iter()
            .map(|x| x as usize)
            .collect::<Vec<_>>();

        let projection_opt = if projection.is_empty() {
            None
        } else {
            Some(projection)
        };

        Ok(Self {
            table,
            limit,
            projection: projection_opt,
        })
    }
}

impl From<TestTableExec> for proto::TestTableExec {
    fn from(value: TestTableExec) -> Self {
        Self {
            table: Some(value.table.as_ref().clone().into()),
            limit: value.limit.map(|x| x as u64),
            projection: value
                .projection
                .unwrap_or_default()
                .into_iter()
                .map(|x| x as u64)
                .collect(),
        }
    }
}
