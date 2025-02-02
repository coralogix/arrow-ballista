use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use std::any::Any;
use std::sync::Arc;

use crate::proto;
use crate::test_table_exec::TestTableExec;

#[derive(Debug, Clone)]
pub(crate) struct TestTable {
    pub parallelism: usize,
    pub schema: SchemaRef,
    pub global_limit: u64,
    pub state_id: String,
    pub value: i32,
    pub preempt_stage: bool,
}

impl TestTable {
    pub(crate) fn new(
        parallelism: usize,
        global_limit: u64,
        state_id: String,
        value: i32,
        preempt_stage: bool,
    ) -> Self {
        let schema =
            SchemaRef::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        Self {
            parallelism,
            schema,
            global_limit,
            state_id,
            value,
            preempt_stage,
        }
    }
}

#[async_trait]
impl TableProvider for TestTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(filters.is_empty());

        Ok(Arc::new(TestTableExec::new(
            Arc::new(self.clone()),
            limit,
            projection.cloned(),
            self.global_limit,
            self.state_id.clone(),
            self.value,
            self.preempt_stage,
        )))
    }
}

impl From<proto::TestTable> for TestTable {
    fn from(proto: proto::TestTable) -> Self {
        Self::new(
            proto.parallelism as usize,
            proto.global_limit,
            proto.state_id,
            proto.value as i32,
            proto.preempt_stage,
        )
    }
}

impl From<TestTable> for proto::TestTable {
    fn from(table: TestTable) -> Self {
        Self {
            parallelism: table.parallelism as u64,
            global_limit: table.global_limit,
            state_id: table.state_id,
            value: table.value as u32,
            preempt_stage: table.preempt_stage,
        }
    }
}
