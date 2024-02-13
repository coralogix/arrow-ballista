// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This crate contains code generated from the Ballista Protocol Buffer Definition as well
//! as convenience code for interacting with the generated code.

use crate::client::BallistaClient;
use crate::{error::BallistaError, serde::scheduler::Action as BallistaAction};

use arrow_flight::sql::ProstMessageExt;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_proto::common::proto_error;
use datafusion_proto::physical_plan::from_proto::{
    parse_physical_expr, parse_protobuf_hash_partitioning,
};
use datafusion_proto::protobuf::{
    LogicalPlanNode, PhysicalPlanNode, PhysicalSortExprNode,
};
use datafusion_proto::{
    convert_required,
    logical_plan::{AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec},
    physical_plan::{AsExecutionPlan, PhysicalExtensionCodec},
};

use moka::future::Cache;
use object_store::ObjectStore;
use prost::Message;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{convert::TryInto, io::Cursor};

use crate::execution_plans::{
    CoalesceTasksExec, ShuffleReaderExec, ShuffleWriterExec, UnresolvedShuffleExec,
};
use crate::serde::protobuf::ballista_physical_plan_node::PhysicalPlanType;
use crate::serde::scheduler::PartitionLocation;
pub use generated::ballista as protobuf;

pub mod generated;
pub mod scheduler;

impl ProstMessageExt for protobuf::Action {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.Action"
    }

    fn as_any(&self) -> arrow_flight::sql::Any {
        arrow_flight::sql::Any {
            type_url: protobuf::Action::type_url().to_string(),
            value: self.encode_to_vec().into(),
        }
    }
}

pub fn decode_protobuf(bytes: &[u8]) -> Result<BallistaAction, BallistaError> {
    let mut buf = Cursor::new(bytes);

    protobuf::Action::decode(&mut buf)
        .map_err(|e| BallistaError::Internal(format!("{e:?}")))
        .and_then(|node| node.try_into())
}

#[derive(Clone, Debug)]
pub struct BallistaCodec<
    T: 'static + AsLogicalPlan = LogicalPlanNode,
    U: 'static + AsExecutionPlan = PhysicalPlanNode,
> {
    logical_extension_codec: Arc<dyn LogicalExtensionCodec>,
    physical_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    logical_plan_repr: PhantomData<T>,
    physical_plan_repr: PhantomData<U>,
}

impl BallistaCodec {
    pub fn new_with_object_store_and_clients(
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, BallistaClient>>,
    ) -> Self {
        Self {
            logical_extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            physical_extension_codec: Arc::new(BallistaPhysicalExtensionCodec {
                object_store,
                clients,
            }),
            logical_plan_repr: PhantomData,
            physical_plan_repr: PhantomData,
        }
    }
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> BallistaCodec<T, U> {
    pub fn new(
        logical_extension_codec: Arc<dyn LogicalExtensionCodec>,
        physical_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> Self {
        Self {
            logical_extension_codec,
            physical_extension_codec,
            logical_plan_repr: PhantomData,
            physical_plan_repr: PhantomData,
        }
    }

    pub fn logical_extension_codec(&self) -> &dyn LogicalExtensionCodec {
        self.logical_extension_codec.as_ref()
    }

    pub fn physical_extension_codec(&self) -> &dyn PhysicalExtensionCodec {
        self.physical_extension_codec.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct BallistaPhysicalExtensionCodec {
    pub object_store: Option<Arc<dyn ObjectStore>>,
    pub clients: Arc<Cache<String, BallistaClient>>,
}

impl BallistaPhysicalExtensionCodec {
    pub fn new(
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, BallistaClient>>,
    ) -> Self {
        Self {
            object_store,
            clients,
        }
    }
}

impl PhysicalExtensionCodec for BallistaPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let ballista_plan: protobuf::BallistaPhysicalPlanNode =
            protobuf::BallistaPhysicalPlanNode::decode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Could not deserialize BallistaPhysicalPlanNode: {e}"
                ))
            })?;

        let ballista_plan =
            ballista_plan.physical_plan_type.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "Could not deserialize BallistaPhysicalPlanNode because it's physical_plan_type is none".to_string()
                )
            })?;

        match ballista_plan {
            PhysicalPlanType::ShuffleWriter(shuffle_writer) => {
                let input = inputs[0].clone();

                let shuffle_output_partitioning = parse_protobuf_hash_partitioning(
                    shuffle_writer.output_partitioning.as_ref(),
                    registry,
                    input.schema().as_ref(),
                )?;

                Ok(Arc::new(ShuffleWriterExec::try_new(
                    shuffle_writer.job_id.clone(),
                    shuffle_writer.stage_id as usize,
                    shuffle_writer
                        .partitions
                        .iter()
                        .map(|p| *p as usize)
                        .collect(),
                    input,
                    "".to_string(), // this is intentional but hacky - the executor will fill this in
                    shuffle_output_partitioning,
                    None,
                )?))
            }
            PhysicalPlanType::ShuffleReader(shuffle_reader) => {
                let schema = Arc::new(convert_required!(shuffle_reader.schema)?);
                let partition_location: Vec<Vec<PartitionLocation>> = shuffle_reader
                    .partition
                    .iter()
                    .map(|p| {
                        p.location
                            .iter()
                            .map(|l| {
                                l.try_into().map_err(|e| {
                                    DataFusionError::Internal(format!(
                                        "Fail to get partition location due to {e:?}"
                                    ))
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, DataFusionError>>()?;
                let shuffle_reader = ShuffleReaderExec::new(
                    partition_location,
                    schema,
                    self.object_store.clone(),
                    self.clients.clone(),
                    shuffle_reader.max_request_num as usize,
                );
                Ok(Arc::new(shuffle_reader))
            }
            PhysicalPlanType::UnresolvedShuffle(unresolved_shuffle) => {
                let schema = Arc::new(convert_required!(unresolved_shuffle.schema)?);
                Ok(Arc::new(UnresolvedShuffleExec {
                    stage_id: unresolved_shuffle.stage_id as usize,
                    schema,
                    input_partition_count: unresolved_shuffle.input_partition_count
                        as usize,
                    output_partition_count: unresolved_shuffle.output_partition_count
                        as usize,
                }))
            }
            PhysicalPlanType::CoalesceTasks(coalesce_task) => {
                let partitions = coalesce_task
                    .partitions
                    .iter()
                    .map(|p| *p as usize)
                    .collect();
                let input = inputs[0].clone();
                let order_by = if coalesce_task.order_by.is_empty() {
                    None
                } else {
                    let exprs = coalesce_task
                        .order_by
                        .iter()
                        .map(|n| {
                            decode_physical_sort_node(
                                n,
                                registry,
                                input.schema().as_ref(),
                            )
                        })
                        .collect::<Result<Vec<_>, DataFusionError>>()?;
                    Some(exprs)
                };
                Ok(Arc::new(CoalesceTasksExec::new(
                    input, partitions, order_by,
                )))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        if let Some(exec) = node.as_any().downcast_ref::<ShuffleWriterExec>() {
            // note that we use shuffle_output_partitioning() rather than output_partitioning()
            // to get the true output partitioning
            let output_partitioning = match exec.shuffle_output_partitioning() {
                Some(Partitioning::Hash(exprs, partition_count)) => {
                    Some(datafusion_proto::protobuf::PhysicalHashRepartition {
                        hash_expr: exprs
                            .iter()
                            .map(|expr| expr.clone().try_into())
                            .collect::<Result<Vec<_>, DataFusionError>>()?,
                        partition_count: *partition_count as u64,
                    })
                }
                None => None,
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "physical_plan::to_proto() invalid partitioning for ShuffleWriterExec: {other:?}"
                    )));
                }
            };

            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleWriter(
                    protobuf::ShuffleWriterExecNode {
                        job_id: exec.job_id().to_string(),
                        stage_id: exec.stage_id() as u32,
                        partitions: exec.partitions().iter().map(|p| *p as u32).collect(),
                        input: None,
                        output_partitioning,
                    },
                )),
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode shuffle writer execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<ShuffleReaderExec>() {
            let mut partition = vec![];
            for location in &exec.partition {
                partition.push(protobuf::ShuffleReaderPartition {
                    location: location
                        .iter()
                        .map(|l| {
                            l.try_into().map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Fail to get partition location due to {e:?}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                });
            }
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleReader(
                    protobuf::ShuffleReaderExecNode {
                        partition,
                        schema: Some(exec.schema().as_ref().try_into()?),
                        max_request_num: exec.shuffle_reader_parallelism as u32,
                    },
                )),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode shuffle reader execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<UnresolvedShuffleExec>() {
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::UnresolvedShuffle(
                    protobuf::UnresolvedShuffleExecNode {
                        stage_id: exec.stage_id as u32,
                        schema: Some(exec.schema().as_ref().try_into()?),
                        input_partition_count: exec.input_partition_count as u32,
                        output_partition_count: exec.output_partition_count as u32,
                    },
                )),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode unresolved shuffle execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<CoalesceTasksExec>() {
            let proto = protobuf::BallistaPhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CoalesceTasks(
                    protobuf::CoalesceTaskExecNode {
                        partitions: exec.partitions().iter().map(|p| *p as u32).collect(),
                        input: None,
                        order_by: exec
                            .output_ordering()
                            .unwrap_or_default()
                            .iter()
                            .map(encode_physical_sort_expr)
                            .collect::<Result<Vec<_>, DataFusionError>>()?,
                    },
                )),
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to encode unresolved shuffle execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else {
            Err(DataFusionError::Internal(
                "unsupported plan type".to_string(),
            ))
        }
    }
}

pub fn encode_physical_sort_expr(
    expr: &PhysicalSortExpr,
) -> Result<PhysicalSortExprNode, DataFusionError> {
    let proto_expr = expr.expr.clone().try_into()?;
    let asc = !expr.options.descending;
    let nulls_first = expr.options.nulls_first;
    Ok(PhysicalSortExprNode {
        expr: Some(Box::new(proto_expr)),
        asc,
        nulls_first,
    })
}

pub fn decode_physical_sort_node(
    n: &PhysicalSortExprNode,
    registry: &dyn FunctionRegistry,
    schema: &Schema,
) -> Result<PhysicalSortExpr, DataFusionError> {
    let proto_expr =
        n.expr
            .as_ref()
            .map(|v| v.as_ref())
            .ok_or(DataFusionError::Internal(
                "Physical sort expression node is missing".to_string(),
            ))?;
    let expr = parse_physical_expr(proto_expr, registry, schema)?;
    Ok(PhysicalSortExpr {
        expr,
        options: SortOptions {
            descending: !n.asc,
            nulls_first: n.nulls_first,
        },
    })
}
