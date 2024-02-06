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

use datafusion::error::DataFusionError;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::Partitioning;
use std::convert::TryInto;

use crate::error::BallistaError;

use crate::serde::protobuf;
use datafusion_proto::protobuf as datafusion_protobuf;

use crate::serde::scheduler::{
    Action, ExecutorData, ExecutorMetadata, ExecutorSpecification, PartitionLocation,
    PartitionStats, TaskDefinition,
};
use protobuf::{
    action::ActionType, operator_metric, KeyValuePair, NamedCount, NamedGauge, NamedTime,
};

impl TryInto<protobuf::Action> for Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Action, Self::Error> {
        match self {
            Action::FetchPartition {
                job_id,
                stage_id,
                partition_id,
                path,
                host,
                port,
            } => Ok(protobuf::Action {
                action_type: Some(ActionType::FetchPartition(protobuf::FetchPartition {
                    job_id,
                    stage_id: stage_id as u32,
                    partition_id: partition_id as u32,
                    path,
                    host,
                    port: port as u32,
                })),
                settings: vec![],
            }),
        }
    }
}

impl TryInto<protobuf::PartitionLocation> for &PartitionLocation {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::PartitionLocation, Self::Error> {
        Ok(protobuf::PartitionLocation {
            job_id: self.job_id.clone(),
            stage_id: self.stage_id as u32,
            map_partitions: self.map_partitions.iter().map(|p| *p as u32).collect(),
            output_partition: self.output_partition as u32,
            executor_meta: Some((&self.executor_meta).into()),
            partition_stats: Some(self.partition_stats.into()),
            path: self.path.clone(),
        })
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::PartitionStats> for PartitionStats {
    fn into(self) -> protobuf::PartitionStats {
        let none_value = -1_i64;
        protobuf::PartitionStats {
            num_rows: self.num_rows.map(|n| n as i64).unwrap_or(none_value),
            num_batches: self.num_batches.map(|n| n as i64).unwrap_or(none_value),
            num_bytes: self.num_bytes.map(|n| n as i64).unwrap_or(none_value),
            column_stats: vec![],
        }
    }
}

pub fn hash_partitioning_to_proto(
    output_partitioning: Option<&Partitioning>,
) -> Result<Option<datafusion_protobuf::PhysicalHashRepartition>, BallistaError> {
    match output_partitioning {
        Some(Partitioning::Hash(exprs, partition_count)) => {
            Ok(Some(datafusion_protobuf::PhysicalHashRepartition {
                hash_expr: exprs
                    .iter()
                    .map(|expr| expr.clone().try_into())
                    .collect::<Result<Vec<_>, DataFusionError>>()?,
                partition_count: *partition_count as u64,
            }))
        }
        None => Ok(None),
        other => Err(BallistaError::General(format!(
            "scheduler::to_proto() invalid partitioning for ExecutePartition: {other:?}"
        ))),
    }
}

impl TryInto<protobuf::OperatorMetric> for &MetricValue {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::OperatorMetric, Self::Error> {
        match self {
            MetricValue::OutputRows(count) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::OutputRows(count.value() as u64)),
            }),
            MetricValue::ElapsedCompute(time) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::ElapseTime(time.value() as u64)),
            }),
            MetricValue::SpillCount(count) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::SpillCount(count.value() as u64)),
            }),
            MetricValue::SpilledBytes(count) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::SpilledBytes(count.value() as u64)),
            }),
            MetricValue::CurrentMemoryUsage(gauge) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::CurrentMemoryUsage(
                    gauge.value() as u64
                )),
            }),
            MetricValue::Count { name, count } => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::Count(NamedCount {
                    name: name.to_string(),
                    value: count.value() as u64,
                })),
            }),
            MetricValue::Gauge { name, gauge } => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::Gauge(NamedGauge {
                    name: name.to_string(),
                    value: gauge.value() as u64,
                })),
            }),
            MetricValue::Time { name, time } => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::Time(NamedTime {
                    name: name.to_string(),
                    value: time.value() as u64,
                })),
            }),
            MetricValue::StartTimestamp(timestamp) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::StartTimestamp(
                    timestamp
                        .value()
                        .map(|m| {
                            m.timestamp_nanos_opt()
                                .expect("value can't be represented in nanos")
                        })
                        .unwrap_or(0),
                )),
            }),
            MetricValue::EndTimestamp(timestamp) => Ok(protobuf::OperatorMetric {
                metric: Some(operator_metric::Metric::EndTimestamp(
                    timestamp
                        .value()
                        .map(|m| {
                            m.timestamp_nanos_opt()
                                .expect("value can't be represented in nanos")
                        })
                        .unwrap_or(0),
                )),
            }),
        }
    }
}

impl TryInto<protobuf::OperatorMetricsSet> for &MetricsSet {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::OperatorMetricsSet, Self::Error> {
        let metrics = self
            .iter()
            .map(|m| m.value().try_into())
            .collect::<Result<Vec<_>, BallistaError>>()?;
        Ok(protobuf::OperatorMetricsSet { metrics })
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::ExecutorMetadata> for &ExecutorMetadata {
    fn into(self) -> protobuf::ExecutorMetadata {
        protobuf::ExecutorMetadata {
            id: self.id.clone(),
            host: self.host.clone(),
            port: self.port as u32,
            grpc_port: self.grpc_port as u32,
            specification: Some((&self.specification).into()),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::ExecutorSpecification> for &ExecutorSpecification {
    fn into(self) -> protobuf::ExecutorSpecification {
        protobuf::ExecutorSpecification {
            resources: vec![
                protobuf::ExecutorResource {
                    resource: Some(protobuf::executor_resource::Resource::TaskSlots(
                        self.task_slots,
                    )),
                },
                protobuf::ExecutorResource {
                    resource: Some(protobuf::executor_resource::Resource::Version(
                        self.version.clone(),
                    )),
                },
            ],
        }
    }
}

struct ExecutorResourcePair {
    total: protobuf::executor_resource::Resource,
    available: protobuf::executor_resource::Resource,
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::ExecutorData> for ExecutorData {
    fn into(self) -> protobuf::ExecutorData {
        protobuf::ExecutorData {
            executor_id: self.executor_id,
            executor_version: self.executor_version,
            resources: vec![ExecutorResourcePair {
                total: protobuf::executor_resource::Resource::TaskSlots(
                    self.total_task_slots,
                ),
                available: protobuf::executor_resource::Resource::TaskSlots(
                    self.available_task_slots,
                ),
            }]
            .into_iter()
            .map(|r| protobuf::ExecutorResourcePair {
                total: Some(protobuf::ExecutorResource {
                    resource: Some(r.total),
                }),
                available: Some(protobuf::ExecutorResource {
                    resource: Some(r.available),
                }),
            })
            .collect(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<protobuf::TaskDefinition> for TaskDefinition {
    fn into(self) -> protobuf::TaskDefinition {
        let props = self
            .props
            .iter()
            .map(|(k, v)| KeyValuePair {
                key: k.to_owned(),
                value: v.to_owned(),
            })
            .collect::<Vec<_>>();

        protobuf::TaskDefinition {
            task_id: self.task_id as u32,
            job_id: self.job_id,
            stage_id: self.stage_id as u32,
            stage_attempt_num: self.stage_attempt_num as u32,
            partitions: self.partitions.into_iter().map(|p| p as u32).collect(),
            plan: self.plan,
            output_partitioning: self.output_partitioning,
            session_id: self.session_id,
            launch_time: self.launch_time,
            props,
        }
    }
}
