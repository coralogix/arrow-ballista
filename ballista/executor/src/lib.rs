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

#![doc = include_str!("../README.md")]

pub mod collect;
pub mod execution_engine;
pub mod execution_loop;
pub mod executor;
pub mod executor_process;
pub mod executor_server;
pub mod flight_service;
pub mod metrics;
pub mod short_circuit;
pub mod shutdown;
pub mod terminate;

mod cpu_bound_executor;
mod standalone;

pub use standalone::new_standalone_executor;

use log::info;

use ballista_core::serde::protobuf::{
    task_status, FailedTask, OperatorMetricsSet, ShuffleWritePartition, SuccessfulTask,
    TaskStatus,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskExecutionTimes {
    launch_time: u64,
    start_exec_time: u64,
    end_exec_time: u64,
}

#[allow(clippy::too_many_arguments)]
pub fn as_task_status(
    execution_result: ballista_core::error::Result<Vec<ShuffleWritePartition>>,
    executor_id: String,
    job_id: String,
    stage_id: usize,
    task_id: usize,
    partitions: Vec<usize>,
    stage_attempt_num: usize,
    operator_metrics: Option<Vec<OperatorMetricsSet>>,
    execution_times: TaskExecutionTimes,
) -> TaskStatus {
    let metrics = operator_metrics.unwrap_or_default();
    match execution_result {
        Ok(shuffle_partitions) => {
            info!(
                "Task {:?} finished with operator_metrics array size {}",
                task_id,
                metrics.len()
            );
            TaskStatus {
                task_id: task_id as u32,
                job_id,
                stage_id: stage_id as u32,
                stage_attempt_num: stage_attempt_num as u32,
                partitions: partitions.into_iter().map(|p| p as u32).collect(),
                launch_time: execution_times.launch_time,
                start_exec_time: execution_times.start_exec_time,
                end_exec_time: execution_times.end_exec_time,
                metrics,
                status: Some(task_status::Status::Successful(SuccessfulTask {
                    executor_id,
                    partitions: shuffle_partitions,
                })),
            }
        }
        Err(e) => {
            let error_msg = e.to_string();
            info!("Task {:?} failed: {}", task_id, error_msg);

            TaskStatus {
                task_id: task_id as u32,
                job_id,
                stage_id: stage_id as u32,
                stage_attempt_num: stage_attempt_num as u32,
                partitions: partitions.into_iter().map(|p| p as u32).collect(),
                launch_time: execution_times.launch_time,
                start_exec_time: execution_times.start_exec_time,
                end_exec_time: execution_times.end_exec_time,
                metrics,
                status: Some(task_status::Status::Failed(FailedTask::from(e))),
            }
        }
    }
}
