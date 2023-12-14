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

pub mod circuit_breaker;
pub mod collect;
pub mod execution_engine;
pub mod execution_loop;
pub mod executor;
pub mod executor_process;
pub mod executor_server;
pub mod flight_service;
pub mod metrics;
pub mod shutdown;
pub mod terminate;

mod cpu_bound_executor;
mod scheduler_client_registry;
mod standalone;

pub use standalone::new_standalone_executor;

use log::info;
use tracing::error;

use ballista_core::serde::protobuf::{
    task_status, FailedTask, OperatorMetricsSet, ShuffleWritePartition, SuccessfulTask,
    TaskStatus,
};

/// Terminate the executor in cases where we are in an unrecoverable situation.
///
/// On unix systems this will send a SIGTERM to the current process to attempt a graceful shutdown
#[cfg(unix)]
pub(crate) fn halt_and_catch_fire() {
    let pid = std::process::id();

    if let Err(err) = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid as i32),
        nix::sys::signal::Signal::SIGTERM,
    ) {
        error!(?err, "failed to send SIGTERM to current process, exiting");
        std::process::exit(1);
    }
}

/// Terminate the executor in cases where we are in an unrecoverable situation.
///
/// Currently this will just exit the process
#[cfg(not(unix))]
pub(crate) fn halt_and_catch_fire() {
    std::process::exit(1);
}

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
