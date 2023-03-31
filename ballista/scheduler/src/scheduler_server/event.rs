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

use crate::state::executor_manager::ExecutorReservation;

use datafusion::{error::DataFusionError, logical_expr::LogicalPlan};

use crate::state::execution_graph::RunningTaskInfo;
use ballista_core::{error::BallistaError, serde::protobuf::TaskStatus};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

#[derive(Clone)]
pub enum ErrorType {
    Internal,
    External,
}

impl ErrorType {
    pub fn from_ballista_error(err: BallistaError) -> Self {
        match err {
            BallistaError::DataFusionError(DataFusionError::External(_)) => {
                ErrorType::External
            }
            _ => ErrorType::Internal,
        }
    }
}
#[derive(Clone)]
pub enum QueryStageSchedulerEvent {
    JobQueued {
        job_id: String,
        job_name: String,
        session_ctx: Arc<SessionContext>,
        plan: Box<LogicalPlan>,
        queued_at: u64,
    },
    JobSubmitted {
        job_id: String,
        queued_at: u64,
        submitted_at: u64,
        resubmit: bool,
    },
    // For a job which failed during planning
    JobPlanningFailed {
        job_id: String,
        fail_message: String,
        queued_at: u64,
        failed_at: u64,
        error_type: ErrorType,
    },
    JobFinished {
        job_id: String,
        queued_at: u64,
        completed_at: u64,
    },
    // For a job fails with its execution graph setting failed
    JobRunningFailed {
        job_id: String,
        fail_message: String,
        queued_at: u64,
        failed_at: u64,
        error_type: ErrorType,
    },
    JobUpdated(String),
    JobCancel(String),
    JobDataClean(String),
    TaskUpdating(String, Vec<TaskStatus>),
    ReservationOffering(Vec<ExecutorReservation>),
    ExecutorLost(String, Option<String>),
    CancelTasks(Vec<RunningTaskInfo>),
}
