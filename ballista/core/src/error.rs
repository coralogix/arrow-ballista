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

//! Ballista error types

use std::{
    error::Error,
    fmt::{Display, Formatter},
    io, result,
};

use crate::serde::protobuf::{
    failed_job::{
        self, arrow_error,
        datafusion_error::{self, parquet_error, schema_error},
        General, Internal, NotImplemented,
    },
    failed_task::FailedReason,
};
use crate::serde::protobuf::{ExecutionError, FailedTask, FetchPartitionError, IoError};
use datafusion::error::DataFusionError;
use datafusion::{arrow::error::ArrowError, parquet::errors::ParquetError};
use futures::future::Aborted;
use itertools::Itertools;
use sqlparser::parser::{self, ParserError};

pub type Result<T> = result::Result<T, BallistaError>;

/// Ballista error
#[derive(Debug)]
pub enum BallistaError {
    NotImplemented(String),
    General(String),
    Internal(String),
    ArrowError(ArrowError),
    DataFusionError(DataFusionError),
    SqlError(parser::ParserError),
    IoError(io::Error),
    // ReqwestError(reqwest::Error),
    // HttpError(http::Error),
    // KubeAPIError(kube::error::Error),
    // KubeAPIRequestError(k8s_openapi::RequestError),
    // KubeAPIResponseError(k8s_openapi::ResponseError),
    TonicError(tonic::transport::Error),
    GrpcError(tonic::Status),
    GrpcConnectionError(String),
    TokioError(tokio::task::JoinError),
    GrpcActionError(String),
    // (executor_id, map_stage_id, map_partition_id, message)
    FetchFailed(String, usize, usize, String),
    Cancelled,
}

#[allow(clippy::from_over_into)]
impl<T> Into<Result<T>> for BallistaError {
    fn into(self) -> Result<T> {
        Err(self)
    }
}

pub fn ballista_error(message: &str) -> BallistaError {
    BallistaError::General(message.to_owned())
}

impl From<String> for BallistaError {
    fn from(e: String) -> Self {
        BallistaError::General(e)
    }
}

impl From<ArrowError> for BallistaError {
    fn from(e: ArrowError) -> Self {
        match e {
            ArrowError::ExternalError(e)
                if e.downcast_ref::<BallistaError>().is_some() =>
            {
                *e.downcast::<BallistaError>().unwrap()
            }
            ArrowError::ExternalError(e)
                if e.downcast_ref::<DataFusionError>().is_some() =>
            {
                BallistaError::DataFusionError(*e.downcast::<DataFusionError>().unwrap())
            }
            other => BallistaError::ArrowError(other),
        }
    }
}

impl From<parser::ParserError> for BallistaError {
    fn from(e: parser::ParserError) -> Self {
        BallistaError::SqlError(e)
    }
}

impl From<DataFusionError> for BallistaError {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::ArrowError(e) => Self::from(e),
            _ => BallistaError::DataFusionError(e),
        }
    }
}

impl From<io::Error> for BallistaError {
    fn from(e: io::Error) -> Self {
        BallistaError::IoError(e)
    }
}

// impl From<reqwest::Error> for BallistaError {
//     fn from(e: reqwest::Error) -> Self {
//         BallistaError::ReqwestError(e)
//     }
// }
//
// impl From<http::Error> for BallistaError {
//     fn from(e: http::Error) -> Self {
//         BallistaError::HttpError(e)
//     }
// }

// impl From<kube::error::Error> for BallistaError {
//     fn from(e: kube::error::Error) -> Self {
//         BallistaError::KubeAPIError(e)
//     }
// }

// impl From<k8s_openapi::RequestError> for BallistaError {
//     fn from(e: k8s_openapi::RequestError) -> Self {
//         BallistaError::KubeAPIRequestError(e)
//     }
// }

// impl From<k8s_openapi::ResponseError> for BallistaError {
//     fn from(e: k8s_openapi::ResponseError) -> Self {
//         BallistaError::KubeAPIResponseError(e)
//     }
// }

impl From<tonic::transport::Error> for BallistaError {
    fn from(e: tonic::transport::Error) -> Self {
        BallistaError::TonicError(e)
    }
}

impl From<tonic::Status> for BallistaError {
    fn from(e: tonic::Status) -> Self {
        BallistaError::GrpcError(e)
    }
}

impl From<tokio::task::JoinError> for BallistaError {
    fn from(e: tokio::task::JoinError) -> Self {
        BallistaError::TokioError(e)
    }
}

impl From<datafusion_proto::logical_plan::from_proto::Error> for BallistaError {
    fn from(e: datafusion_proto::logical_plan::from_proto::Error) -> Self {
        BallistaError::General(e.to_string())
    }
}

impl From<datafusion_proto::logical_plan::to_proto::Error> for BallistaError {
    fn from(e: datafusion_proto::logical_plan::to_proto::Error) -> Self {
        BallistaError::General(e.to_string())
    }
}

impl From<futures::future::Aborted> for BallistaError {
    fn from(_: Aborted) -> Self {
        BallistaError::Cancelled
    }
}

impl From<&ParserError> for failed_job::parser_error::Error {
    fn from(value: &ParserError) -> Self {
        match value {
            parser::ParserError::TokenizerError(message) => {
                failed_job::parser_error::Error::TokenizerError(
                    failed_job::parser_error::TokenizerError {
                        message: message.clone(),
                    },
                )
            }
            parser::ParserError::ParserError(message) => {
                failed_job::parser_error::Error::ParserError(
                    failed_job::parser_error::ParserError {
                        message: message.clone(),
                    },
                )
            }
            parser::ParserError::RecursionLimitExceeded => {
                failed_job::parser_error::Error::RecursionLimitExceeded(
                    failed_job::parser_error::RecursionLimitExceeded {},
                )
            }
        }
    }
}

impl From<&DataFusionError> for failed_job::datafusion_error::Error {
    fn from(value: &DataFusionError) -> Self {
        match value {
            DataFusionError::ArrowError(error) => {
                    datafusion_error::Error::ArrowError(
                            failed_job::ArrowError {
                                error: Some(error.into()),
                            },
                        )
            },
            DataFusionError::ParquetError(err) => match err {
                datafusion::parquet::errors::ParquetError::General(message) => {
                    datafusion_error::Error::ParquetError(
                            failed_job::datafusion_error::ParquetError {
                                error: Some(parquet_error::Error::General(
                                    parquet_error::General { message: message.clone() },
                                )),
                            },
                        )
                }
                datafusion::parquet::errors::ParquetError::NYI(message) => {
                    datafusion_error::Error::ParquetError(
                            failed_job::datafusion_error::ParquetError {
                                error: Some(parquet_error::Error::NotYetImplemented(
                                    parquet_error::NotYetImplemented { message: message.clone() },
                                )),
                            },
                        )
                }
                datafusion::parquet::errors::ParquetError::EOF(message) => {
                    datafusion_error::Error::ParquetError(
                            failed_job::datafusion_error::ParquetError {
                                error: Some(parquet_error::Error::Eof(
                                    parquet_error::Eof { message: message.clone() },
                                )),
                            },
                        )
                }
                datafusion::parquet::errors::ParquetError::ArrowError(message) => {
                    datafusion_error::Error::ParquetError(
                            failed_job::datafusion_error::ParquetError {
                                error: Some(parquet_error::Error::ArrowError(
                                    parquet_error::ArrowError { message: message.clone() },
                                )),
                            },
                        )
                }
                datafusion::parquet::errors::ParquetError::IndexOutOfBound(
                    index,
                    bound,
                ) => {
                    datafusion_error::Error::ParquetError(
                            failed_job::datafusion_error::ParquetError {
                                error: Some(parquet_error::Error::IndexOutOfBound(
                                    parquet_error::IndexOutOfBound {
                                        index: *index as u32,
                                        bound: *bound as u32,
                                    },
                                )),
                            },
                        )
                }
                datafusion::parquet::errors::ParquetError::External(message) => {
                    datafusion_error::Error::ParquetError(
                        failed_job::datafusion_error::ParquetError {
                            error: Some(parquet_error::Error::External(
                                parquet_error::External { message: message.to_string() },
                            )),
                        },
                    )
                }
            },
            DataFusionError::ObjectStore(err) => match err {
                object_store::Error::Generic { store, source } => {
                    datafusion_error::Error::ObjectStore(
                            failed_job::datafusion_error::ObjectStore {
                                error: Some(failed_job::datafusion_error::object_store::Error::Generic(
                                    failed_job::datafusion_error::object_store::Generic {
                                        store: store.to_string(),
                                        source: source.to_string(),
                                    },
                                )),
                            },
                        )
                }
                object_store::Error::NotFound { path, source } => datafusion_error::Error::ObjectStore(
                        failed_job::datafusion_error::ObjectStore {
                            error: Some(failed_job::datafusion_error::object_store::Error::NotFound(
                                failed_job::datafusion_error::object_store::NotFound {
                                    path: path.clone(),
                                    source: source.to_string(),
                                },
                            )),
                        },
                    ),
                object_store::Error::InvalidPath { source } => datafusion_error::Error::ObjectStore(
                        failed_job::datafusion_error::ObjectStore {
                            error: Some(failed_job::datafusion_error::object_store::Error::InvalidPath(
                                failed_job::datafusion_error::object_store::InvalidPath {
                                    source: source.to_string(),
                                },
                            )),
                        },
                    ),
                object_store::Error::JoinError { source } => datafusion_error::Error::ObjectStore(
                        failed_job::datafusion_error::ObjectStore {
                            error: Some(failed_job::datafusion_error::object_store::Error::JoinError(
                                failed_job::datafusion_error::object_store::JoinError {
                                    source: source.to_string(),
                                },
                            )),
                        },
                    ),
                object_store::Error::NotSupported { source } => datafusion_error::Error::ObjectStore(
                        failed_job::datafusion_error::ObjectStore {
                            error: Some(failed_job::datafusion_error::object_store::Error::NotSupported(
                                failed_job::datafusion_error::object_store::NotSupported{
                                    source: source.to_string(),
                                },
                            )),
                        },
                    ),
                object_store::Error::AlreadyExists { path, source } => datafusion_error::Error::ObjectStore(
                        failed_job::datafusion_error::ObjectStore {
                            error: Some(failed_job::datafusion_error::object_store::Error::AlreadyExists(
                                failed_job::datafusion_error::object_store::AlreadyExists {
                                    path: path.clone(),
                                    source: source.to_string(),
                                },
                            )),
                        },
                    ),
                object_store::Error::NotImplemented => datafusion_error::Error::ObjectStore(
                        failed_job::datafusion_error::ObjectStore {
                            error: Some(failed_job::datafusion_error::object_store::Error::NotImplemented(
                                failed_job::datafusion_error::object_store::NotImplemented {
                                },
                            )),
                        },
                    ),
                object_store::Error::UnknownConfigurationKey { store, key } => datafusion_error::Error::ObjectStore(
                        failed_job::datafusion_error::ObjectStore {
                            error: Some(failed_job::datafusion_error::object_store::Error::UnknownConfigurationKey(
                                failed_job::datafusion_error::object_store::UnknownConfigurationKey {
                                    store: store.to_string(), key: key.clone()
                                },
                            )),
                        },
                    )
            },
            DataFusionError::IoError(err) => datafusion_error::Error::IoError(
                    failed_job::datafusion_error::IoError {
                        message: err.to_string()
                    },
                ),
            DataFusionError::SQL(error) => {
                datafusion_error::Error::ParserError(
                        failed_job::ParserError { error: Some(error.into())},
                    )
            },
            DataFusionError::NotImplemented(message) => datafusion_error::Error::NotImplemented(
                    failed_job::datafusion_error::NotImplemented {
                        message: message.clone()
                    },
                ),
            DataFusionError::Internal(message) => datafusion_error::Error::Internal(
                    failed_job::datafusion_error::Internal {
                        message: message.clone()
                    },
                ),
            DataFusionError::Plan(message) => datafusion_error::Error::Plan(
                    failed_job::datafusion_error::Plan {
                        message: message.clone()
                    },
                ),
            DataFusionError::SchemaError(err) => match err {
                datafusion::common::SchemaError::AmbiguousReference { qualifier, name } => {
                    datafusion_error::Error::SchemaError(
                            failed_job::datafusion_error::SchemaError {
                                error: Some(schema_error::Error::AmbiguousReference(
                                    schema_error::AmbiguousReference {
                                        qualifier: qualifier.clone(),
                                        name: name.clone()
                                    },
                                )),
                            },
                        )
                },
                datafusion::common::SchemaError::DuplicateQualifiedField { qualifier, name } => {
                    datafusion_error::Error::SchemaError(
                            failed_job::datafusion_error::SchemaError {
                                error: Some(schema_error::Error::DuplicateQualifiedField(
                                    schema_error::DuplicateQualifiedField {
                                        qualifier: qualifier.clone(),
                                        name: name.clone()
                                    },
                                )),
                            },
                        )
                },
                datafusion::common::SchemaError::DuplicateUnqualifiedField { name } => {
                    datafusion_error::Error::SchemaError(
                            failed_job::datafusion_error::SchemaError {
                                error: Some(schema_error::Error::DuplicateUnqualifiedField(
                                    schema_error::DuplicateUnqualifiedField {
                                        name: name.clone()
                                    },
                                )),
                            },
                        )
                },
                datafusion::common::SchemaError::FieldNotFound { field, valid_fields } => {
                    datafusion_error::Error::SchemaError(
                            failed_job::datafusion_error::SchemaError {
                                error: Some(schema_error::Error::FieldNotFound(
                                    schema_error::FieldNotFound {
                                        field: field.flat_name(),
                                        valid_fields: valid_fields.iter().map(|f| f.flat_name()).collect_vec()
                                    },
                                )),
                            },
                        )
                },
            },
            DataFusionError::Execution(message) => datafusion_error::Error::Execution(
                    failed_job::datafusion_error::Execution {
                        message: message.clone()
                    },
                ),
            DataFusionError::ResourcesExhausted(message) => datafusion_error::Error::ResourcesExhausted(
                    failed_job::datafusion_error::ResourcesExhausted {
                        message: message.clone()
                    },
                ),
            DataFusionError::External(message) => datafusion_error::Error::External(
                    failed_job::datafusion_error::External {
                        message: message.to_string()
                    },
                ),
            DataFusionError::Context(ctx, error) => datafusion_error::Error::Context(
                    Box::new(failed_job::datafusion_error::Context {
                        ctx: ctx.clone(),
                        error: Some(Box::new(
                            failed_job::DatafusionError { error: Some(error.as_ref().into()) }
                        )),
                    })
                ),
            DataFusionError::Substrait(_) => todo!()
    }
    }
}

impl From<&ArrowError> for failed_job::arrow_error::Error {
    fn from(value: &ArrowError) -> Self {
        match value {
            ArrowError::NotYetImplemented(message) => {
                arrow_error::Error::NotYetImplemented(arrow_error::NotYetImplemented {
                    message: message.clone(),
                })
            }
            ArrowError::ExternalError(message) => {
                arrow_error::Error::ExteranlError(arrow_error::ExternalError {
                    message: message.to_string(),
                })
            }
            ArrowError::CastError(message) => {
                arrow_error::Error::CastError(arrow_error::CastError {
                    message: message.clone(),
                })
            }
            ArrowError::MemoryError(message) => {
                arrow_error::Error::MemoryError(arrow_error::MemoryError {
                    message: message.clone(),
                })
            }
            ArrowError::ParseError(message) => {
                arrow_error::Error::ParseError(arrow_error::ParseError {
                    message: message.clone(),
                })
            }
            ArrowError::SchemaError(message) => {
                arrow_error::Error::SchemaError(arrow_error::SchemaError {
                    message: message.clone(),
                })
            }
            ArrowError::ComputeError(message) => {
                arrow_error::Error::ComputeError(arrow_error::ComputeError {
                    message: message.clone(),
                })
            }
            ArrowError::DivideByZero => {
                arrow_error::Error::DivideByZero(arrow_error::DivideByZero {})
            }
            ArrowError::CsvError(message) => {
                arrow_error::Error::CsvError(arrow_error::CsvError {
                    message: message.clone(),
                })
            }
            ArrowError::JsonError(message) => {
                arrow_error::Error::JsonError(arrow_error::JsonError {
                    message: message.clone(),
                })
            }
            ArrowError::IoError(message) => {
                arrow_error::Error::IoError(arrow_error::IoError {
                    message: message.clone(),
                })
            }
            ArrowError::InvalidArgumentError(message) => {
                arrow_error::Error::InvalidArgumentError(
                    arrow_error::InvalidArgumentError {
                        message: message.clone(),
                    },
                )
            }
            ArrowError::ParquetError(message) => {
                arrow_error::Error::ParquetError(arrow_error::ParquetError {
                    message: message.clone(),
                })
            }
            ArrowError::CDataInterface(message) => {
                arrow_error::Error::CDataInterface(arrow_error::CDataInterface {
                    message: message.clone(),
                })
            }
            ArrowError::DictionaryKeyOverflowError => {
                arrow_error::Error::DictionaryKeyOverflowError(
                    arrow_error::DictionaryKeyOverflowError {},
                )
            }
            ArrowError::RunEndIndexOverflowError => {
                arrow_error::Error::RunEndIndexOverflowError(
                    arrow_error::RunEndIndexOverflowError {},
                )
            }
        }
    }
}

impl From<&BallistaError> for failed_job::Error {
    fn from(value: &BallistaError) -> Self {
        match value {
            BallistaError::NotImplemented(message) => {
                failed_job::Error::NotImplemented(NotImplemented {
                    message: message.clone(),
                })
            }
            BallistaError::General(message) => failed_job::Error::General(General {
                message: message.clone(),
            }),
            BallistaError::Internal(message) => failed_job::Error::Internal(Internal {
                message: message.clone(),
            }),
            BallistaError::ArrowError(error) => {
                failed_job::Error::ArrowError(failed_job::ArrowError {
                    error: Some(error.into()),
                })
            }
            BallistaError::DataFusionError(error) => {
                failed_job::Error::DatafusionError(failed_job::DatafusionError {
                    error: Some(error.into()),
                })
            }
            BallistaError::SqlError(error) => {
                failed_job::Error::SqlError(failed_job::SqlError {
                    error: Some(failed_job::ParserError {
                        error: Some(error.into()),
                    }),
                })
            }
            BallistaError::IoError(_) => todo!(),
            BallistaError::TonicError(_) => todo!(),
            BallistaError::GrpcError(_) => todo!(),
            BallistaError::GrpcConnectionError(_) => todo!(),
            BallistaError::TokioError(_) => todo!(),
            BallistaError::GrpcActionError(_) => todo!(),
            BallistaError::FetchFailed(_, _, _, _) => todo!(),
            BallistaError::Cancelled => todo!(),
        }
    }
}

impl Display for BallistaError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            BallistaError::NotImplemented(ref desc) => {
                write!(f, "Not implemented: {desc}")
            }
            BallistaError::General(ref desc) => write!(f, "General error: {desc}"),
            BallistaError::ArrowError(ref desc) => write!(f, "Arrow error: {desc}"),
            BallistaError::DataFusionError(ref desc) => {
                write!(f, "DataFusion error: {desc:?}")
            }
            BallistaError::SqlError(ref desc) => write!(f, "SQL error: {desc:?}"),
            BallistaError::IoError(ref desc) => write!(f, "IO error: {desc}"),
            // BallistaError::ReqwestError(ref desc) => write!(f, "Reqwest error: {}", desc),
            // BallistaError::HttpError(ref desc) => write!(f, "HTTP error: {}", desc),
            // BallistaError::KubeAPIError(ref desc) => write!(f, "Kube API error: {}", desc),
            // BallistaError::KubeAPIRequestError(ref desc) => {
            //     write!(f, "KubeAPI request error: {}", desc)
            // }
            // BallistaError::KubeAPIResponseError(ref desc) => {
            //     write!(f, "KubeAPI response error: {}", desc)
            // }
            BallistaError::TonicError(desc) => write!(f, "Tonic error: {desc}"),
            BallistaError::GrpcError(desc) => write!(f, "Grpc error: {desc}"),
            BallistaError::GrpcConnectionError(desc) => {
                write!(f, "Grpc connection error: {desc}")
            }
            BallistaError::Internal(desc) => {
                write!(f, "Internal Ballista error: {desc}")
            }
            BallistaError::TokioError(desc) => write!(f, "Tokio join error: {desc}"),
            BallistaError::GrpcActionError(desc) => {
                write!(f, "Grpc Execute Action error: {desc}")
            }
            BallistaError::FetchFailed(executor_id, map_stage, map_partition, desc) => {
                write!(
                    f,
                    "Shuffle fetch partition error from Executor {executor_id}, map_stage {map_stage}, \
                map_partition {map_partition}, error desc: {desc}"
                )
            }
            BallistaError::Cancelled => write!(f, "Task cancelled"),
        }
    }
}

impl From<BallistaError> for FailedTask {
    fn from(e: BallistaError) -> Self {
        match e {
            BallistaError::FetchFailed(
                executor_id,
                map_stage_id,
                map_partition_id,
                desc,
            ) => {
                FailedTask {
                    error: desc,
                    // fetch partition error is considered to be non-retryable
                    retryable: false,
                    count_to_failures: false,
                    failed_reason: Some(FailedReason::FetchPartitionError(
                        FetchPartitionError {
                            executor_id,
                            map_stage_id: map_stage_id as u32,
                            map_partition_id: map_partition_id as u32,
                        },
                    )),
                }
            }
            BallistaError::IoError(io) => {
                FailedTask {
                    error: format!("Task failed due to Ballista IO error: {io:?}"),
                    // IO error is considered to be temporary and retryable
                    retryable: true,
                    count_to_failures: true,
                    failed_reason: Some(FailedReason::IoError(IoError {})),
                }
            }
            BallistaError::DataFusionError(DataFusionError::IoError(io)) => {
                FailedTask {
                    error: format!("Task failed due to DataFusion IO error: {io:?}"),
                    // IO error is considered to be temporary and retryable
                    retryable: true,
                    count_to_failures: true,
                    failed_reason: Some(FailedReason::IoError(IoError {})),
                }
            }
            other => FailedTask {
                error: format!("Task failed due to runtime execution error: {other:?}"),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(FailedReason::ExecutionError(ExecutionError {})),
            },
        }
    }
}

impl Error for BallistaError {}
