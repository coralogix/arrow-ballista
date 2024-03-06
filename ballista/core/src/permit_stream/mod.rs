use std::{
    pin::Pin,
    task::{Context, Poll},
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::{Stream, StreamExt};

pub struct PermitRecordBatchStream {
    inner: SendableRecordBatchStream,

    #[allow(dead_code)]
    permit: tokio::sync::OwnedSemaphorePermit,
}

impl PermitRecordBatchStream {
    pub fn new(
        inner: SendableRecordBatchStream,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self { permit, inner }
    }

    pub fn wrap(
        inner: SendableRecordBatchStream,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> SendableRecordBatchStream {
        Box::pin(Self::new(inner, permit))
    }
}

impl Stream for PermitRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(batch)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for PermitRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
