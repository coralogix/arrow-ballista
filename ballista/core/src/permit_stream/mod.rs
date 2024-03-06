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
use tokio::time::Instant;

pub struct PermitRecordBatchStream {
    inner: SendableRecordBatchStream,
    started_at: Instant,
    on_close: Option<Box<dyn Fn(f64) + Send + 'static>>,

    // used to release a permit when the stream is dropped
    #[allow(dead_code)]
    permit: tokio::sync::OwnedSemaphorePermit,
}

impl PermitRecordBatchStream {
    pub fn new(
        inner: SendableRecordBatchStream,
        on_close: Option<Box<dyn Fn(f64) + Send + 'static>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            permit,
            inner,
            on_close,
            started_at: Instant::now(),
        }
    }

    pub fn wrap(
        inner: SendableRecordBatchStream,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> SendableRecordBatchStream {
        Box::pin(Self::new(inner, None, permit))
    }

    pub fn wrap_with_on_close(
        inner: SendableRecordBatchStream,
        on_close: Option<Box<dyn Fn(f64) + Send + 'static>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> SendableRecordBatchStream {
        Box::pin(Self::new(inner, on_close, permit))
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

impl Drop for PermitRecordBatchStream {
    fn drop(&mut self) {
        if let Some(func) = self.on_close.as_ref() {
            func(self.started_at.elapsed().as_secs_f64())
        }
    }
}
