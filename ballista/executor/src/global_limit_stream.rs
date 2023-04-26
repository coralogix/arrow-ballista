use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub struct GlobalLimitConfig {
    pub send_update: Sender<GlobalLimitUpdate>,
    pub short_circuit: Arc<AtomicBool>,
}

pub struct GlobalLimitStream {
    pub inner: Pin<Box<dyn RecordBatchStream + Send>>,
    pub config: GlobalLimitConfig,
    pub task_id: String,
}

impl GlobalLimitStream {
    pub fn new(
        inner: Pin<Box<dyn RecordBatchStream + Send>>,
        config: GlobalLimitConfig,
        task_id: String,
    ) -> Self {
        Self {
            inner,
            config,
            task_id,
        }
    }
}

pub struct GlobalLimitUpdate {
    pub task_id: String,
    pub num_rows: u64,
    pub num_bytes: u64,
}

impl RecordBatchStream for GlobalLimitStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for GlobalLimitStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.config.short_circuit.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }

        let poll = self.inner.poll_next_unpin(cx);

        if let Poll::Ready(Some(Ok(record_batch))) = &poll {
            let status_update = GlobalLimitUpdate {
                task_id: self.task_id.clone(),
                num_rows: record_batch.num_rows() as u64,
                num_bytes: record_batch.get_array_memory_size() as u64,
            };

            if let Err(e) = self.config.send_update.try_send(status_update) {
                // Fail or log warning?
                return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                    "Failed to send global limit status update: {}",
                    e
                )))));
            }
        }

        poll
    }
}
