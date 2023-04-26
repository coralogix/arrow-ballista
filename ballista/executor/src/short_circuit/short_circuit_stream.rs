use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::Sender;
use tracing::warn;

#[derive(Debug)]
pub struct ShortCircuitConfig {
    pub send_update: Sender<ShortCircuitUpdate>,
    pub short_circuit: Arc<AtomicBool>,
}

pub struct ShortCircuitStream {
    pub inner: Pin<Box<dyn RecordBatchStream + Send>>,
    pub config: ShortCircuitConfig,
    pub task_id: String,
    buffered_row_count: u64,
    buffered_byte_size: u64,
}

impl ShortCircuitStream {
    pub fn new(
        inner: Pin<Box<dyn RecordBatchStream + Send>>,
        config: ShortCircuitConfig,
        task_id: String,
    ) -> Self {
        Self {
            inner,
            config,
            task_id,
            buffered_row_count: 0,
            buffered_byte_size: 0,
        }
    }
}

#[derive(Debug)]
pub struct ShortCircuitUpdate {
    pub task_id: String,
    pub num_rows: u64,
    pub num_bytes: u64,
}

impl RecordBatchStream for ShortCircuitStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for ShortCircuitStream {
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
            let num_rows = record_batch.num_rows() as u64;
            let num_bytes = record_batch.get_array_memory_size() as u64;
            let status_update = ShortCircuitUpdate {
                task_id: self.task_id.clone(),
                num_rows,
                num_bytes,
            };

            if let Err(e) = self.config.send_update.try_send(status_update) {
                if self.buffered_byte_size + self.buffered_row_count == 0 {
                    warn!("Stream could not send short circuit update to daemon, it might be running very fast! ({:?})", e);
                }

                self.buffered_byte_size += num_rows;
                self.buffered_row_count += num_bytes;
            }
        }

        poll
    }
}
